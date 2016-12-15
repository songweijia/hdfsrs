/*
 * group_manager.cpp
 *
 *  Created on: Apr 22, 2016
 *      Author: edward
 */

#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <exception>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <iterator>
#include <cstring>
#include <atomic>

#include "managed_group.h"

#include "derecho_group.h"
#include "derecho_row.h"
#include "view.h"
#include "view_utils.h"
#include "sst/sst.h"

namespace derecho {

using std::map;
using std::vector;
using std::make_shared;
using std::make_unique;
using std::unique_ptr;
using std::cout;
using std::endl;
using sst::SST;
using std::chrono::high_resolution_clock;
using util::debug_log;

using lock_guard_t = std::lock_guard<std::mutex>;
using unique_lock_t = std::unique_lock<std::mutex>;

bool ManagedGroup::rdmc_globals_initialized = false;

ManagedGroup::ManagedGroup(const int gms_port, const map<node_id_t, ip_addr>& member_ips, node_id_t my_id, node_id_t leader_id,
        long long unsigned int _max_payload_size, message_callback global_stability_callback,
        long long unsigned int _block_size, unsigned int _window_size, rdmc::send_algorithm _type) :
        member_ips_by_id(member_ips), last_suspected(View::MAX_MEMBERS), gms_port(gms_port), server_socket(gms_port),
        thread_shutdown(false), next_view(nullptr) {

    if (!rdmc_globals_initialized) {
        global_setup(member_ips, my_id);
    }
    std::vector<MessageBuffer> message_buffers;
    auto max_msg_size = DerechoGroup<View::MAX_MEMBERS>::compute_max_msg_size(_max_payload_size, _block_size);
    while(message_buffers.size() < _window_size * View::MAX_MEMBERS) {
        message_buffers.emplace_back(max_msg_size);
    }
    if (my_id != leader_id){
        curr_view = join_existing(member_ips_by_id[leader_id], gms_port);
    } else {
        curr_view = start_group(my_id);
//        tcp::connection_listener serversocket(gms_port);
        tcp::socket client_socket = server_socket.accept();
        ip_addr& joiner_ip = client_socket.remote_ip;
        curr_view->num_members++;
        curr_view->member_ips.push_back(joiner_ip);
        for (const auto& entry : member_ips_by_id) {
            if (entry.second == joiner_ip) {
                curr_view->members.push_back(entry.first);
                break;
            }
        }
        curr_view->failed.push_back(false);

        client_socket.write((char*) &curr_view->vid, sizeof(curr_view->vid));
        client_socket.write((char*) &curr_view->num_members, sizeof(curr_view->num_members));
        for (auto nodeID : curr_view->members) {
            client_socket.write((char*) &nodeID, sizeof(nodeID));
        }
        for (auto & str : curr_view->member_ips) {
            //includes null-terminator
            const int str_size = str.size() + 1;
            client_socket.write((char*) &str_size, sizeof(str_size));
            client_socket.write(str.c_str(), str_size);
        }
        for (bool fbool : curr_view->failed) {
            client_socket.write((char*) &fbool, sizeof(fbool));
        }
    }
    curr_view->my_rank = curr_view->rank_of(my_id);
    //Temporarily disabled because all member IP->ID mappings are fixed at startup
//    for(int rank = 0; rank < curr_view->num_members; ++rank) {
//        member_ips_by_id[curr_view->members[rank]] = curr_view->member_ips[rank];
//    }
    
    log_event("Initializing SST and RDMC for the first time.");
    setup_sst_and_rdmc(message_buffers, _max_payload_size, global_stability_callback, _block_size, _window_size, _type);
    curr_view->gmsSST->put();
    curr_view->gmsSST->sync_with_members();
	log_event("Done setting up initial SST and RDMC");

    if(my_id != leader_id && curr_view->vid != 0) {
        //If this node is joining an existing group with a non-initial view, copy the leader's nChanges and nAcked
        //Otherwise, you'll immediately think that there's as new proposed view changed because [leader].nChanges > nAcked
        gmssst::init_from_existing((*curr_view->gmsSST)[curr_view->my_rank],(*curr_view->gmsSST)[curr_view->rank_of_leader()]);
        curr_view->gmsSST->put();
//         << "New node initialized its row to: " << gmssst::to_string((*curr_view->gmsSST)[curr_view->my_rank]) << endl;
		log_event("Joining node initialized its SST row from the leader");
    }

    client_listener_thread = std::thread{[this](){
        while (!thread_shutdown) {
            tcp::socket client_socket = server_socket.accept();
            debug_log().log_event(std::stringstream() << "Background thread got a client connection from " << client_socket.remote_ip);
            pending_joins.locked().access.emplace_back(std::move(client_socket));
        }
        cout << "Connection listener thread shutting down." << endl;
    }};

    old_view_cleanup_thread = std::thread([this]() {
       while(!thread_shutdown) {
           unique_lock_t old_views_lock(old_views_mutex);
           old_views_cv.wait(old_views_lock, [this](){return !old_views.empty() || thread_shutdown;});
           if(!thread_shutdown) {
               old_views.front().reset();
               old_views.pop();
           }
       }
       cout << "Old View cleanup thread shutting down." << endl;
    });

    register_predicates();
	curr_view->gmsSST->start_predicate_evaluation();

	// curr_view->rdmc_sending_group->debug_print();
}

void ManagedGroup::global_setup(const map<node_id_t, ip_addr>& member_ips, node_id_t my_id) {
    cout << "Doing global setup of SST and RDMC" << endl;
    rdmc::initialize(member_ips, my_id);
    sst::tcp::tcp_initialize(my_id, member_ips);
    sst::verbs_initialize();
    rdmc_globals_initialized = true;
}

void ManagedGroup::register_predicates() {

    using DerechoSST = View::DerechoSST;

    auto suspected_changed = [this](const DerechoSST& sst) {
        return suspected_not_equal(sst, last_suspected);
    };
    auto suspected_changed_trig = [this](DerechoSST& gmsSST) {
		log_event("Suspected[] changed");
        View& Vc = *curr_view;
        int myRank = curr_view->my_rank;
        //These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);
        // Aggregate suspicions into gmsSST[myRank].Suspected;
        for (int r = 0; r < Vc.num_members; r++)
        {
            for (int who = 0; who < Vc.num_members; who++)
            {
                gmssst::set(gmsSST[myRank].suspected[who], gmsSST[myRank].suspected[who] || gmsSST[r].suspected[who]);
            }
        }

//        cout << "Suspected a new failure! View is " << curr_view->ToString() << endl << "and my row is: " << gmssst::to_string(gmsSST[myRank]) << endl;
        for (int q = 0; q < Vc.num_members; q++)
        {
            if (gmsSST[myRank].suspected[q] && !Vc.failed[q])
            {
				log_event(std::string("Marking ") + std::to_string(Vc.members[q]) + std::string(" failed"));
                if (Vc.nFailed + 1 >= Vc.num_members / 2)
                {
                    throw derecho_exception("Majority of a Derecho group simultaneously failed ... shutting down");
                }

                std::stringstream s;
                s << "GMS telling SST to freeze row " << q << " which is node " << Vc.members[q];
                log_event(s);
                gmsSST.freeze(q); // Cease to accept new updates from q
                Vc.rdmc_sending_group->wedge();
                gmssst::set(gmsSST[myRank].wedged, true); // RDMC has halted new sends and receives in theView
                Vc.failed[q] = true;
                Vc.nFailed++;

                if (Vc.nFailed > Vc.num_members / 2 || (Vc.nFailed == Vc.num_members / 2 && Vc.num_members % 2 == 0))
                {
                    throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
                }

                gmsSST.put();
                if (Vc.IAmLeader() && !changes_contains(gmsSST, Vc.members[q])) // Leader initiated
                {
                    if ((gmsSST[myRank].nChanges - gmsSST[myRank].nCommitted) == View::MAX_MEMBERS)
                    {
                        throw derecho_exception("Ran out of room in the pending changes list");
                    }

                    gmssst::set(gmsSST[myRank].changes[gmsSST[myRank].nChanges % View::MAX_MEMBERS], Vc.members[q]); // Reports the failure (note that q NotIn members)
                    gmssst::increment(gmsSST[myRank].nChanges);
//                    std::cout << std::string("NEW SUSPICION: adding ") << Vc.members[q] << std::string(" to the CHANGES/FAILED list") << std::endl;
					log_event(std::stringstream() << "Leader proposed a change to remove failed node " << Vc.members[q]);
                    gmsSST.put();
                }
            }
        }
        copy_suspected(gmsSST, last_suspected);
    };

    //Only start one join at a time
    auto start_join_pred = [this](const DerechoSST& sst) {
//        cout << "start_join_pred, values are: preds_disabled=" << preds_disabled << " IAmLeader=" << curr_view->IAmLeader() << " has_pending_join=" << has_pending_join() << " socket is empty = " << joining_client_socket.is_empty() << endl;
        return curr_view->IAmLeader() && has_pending_join() && joining_client_socket.is_empty();
    };
    auto start_join_trig = [this](DerechoSST& sst) {
		log_event("GMS handling a new client connection");
        joining_client_socket = std::move(pending_joins.locked().access.front()); //list.front() is now invalid because sockets are move-only, but C++ leaves it on the list
        pending_joins.locked().access.pop_front(); //because C++ list doesn't properly implement queues, this returns void
        receive_join(joining_client_socket);
    };


    auto change_commit_ready = [this](const DerechoSST& gmsSST) {
        return curr_view->my_rank == curr_view->rank_of_leader()
                && min_acked(gmsSST, curr_view->failed) > gmsSST[gmsSST.get_local_index()].nCommitted;
    };
    auto commit_change = [this](DerechoSST& gmsSST) {
        gmssst::set(gmsSST[gmsSST.get_local_index()].nCommitted, min_acked(gmsSST, curr_view->failed)); // Leader commits a new request
//        cout << "Leader's row is: " << gmssst::to_string(gmsSST[gmsSST.get_local_index()]) << endl;
        log_event(std::stringstream() << "Leader committing view proposal #" << gmsSST[gmsSST.get_local_index()].nCommitted);
        gmsSST.put();
    };

    auto leader_proposed_change = [this](const DerechoSST& gmsSST) {
        return gmsSST[curr_view->rank_of_leader()].nChanges > gmsSST[gmsSST.get_local_index()].nAcked;
    };
    auto ack_proposed_change = [this](DerechoSST& gmsSST) {
        //These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);
        int myRank = gmsSST.get_local_index();
        int leader = curr_view->rank_of_leader();
        log_event(std::stringstream() << "Detected that leader proposed view change #" <<  gmsSST[leader].nChanges << ". Acknowledging.");
        if (myRank != leader)
        {
            gmssst::set(gmsSST[myRank].changes, gmsSST[leader].changes); // Echo (copy) the vector including the new changes
            gmssst::set(gmsSST[myRank].joiner_ip, gmsSST[leader].joiner_ip); //Echo the new member's IP
            gmssst::set(gmsSST[myRank].nChanges, gmsSST[leader].nChanges); // Echo the count
            gmssst::set(gmsSST[myRank].nCommitted, gmsSST[leader].nCommitted);
        }

        gmssst::set(gmsSST[myRank].nAcked, gmsSST[leader].nChanges); // Notice a new request, acknowledge it
        gmsSST.put();
        log_event("Wedging current view.");
        wedge_view(*curr_view);
        log_event("Done wedging current view.");

    };

    auto leader_committed_next_view = [this](const DerechoSST& gmsSST) {
        return gmsSST[curr_view->rank_of_leader()].nCommitted > curr_view->vid;
    };
    auto start_view_change = [this](DerechoSST& gmsSST)  {
        log_event(std::stringstream() << "Starting view change to view " << curr_view->vid + 1);
        // Disable all the other SST predicates, except suspected_changed and the one I'm about to register
        gmsSST.predicates.remove(start_join_handle);
        gmsSST.predicates.remove(change_commit_ready_handle);
        gmsSST.predicates.remove(leader_proposed_handle);

        View& Vc = *curr_view;
        int myRank = curr_view->my_rank;
        //These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);

        wedge_view(Vc);
        node_id_t currChangeID = gmsSST[myRank].changes[Vc.vid % View::MAX_MEMBERS];
        next_view = std::make_unique<View>();
        next_view->vid = Vc.vid + 1;
        next_view->IKnowIAmLeader = Vc.IKnowIAmLeader;
        node_id_t myID = Vc.members[myRank];
        bool failed;
        int whoFailed = Vc.rank_of(currChangeID);
        if (whoFailed != -1)
        {
            failed = true;
            next_view->nFailed = Vc.nFailed - 1;
            next_view->num_members = Vc.num_members - 1;
            next_view->init_vectors();
        }
        else
        {
            failed = false;
            next_view->nFailed = Vc.nFailed;
            next_view->num_members = Vc.num_members;
            int new_member_rank = next_view->num_members++;
            next_view->init_vectors();
            next_view->members[new_member_rank] = currChangeID;
            next_view->member_ips[new_member_rank] = std::string(const_cast<cstring &>(gmsSST[myRank].joiner_ip));
            member_ips_by_id[currChangeID] = next_view->member_ips[new_member_rank];
        }


        int m = 0;
        for (int n = 0; n < Vc.num_members; n++)
        {
            if (n != whoFailed)
            {
                next_view->members[m] = Vc.members[n];
                next_view->failed[m] = Vc.failed[n];
                ++m;
            }
        }

        next_view->who = std::make_shared<node_id_t>(currChangeID);
        if ((next_view->my_rank = next_view->rank_of(myID)) == -1)
        {
            throw derecho_exception((std::stringstream() << "Some other node reported that I failed.  Node " << myID << " terminating").str());
        }

        if (next_view->gmsSST != nullptr)
        {
            throw derecho_exception("Overwriting the SST");
        }


        //At this point we need to await "meta wedged."
        //To do that, we create a predicate that will fire when meta wedged is true, and put the rest of the code in its trigger.

        auto is_meta_wedged = [this] (const DerechoSST& gmsSST) mutable {
            assert(next_view);
            for(int n = 0; n < gmsSST.get_num_rows(); ++n) {
                if(!curr_view->failed[n] && !gmsSST[n].wedged) {
                    return false;
                }
            }
            return true;
        };
        auto meta_wedged_continuation = [this, failed, whoFailed] (DerechoSST& gmsSST) {
            log_event("MetaWedged is true; continuing view change");
            unique_lock_t lock(view_mutex);
            assert(next_view);

            auto globalMin_ready_continuation = [this, failed, whoFailed](DerechoSST& gmsSST) {
                lock_guard_t lock(view_mutex);
                assert(next_view);

                ragged_edge_cleanup(*curr_view);
                if(curr_view->IAmLeader() && !failed) {
                    //Send the view to the newly joined client before we try to do SST and RDMC setup
                    commit_join(*next_view, joining_client_socket);
                    //Close the client's socket
                    joining_client_socket = tcp::socket();
                }

                //Delete the last two GMS predicates from the old SST in preparation for deleting it
                gmsSST.predicates.remove(leader_committed_handle);
                gmsSST.predicates.remove(suspected_changed_handle);

				log_event(std::stringstream() << "Starting creation of new SST and DerechoGroup for view " << next_view->vid);
                //This will block until everyone responds to SST/RDMC initial handshakes
                transition_sst_and_rdmc(*next_view, whoFailed);
                next_view->gmsSST->put();
                next_view->gmsSST->sync_with_members();
				log_event(std::stringstream() << "Done setting up SST and DerechoGroup for view " << next_view->vid);
                {
                    lock_guard_t old_views_lock(old_views_mutex);
                    old_views.push(std::move(curr_view));
                    old_views_cv.notify_all();
                }
                curr_view = std::move(next_view);
                curr_view->newView(*curr_view); // Announce the new view to the application

                view_change_cv.notify_all();


                //Register predicates in the new view
                register_predicates();
				curr_view->gmsSST->start_predicate_evaluation();

                // First task with my new view...
                if (IAmTheNewLeader(*curr_view)) // I'm the new leader and everyone who hasn't failed agrees
                {
                    merge_changes(*curr_view); // Create a combined list of Changes
                }
            };

            if(curr_view->IAmLeader()) {
                //The leader doesn't need to wait any more, it can execute continuously from here.
                lock.unlock();
                globalMin_ready_continuation(gmsSST);
            } else {
                //Non-leaders need another level of continuation to wait for GlobalMinReady
                auto leader_globalMin_is_ready = [this](const DerechoSST& gmsSST) {
                    assert(next_view);
                    return gmsSST[curr_view->rank_of_leader()].globalMinReady;
                };
                gmsSST.predicates.insert(leader_globalMin_is_ready, globalMin_ready_continuation, sst::PredicateType::ONE_TIME);
            }

        };
        gmsSST.predicates.insert(is_meta_wedged, meta_wedged_continuation, sst::PredicateType::ONE_TIME);

    };

    suspected_changed_handle = curr_view->gmsSST->predicates.insert(suspected_changed, suspected_changed_trig, sst::PredicateType::RECURRENT);
	start_join_handle = curr_view->gmsSST->predicates.insert(start_join_pred, start_join_trig, sst::PredicateType::RECURRENT);
	change_commit_ready_handle = curr_view->gmsSST->predicates.insert(change_commit_ready, commit_change, sst::PredicateType::RECURRENT);
	leader_proposed_handle = curr_view->gmsSST->predicates.insert(leader_proposed_change, ack_proposed_change, sst::PredicateType::RECURRENT);
	leader_committed_handle = curr_view->gmsSST->predicates.insert(leader_committed_next_view, start_view_change, sst::PredicateType::ONE_TIME);
}

ManagedGroup::~ManagedGroup() {
    thread_shutdown = true;
    //force accept to return.
    tcp::socket s{"localhost", gms_port};
    if(client_listener_thread.joinable()){
        client_listener_thread.join();
    }
    old_views_cv.notify_all();
    if(old_view_cleanup_thread.joinable()) {
        old_view_cleanup_thread.join();
    }
}

void ManagedGroup::setup_sst_and_rdmc(vector<MessageBuffer>& message_buffers, long long unsigned int max_payload_size, message_callback global_stability_callback,
        long long unsigned int block_size, unsigned int window_size, rdmc::send_algorithm type) {

    curr_view->gmsSST = make_shared<sst::SST<DerechoRow<View::MAX_MEMBERS>>>(curr_view->members, curr_view->members[curr_view->my_rank],
            [this](const uint32_t node_id){report_failure(node_id);});
    for(int r = 0; r < curr_view->num_members; ++r) {
        gmssst::init((*curr_view->gmsSST)[r]);
    }
    gmssst::set((*curr_view->gmsSST)[curr_view->my_rank].vid, curr_view->vid);

    curr_view->rdmc_sending_group = make_unique<DerechoGroup<View::MAX_MEMBERS>>(curr_view->members, curr_view->members[curr_view->my_rank],
            curr_view->gmsSST, message_buffers, max_payload_size, global_stability_callback, block_size, window_size, 1, type);
}

/**
 *
 * @param newView The new view in which to construct an SST and derecho_group
 * @param whichFailed The rank of the node in the old view that failed, if any.
 */
void ManagedGroup::transition_sst_and_rdmc(View& newView, int whichFailed) {

    //Temporarily disabled because all nodes are initialized at the beginning
//    if(whichFailed == -1) { //This is a join
//        rdmc::add_address(newView.members.back(), newView.member_ips.back());
//    }
//
//    std::map<node_id_t, ip_addr> new_member_map {{newView.members[newView.my_rank], newView.member_ips[newView.my_rank]}, {newView.members.back(), newView.member_ips.back()}};
//    sst::tcp::tcp_initialize(newView.members[newView.my_rank], new_member_map);
    newView.gmsSST = make_shared<sst::SST<DerechoRow<View::MAX_MEMBERS>>>(newView.members, newView.members[newView.my_rank],
            [this](const uint32_t node_id){report_failure(node_id);});
    newView.rdmc_sending_group = make_unique<DerechoGroup<View::MAX_MEMBERS>>(newView.members, newView.members[newView.my_rank],
            newView.gmsSST, std::move(*curr_view->rdmc_sending_group));
	curr_view->rdmc_sending_group.reset();
	
	//Don't need to initialize every row in the SST - all the others will be filled by the first put()
//    int m = 0;
//    for (int n = 0; n < curr_view->num_members; n++)
//    {
//        if (n != whichFailed)
//        {
//            //the compiler won't upcast these references inside the function call,
//            //but it can figure out what I mean if I declare them as locals.
//            volatile auto& new_row = (*newView.gmsSST)[m++];
//            volatile auto& old_row = (*curr_view->gmsSST)[n];
//            gmssst::template init_from_existing<View::MAX_MEMBERS>(new_row, old_row);
//            new_row.vid = newView.vid;
//        }
//    }
	//Initialize this node's row in the new SST
	gmssst::template init_from_existing<View::MAX_MEMBERS>((*newView.gmsSST)[newView.my_rank], (*curr_view->gmsSST)[curr_view->my_rank]);
	gmssst::set((*newView.gmsSST)[newView.my_rank].vid, newView.vid);

}

unique_ptr<View> ManagedGroup::start_group(const node_id_t my_id) {
    log_event("Starting new empty group with myself as leader");
    unique_ptr<View> newView = std::make_unique<View>(1);
    newView->members[0] = my_id;
    newView->member_ips[0] = member_ips_by_id[my_id];
    newView->failed[0] = false;
    newView->IKnowIAmLeader = true;
    return newView;
}

unique_ptr<View> ManagedGroup::join_existing(const ip_addr& leader_ip, const int leader_port) {
//    cout << "Joining group by contacting node at " << leader_ip << endl;
	log_event("Joining group: waiting for a response from the leader");
    tcp::socket leader_socket{leader_ip, leader_port};
    int viewID;
    int numMembers;
//    node_id_t myNodeID;
    //Temporarily disabled because all node IDs are fixed at startup
    //First the leader sends the node ID this client has been assigned
//    bool success = leader_socket.read((char*)&myNodeID,sizeof(myNodeID));
//    assert(success);
    //The leader will send the members of View in the order they're declared
    bool success = leader_socket.read((char*)&viewID,sizeof(viewID));
    assert(success);
    bool success2 = leader_socket.read((char*)&numMembers,sizeof(numMembers));
    assert(success2);
    unique_ptr<View> newView = std::make_unique<View>(numMembers);
    newView->vid = viewID;
    for (int i = 0; i < numMembers; ++i){
        bool success = leader_socket.read((char*)&newView->members[i],
                sizeof(decltype(newView->members)::value_type));
        assert(success);
    }
    //protocol for sending strings: size, followed by string
    //including null terminator
    for (int i = 0; i < numMembers; ++i){
        int str_size{-1};
        bool success = leader_socket.read((char*)&str_size,sizeof(str_size));
        assert(success);
        char str_rec[str_size];
        bool success2 = leader_socket.read(str_rec,str_size);
        assert(success2);
        newView->member_ips[i] = std::string(str_rec);
    }
    //Receive failed[], and also calculate nFailed
    for (int i = 0; i < numMembers; ++i){
        bool newView_failed_i;
        bool success = leader_socket.read((char*) &newView_failed_i,
                sizeof(newView_failed_i));
        assert(success);
        newView->failed[i] = newView_failed_i;
        if(newView->failed[i]) newView->nFailed++;
    }

	log_event("Received View from leader");
    return newView;
}


void ManagedGroup::receive_join(tcp::socket& client_socket) {
    ip_addr& joiner_ip = client_socket.remote_ip;
    using derechoSST = sst::SST<DerechoRow<View::MAX_MEMBERS>>;
    derechoSST& gmsSST = *curr_view->gmsSST;
    if ((gmsSST[curr_view->my_rank].nChanges - gmsSST[curr_view->my_rank].nCommitted) == View::MAX_MEMBERS/2)
    {
        throw derecho_exception("Too many changes to allow a Join right now");
    }

//    node_id_t largest_id = member_ips_by_id.rbegin()->first;
//    joining_client_id = largest_id + 1;
    for (const auto& entry : member_ips_by_id) {
        if (entry.second == joiner_ip) {
            joining_client_id = entry.first;
            break;
        }
    }

	log_event(std::stringstream() << "Proposing change to add node " << joining_client_id);
    size_t next_change = gmsSST[curr_view->my_rank].nChanges % View::MAX_MEMBERS;
    gmssst::set(gmsSST[curr_view->my_rank].changes[next_change], joining_client_id);
    gmssst::set(gmsSST[curr_view->my_rank].joiner_ip, joiner_ip);

    gmssst::increment(gmsSST[curr_view->my_rank].nChanges);

    log_event(std::stringstream() << "Wedging view " << curr_view->vid);
    wedge_view(*curr_view);
	log_event("Leader done wedging view.");
    gmsSST.put();
}

void ManagedGroup::commit_join(const View &new_view, tcp::socket &client_socket) {
	log_event("Sending client the new view");
    //Temporarily disabled because all node IDs are globally fixed at startup
//    client_socket.write((char*) &joining_client_id, sizeof(joining_client_id));
    client_socket.write((char*) &new_view.vid, sizeof(new_view.vid));
    client_socket.write((char*) &new_view.num_members, sizeof(new_view.num_members));
    for (auto nodeID : new_view.members) {
        client_socket.write((char*) &nodeID, sizeof(nodeID));
    }
    for (auto & str : new_view.member_ips) {
        //includes null-terminator
        const int str_size = str.size() + 1;
        client_socket.write((char*) &str_size, sizeof(str_size));
        client_socket.write(str.c_str(), str_size);
    }
    for (bool fbool : new_view.failed) {
        client_socket.write((char*) &fbool, sizeof(fbool));
    }
}

/* ------------------------- Static helper methods ------------------------- */

bool ManagedGroup::suspected_not_equal(const View::DerechoSST& gmsSST, const vector<bool>& old) {
    for (int r = 0; r < gmsSST.get_num_rows(); r++) {
        for (int who = 0; who < View::MAX_MEMBERS; who++) {
            if (gmsSST[r].suspected[who] && !old[who]) {
                return true;
            }
        }
    }
    return false;
}

void ManagedGroup::copy_suspected(const View::DerechoSST& gmsSST, vector<bool>& old) {
    for(int who = 0; who < gmsSST.get_num_rows(); ++who) {
        old[who] = gmsSST[gmsSST.get_local_index()].suspected[who];
    }
}

bool ManagedGroup::changes_contains(const View::DerechoSST& gmsSST, const node_id_t q) {
    auto& myRow = gmsSST[gmsSST.get_local_index()];
    for (int n = myRow.nCommitted; n < myRow.nChanges; n++) {
        int p_index = n % View::MAX_MEMBERS;
        const node_id_t p(const_cast<node_id_t&>(myRow.changes[p_index]));
        if (p_index < myRow.nChanges && p == q) {
            return true;
        }
    }
    return false;
}

int ManagedGroup::min_acked(const View::DerechoSST& gmsSST, const vector<bool>& failed) {
    int myRank = gmsSST.get_local_index();
    int min = gmsSST[myRank].nAcked;
    for (size_t n = 0; n < failed.size(); n++) {
        if (!failed[n] && gmsSST[n].nAcked < min) {
            min = gmsSST[n].nAcked;
        }
    }

    return min;
}


void ManagedGroup::deliver_in_order(const View& Vc, int Leader) {
    // Ragged cleanup is finished, deliver in the implied order
	std::vector<long long int> max_received_indices(Vc.num_members);
    std::string deliveryOrder(" ");
    for (int n = 0; n < Vc.num_members; n++) {
        deliveryOrder += std::to_string(Vc.members[Vc.my_rank]) + std::string(":0..")
        + std::to_string((*Vc.gmsSST)[Leader].globalMin[n]) + std::string(" ");
        max_received_indices[n] = (*Vc.gmsSST)[Leader].globalMin[n];
    }
	debug_log().log_event("Delivering ragged-edge messages in order: " + deliveryOrder);
//    std::cout << "Delivery Order (View " << Vc.vid << ") {" << deliveryOrder << std::string("}") << std::endl;
    Vc.rdmc_sending_group->deliver_messages_upto(max_received_indices);
}

void ManagedGroup::ragged_edge_cleanup(View& Vc) {
	debug_log().log_event("Running RaggedEdgeCleanup");
    if(Vc.IAmLeader()) {
        leader_ragged_edge_cleanup(Vc);
    } else {
        follower_ragged_edge_cleanup(Vc);
    }
	debug_log().log_event("Done with RaggedEdgeCleanup");
}

void ManagedGroup::leader_ragged_edge_cleanup(View& Vc) {
    int myRank = Vc.my_rank;
    int Leader = Vc.rank_of_leader(); // We don't want this to change under our feet
    bool found = false;
    for (int n = 0; n < Vc.num_members && !found; n++)
    {
        if ((*Vc.gmsSST)[n].globalMinReady)
        {

            gmssst::set((*Vc.gmsSST)[myRank].globalMin, (*Vc.gmsSST)[n].globalMin, Vc.num_members);
            found = true;
        }
    }

    if (!found)
    {
        for (int n = 0; n < Vc.num_members; n++)
        {
            int min = (*Vc.gmsSST)[myRank].nReceived[n];
            for (int r = 0; r < Vc.num_members; r++)
            {
                if (/*!Vc.failed[r] && */min > (*Vc.gmsSST)[r].nReceived[n])
                {
                    min = (*Vc.gmsSST)[r].nReceived[n];
                }
            }

            gmssst::set((*Vc.gmsSST)[myRank].globalMin[n], min);
        }
    }

	debug_log().log_event("Leader finished computing globalMin");
    gmssst::set((*Vc.gmsSST)[myRank].globalMinReady, true);
    Vc.gmsSST->put();
//    std::cout << std::string("RaggedEdgeCleanup: FINAL = ") << Vc.ToString() << std::endl;

    deliver_in_order(Vc, Leader);
}

void ManagedGroup::follower_ragged_edge_cleanup(View& Vc) {
    int myRank = Vc.my_rank;
    // Learn the leader's data and push it before acting upon it
	debug_log().log_event("Received leader's globalMin; echoing it");
    int Leader = Vc.rank_of_leader();
    gmssst::set((*Vc.gmsSST)[myRank].globalMin, (*Vc.gmsSST)[Leader].globalMin, Vc.num_members);
    gmssst::set((*Vc.gmsSST)[myRank].globalMinReady, true);
    Vc.gmsSST->put();
//    std::cout << std::string("RaggedEdgeCleanup: FINAL = ") << Vc.ToString() << std::endl;

    deliver_in_order(Vc, Leader);
}

/* ------------------------------------------------------------------------- */

void ManagedGroup::report_failure(const node_id_t who) {
    int r = curr_view->rank_of(who);
    log_event(std::stringstream() << "Node ID " << who << " failure reported; marking suspected[" << r << "]");
    cout << "Node ID " << who << " failure reported; marking suspected[" << r << "]" << endl;
    (*curr_view->gmsSST)[curr_view->my_rank].suspected[r] = true;
	int cnt = 0;
	for (r = 0; r < View::MAX_MEMBERS; r++) {
		if ((*curr_view->gmsSST)[curr_view->my_rank].suspected[r]) {
			++cnt;
		}
	}

//	cout << "Count is " << cnt << ", my row is " << gmssst::to_string((*curr_view->gmsSST)[curr_view->my_rank]) << endl;

	if (cnt >= curr_view->num_members / 2)
	{
		throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
	}
    curr_view->gmsSST->put();
}

void ManagedGroup::leave() {
    log_event("Cleanly leaving the group.");
    curr_view->rdmc_sending_group->wedge();
    curr_view->gmsSST->delete_all_predicates();
    (*curr_view->gmsSST)[curr_view->my_rank].suspected[curr_view->my_rank] = true;
    curr_view->gmsSST->put();
    thread_shutdown = true;
}

char* ManagedGroup::get_sendbuffer_ptr(unsigned long long int payload_size, int pause_sending_turns) {
    lock_guard_t lock(view_mutex);
    return curr_view->rdmc_sending_group->get_position(payload_size, pause_sending_turns);
}

void ManagedGroup::send() {
    std::unique_lock<std::mutex> lock(view_mutex);
    while(true) {
        if(curr_view->rdmc_sending_group->send())
            break;
        view_change_cv.wait(lock);
    }
}

std::vector<node_id_t> ManagedGroup::get_members() {
    lock_guard_t lock(view_mutex);
    //Since pointer swapping is atomic, this doesn't need the view_mutex - it
    //will either get the old view's members list or the new view's
    return curr_view->members;
}

void ManagedGroup::barrier_sync() {
    lock_guard_t lock(view_mutex);
    curr_view->gmsSST->sync_with_members();
}

void ManagedGroup::debug_print_status() const {
    cout << "Member IPs by ID: {";
    for(auto& entry : member_ips_by_id) {
        cout << entry.first << " => " << entry.second << ", ";
    }
    cout << "}" << endl;
    cout << "curr_view = " << curr_view->ToString() << endl;
}

void ManagedGroup::print_log(std::ostream& output_dest) const {
	for(size_t i = 0; i < debug_log().curr_event; ++i) {
		output_dest << debug_log().times[i] << "," << debug_log().events[i] << "," << std::string("ABCDEFGHIJKLMNOPQRSTUVWXYZ")[curr_view->members[curr_view->my_rank]] << endl;
	}
}

} /* namespace derecho */
