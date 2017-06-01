/*
 * view.cpp
 *
 *  Created on: Apr 15, 2016
 *      Author: edward
 */

#include <memory>
#include <string>
#include <iostream>
#include <sstream>

#include "view.h"

namespace derecho {

using std::string;
using std::shared_ptr;

View::View() : View(0) {}

View::View(int num_members) : vid(0), members(num_members), member_ips(num_members),
        failed(num_members), nFailed(0), who(nullptr), num_members(num_members),
        my_rank(0), rdmc_sending_group(nullptr),  gmsSST(nullptr) {}

void View::init_vectors() {
    members.resize(num_members);
    member_ips.resize(num_members);
    failed.resize(num_members);
}

int View::rank_of_leader() const {
    for(int r = 0; r < num_members; ++r) {
        if(!failed[r]) {
            return r;
        }
    }
    return -1;
}

int View::rank_of(const ip_addr& who) const {
    for(int rank = 0; rank < num_members; ++rank) {
        if(member_ips[rank] == who) {
            return rank;
        }
    }
    return -1;
}

int View::rank_of(const node_id_t& who) const {
    for(int rank = 0; rank < num_members; ++rank) {
        if(members[rank] == who) {
            return rank;
        }
    }
    return -1;
}

void View::newView(const View& Vc) {
    //I don't know what this is supposed to do in real life. In the C# simulation it just prints to stdout.
//    std::cout <<"Process " << Vc.members[Vc.my_rank] << " New view: " <<  Vc.ToString() << std::endl;
}


bool View::IAmLeader() const {
    return (rank_of_leader() == my_rank); // True if I know myself to be the leader
}


shared_ptr<node_id_t> View::Joined() const {
    if (who == nullptr) {
        return shared_ptr<node_id_t>();
    }
    for (int r = 0; r < num_members; r++) {
        if (members[r] == *who) {
            return who;
        }
    }
    return shared_ptr<node_id_t>();
}

shared_ptr<node_id_t> View::Departed() const {
    if (who == nullptr) {
        return shared_ptr<node_id_t>();
    }
    for (int r = 0; r < num_members; r++) {
        if (members[r] == *who) {
            return shared_ptr<node_id_t>();
        }
    }
    return who;
}

std::string View::ToString() const {
    std::stringstream s;
    s << "View " << vid << ": MyRank=" << my_rank << ". ";
    string ms = " ";
    for (int m = 0; m < num_members; m++) {
        ms += std::to_string(members[m]) + string("  ");
    }

    s << "Members={" << ms << "}, ";
    string fs = (" ");
    for (int m = 0; m < num_members; m++) {
        fs += failed[m] ? string(" T ") : string(" F ");
    }

    s << "Failed={" << fs << " }, nFailed=" << nFailed;
    shared_ptr<node_id_t> dep = Departed();
    if (dep != nullptr) {
        s << ", Departed: " << *dep;
    }

    shared_ptr<node_id_t> join = Joined();
    if (join != nullptr) {
        s << ", Joined: " << *join;
    }

//    s += string("\n") + gmsSST->ToString();
    return s.str();
}


}
