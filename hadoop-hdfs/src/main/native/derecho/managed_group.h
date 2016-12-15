#ifndef MANAGED_GROUP_H_
#define MANAGED_GROUP_H_

#include <mutex>
#include <list>
#include <queue>
#include <string>
#include <utility>
#include <map>
#include <vector>
#include <ctime>
#include <chrono>

#include "view.h"
#include "logger.h"
#include "rdmc/connection.h"

namespace derecho {

struct derecho_exception : public std::exception {
    public:
        const std::string message;
        derecho_exception(const std::string& message) : message(message) {}

        const char * what() const noexcept {
            return message.c_str();
        }
};

template<typename T>
class LockedQueue {
    private:
        using unique_lock_t = std::unique_lock<std::mutex>;
        std::mutex mutex;
        std::list<T> underlying_list;
    public:
        struct LockedListAccess {
            private:
                unique_lock_t lock;
            public:
                std::list<T> &access;
                LockedListAccess(std::mutex& m, std::list<T>& a) :
                    lock(m), access(a) {};
        };
        LockedListAccess locked() {
            return LockedListAccess{ mutex, underlying_list };
        }
};

class ManagedGroup {
    private:

        using pred_handle = View::DerechoSST::Predicates::pred_handle;

        /** Maps node IDs (what RDMC/SST call "ranks") to IP addresses.
         * Currently, this mapping must be completely known at startup. */
        std::map<node_id_t, ip_addr> member_ips_by_id;
        static bool rdmc_globals_initialized;

        /** Contains client sockets for all pending joins, except the current one.*/
        LockedQueue<tcp::socket> pending_joins;

        /** Contains old Views that need to be cleaned up*/
        std::queue<std::unique_ptr<View>> old_views;
        std::mutex old_views_mutex;
        std::condition_variable old_views_cv;

        /** The socket connected to the client that is currently joining, if any */
        tcp::socket joining_client_socket;
        /** The node ID that has been assigned to the client that is currently joining, if any. */
        node_id_t joining_client_id;
		/** A cached copy of the last known value of this node's suspected[] array.
		 * Helps the SST predicate detect when there's been a change to suspected[].*/ 
		std::vector<bool> last_suspected;

		/** The port that this instance of the GMS communicates on. */
        const int gms_port;

        tcp::connection_listener server_socket;
        /** A flag to signal background threads to shut down; set to true when the group is destroyed. */
        std::atomic<bool> thread_shutdown;
        /** The background thread that listens for clients connecting on our server socket. */
        std::thread client_listener_thread;
        std::thread old_view_cleanup_thread;

        //Handles for all the predicates the GMS registered with the current view's SST.
        pred_handle suspected_changed_handle;
        pred_handle start_join_handle;
        pred_handle change_commit_ready_handle;
        pred_handle leader_proposed_handle;
        pred_handle leader_committed_handle;

        /** Sends a joining node the new view that has been constructed to include it.*/
        void commit_join(const View& new_view, tcp::socket& client_socket);

        bool has_pending_join(){
            return pending_joins.locked().access.size() > 0;
        }

        /** Assuming this node is the leader, handles a join request from a client.*/
        void receive_join(tcp::socket& client_socket);

        /** Starts a new Derecho group with this node as the only member, and initializes the GMS. */
        std::unique_ptr<View> start_group(const node_id_t my_id);
        /** Joins an existing Derecho group, initializing this object to participate in its GMS. */
        std::unique_ptr<View> join_existing(const ip_addr& leader_ip, const int leader_port);

        //Ken's helper methods
        void deliver_in_order(const View& Vc, int Leader);
        void ragged_edge_cleanup(View& Vc);
        void leader_ragged_edge_cleanup(View& Vc);
        void follower_ragged_edge_cleanup(View& Vc);

        static bool suspected_not_equal(const View::DerechoSST& gmsSST, const std::vector<bool>& old);
        static void copy_suspected(const View::DerechoSST& gmsSST, std::vector<bool>& old);
        static bool changes_contains(const View::DerechoSST& gmsSST, const node_id_t q);
        static int min_acked(const View::DerechoSST& gmsSST, const std::vector<bool>& failed);

		/** Constructor helper method to encapsulate creating all the predicates. */
		void register_predicates();

        /** Creates the SST and derecho_group for the current view, using the current view's member list.
         * The parameters are all the possible parameters for constructing derecho_group. */
        void setup_sst_and_rdmc(std::vector<MessageBuffer>& message_buffers, long long unsigned int _max_payload_size, message_callback global_stability_callback,
                long long unsigned int _block_size, unsigned int _window_size, rdmc::send_algorithm _type);
        /** Sets up the SST and derecho_group for a new view, based on the settings in the current view
         * (and copying over the SST data from the current view). */
        void transition_sst_and_rdmc(View& newView, int whichFailed);

        /** Lock this before accessing curr_view, since it's shared by multiple threads */
        std::mutex view_mutex;
        /** Notified when curr_view changes (i.e. we are finished with a pending view change).*/
        std::condition_variable view_change_cv;

        /** May hold a pointer to the partially-constructed next view, if we are
         * in the process of transitioning to a new view. */
        std::unique_ptr<View> next_view;
        std::unique_ptr<View> curr_view; //must be a pointer so we can re-assign it

    public:
        /** Constructor, starts or joins a managed Derecho group.
         * The rest of the parameters are the parameters for the derecho_group that should
         * be constructed for communications within this managed group. */
        ManagedGroup(const int gms_port, const std::map<node_id_t, ip_addr>& member_ips, node_id_t my_id, node_id_t leader_id,
                long long unsigned int _max_payload_size, message_callback global_stability_callback, long long unsigned int _block_size,
                unsigned int _window_size = 3, rdmc::send_algorithm _type = rdmc::BINOMIAL_SEND);
        ~ManagedGroup();

        static void global_setup(const std::map<node_id_t, ip_addr>& member_ips, node_id_t my_id);
        /** Causes this node to cleanly leave the group by setting itself to "failed." */
        void leave();
        /** Creates and returns a vector listing the nodes that are currently members of the group. */
  std::vector<node_id_t> get_members();
        /** Gets a pointer into the managed DerechoGroup's send buffer, at a position where
         * there are at least payload_size bytes remaining in the buffer. The returned pointer
         * can be used to write a message into the buffer. (Analogous to DerechoGroup::get_position) */
        char* get_sendbuffer_ptr(long long unsigned int payload_size, int pause_sending_turns = 0);
        /** Instructs the managed DerechoGroup to send the next message. This returns immediately;
         * the send is scheduled to happen some time in the future. */
        void send();
        /** Reports to the GMS that the given node has failed. */
        void report_failure(const node_id_t who);
        /** Waits until all members of the group have called this function. */
        void barrier_sync();
        void debug_print_status() const;
		static void log_event(const std::string& event_text) {
			util::debug_log().log_event(event_text);
		}
		static void log_event(const std::stringstream& event_text) {
		    util::debug_log().log_event(event_text);
		}
		void print_log(std::ostream& output_dest) const;

};

} /* namespace derecho */

#endif /* MANAGED_GROUP_H_ */
