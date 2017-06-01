#ifndef DERECHO_GROUP_H
#define DERECHO_GROUP_H

#include <functional>
#include <experimental/optional>
#include <mutex>
#include <condition_variable>
#include <tuple>
#include <map>
#include <set>
#include <queue>
#include <vector>
#include <memory>

#include "derecho_row.h"
#include "rdmc/rdmc.h"
#include "sst/sst.h"

namespace derecho {

using std::vector;

typedef std::function<void(int, long long int, char*, long long int)> message_callback;

struct __attribute__ ((__packed__)) header {
        uint32_t header_size;
        uint32_t pause_sending_turns;
};

struct MessageBuffer {
        std::unique_ptr<char[]> buffer;
        std::shared_ptr<rdma::memory_region> mr;

        MessageBuffer(){}
  MessageBuffer(size_t size) {
    if (size != 0) {
      buffer = std::unique_ptr<char[]>(new char[size]);
      // std::cout << "Creating a new memory region :-/" << std::endl;
      mr = std::make_shared<rdma::memory_region>(buffer.get(), size);
    }
  }
	MessageBuffer(const MessageBuffer&) = delete;
	MessageBuffer(MessageBuffer&&) = default;
	MessageBuffer& operator=(const MessageBuffer&) = delete;
	MessageBuffer& operator=(MessageBuffer&&) = default;
};

struct msg_info {
        int sender_rank;
        long long int index;
        long long unsigned int size;
        MessageBuffer message_buffer;
};

/**
 * SST row state variables needed to track message completion status in this group.
 */
struct MessageTrackingRow {
        /** Sequence numbers are interpreted like a row-major pair:
         * (sender, counter) becomes sender + num_members * counter.
         * Since the global order is round-robin, the correct global order of
         * messages becomes a consecutive sequence of these numbers: with 4
         * senders, we expect to receive (0,0), (1,0), (2,0), (3,0), (0,1),
         * (1,1), ... which is 0, 1, 2, 3, 4, 5, ....
         *
         * This variable is the highest sequence number that has been received
         * in-order by this node; if a node updates seq_num, it has received all
         * messages up to seq_num in the global round-robin order. */
        long long int seq_num;
        /** This represents the highest sequence number that has been received
         * by every node, as observed by this node. If a node updates stable_num,
         * then it believes that all messages up to stable_num in the global
         * round-robin order have been received by every node. */
        long long int stable_num;
        /** This represents the highest sequence number that has been delivered
         * at this node. Messages are only delievered once stable, so it must be
         * at least stable_num. */
        long long int delivered_num;
};

/** combines sst and rdmc to give an abstraction of a group where anyone can send
 * template parameter is the maximum possible group size - used for the GMS SST row-struct */
template<unsigned int N>
class DerechoGroup {
	    /** vector of member id's */
        std::vector<node_id_t> members;
        /**  number of members */
        const int num_members;
        /** index of the local node in the members vector, which should also be its row index in the SST */
        const int member_index;
        /** Block size used for message transfer.
         * we keep it simple; one block size for messages from all senders */
        const long long unsigned int block_size;
        // maximum size of any message that can be sent
        const long long unsigned int max_msg_size;
        /** Send algorithm for constructing a multicast from point-to-point unicast.
         *  Binomial pipeline by default. */
        const rdmc::send_algorithm type;
        const unsigned int window_size;
        /** callback for when a message is globally stable */
        const message_callback global_stability_callback;
        /** Offset to add to member ranks to form RDMC group numbers. */
        const uint16_t rdmc_group_num_offset;
        unsigned int total_message_buffers;
        /** Stores message buffers not currently in use. Protected by 
         * msg_state_mtx */
        std::vector<MessageBuffer> free_message_buffers;
	
        // int send_slot;
        // vector<int> recv_slots;
        // /** buffers to store incoming/outgoing messages */
        // std::vector<std::unique_ptr<char[]> > buffers;
        // /** memory regions wrapping the buffers for RDMA ops */
        // std::vector<std::shared_ptr<rdma::memory_region> > mrs;

        /** Index to be used the next time get_position is called.
         * When next_message is not none, then next_message.index = future_message_index-1 */
        long long int future_message_index = 0;
	
        /** next_message is the message that will be sent when send is called the next time.
         * It is boost::none when there is no message to send. */
	    std::experimental::optional<msg_info> next_send;
        /** Messages that are ready to be sent, but must wait until the current send finishes. */
        std::queue<msg_info> pending_sends;
	    /** The message that is currently being sent out using RDMC, or boost::none otherwise. */
        std::experimental::optional<msg_info> current_send;

        /** Messages that are currently being received. */
        std::map<long long int, msg_info> current_receives;

        /** Messages that have finished sending/receiving but aren't yet globally stable */
        std::map<long long int, msg_info> locally_stable_messages;
        long long int next_message_to_deliver = 0;
        std::mutex msg_state_mtx;
        std::condition_variable sender_cv;

        /** The time, in milliseconds, that a sender can wait to send a message before it is considered failed. */
        unsigned int sender_timeout;

        /** Indicates that the group is being destroyed. */
        std::atomic<bool> thread_shutdown{false};
        /** The background thread that sends messages with RDMC. */
        std::thread sender_thread;

        std::thread timeout_thread;

        /** The SST, shared between this group and its GMS. */
        std::shared_ptr<sst::SST<DerechoRow<N>>> sst;

        using pred_handle = typename sst::SST<DerechoRow<N>>::Predicates::pred_handle;
        pred_handle stability_pred_handle;
        pred_handle delivery_pred_handle;
        pred_handle sender_pred_handle;

        /** Continuously waits for a new pending send, then sends it. This function implements the sender thread. */
        void send_loop();

        /** Checks for failures when a sender reaches its timeout. This function implements the timeout thread. */
        void check_failures_loop();

		void create_rdmc_groups();
		void initialize_sst_row();
		void register_predicates();
        void deliver_message(msg_info& msg);

    public:
        // the constructor - takes the list of members, send parameters (block size, buffer size), K0 and K1 callbacks
        DerechoGroup(std::vector<node_id_t> _members, node_id_t my_node_id, std::shared_ptr<sst::SST<DerechoRow<N>, sst::Mode::Writes>> _sst,
                std::vector<MessageBuffer>& free_message_buffers,
                long long unsigned int _max_payload_size, message_callback global_stability_callback, long long unsigned int _block_size,
                unsigned int _window_size = 3, unsigned int timeout_ms = 1, rdmc::send_algorithm _type = rdmc::BINOMIAL_SEND);
        /** Constructor to initialize a new derecho_group from an old one, preserving the same settings but providing a new list of members. */
        DerechoGroup(std::vector<node_id_t> _members, node_id_t my_node_id, std::shared_ptr<sst::SST<DerechoRow<N>, sst::Mode::Writes>> _sst, DerechoGroup&& old_group);
        ~DerechoGroup();
        void deliver_messages_upto(const std::vector<long long int>& max_indices_for_senders);
        /** get a pointer into the buffer, to write data into it before sending */
        char* get_position(long long unsigned int payload_size, int pause_sending_turns = 0);
        /** Note that get_position and send are called one after the another - regexp for using the two is (get_position.send)*
         * This still allows making multiple send calls without acknowledgement; at a single point in time, however,
         * there is only one message per sender in the RDMC pipeline */
        bool send();
        /** Stops all sending and receiving in this group, in preparation for shutting it down. */
        void wedge();
        /** Debugging function; prints the current state of the SST to stdout. */
        void debug_print();
        static long long unsigned int compute_max_msg_size(const long long unsigned int max_payload_size, const long long unsigned int block_size);
};
} //namespace derecho

#include "derecho_group_impl.h"

#endif /* DERECHO_GROUP_H */
