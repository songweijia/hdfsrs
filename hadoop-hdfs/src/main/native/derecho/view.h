/*
 * view.h
 *
 *  Created on: Apr 14, 2016
 *      Author: edward
 */

#ifndef VIEW_H_
#define VIEW_H_

#include <cstdint>
#include <memory>
#include <vector>
#include <mutex>

#include "derecho_group.h"
#include "sst/sst.h"
#include "derecho_row.h"
#include "max_members.h"

namespace derecho {


class View {
    public:
        /** Upper bound on the number of members that will ever be in any one view. */
  static constexpr int MAX_MEMBERS = ::MAX_MEMBERS;

        using DerechoSST = sst::SST<DerechoRow<MAX_MEMBERS>>;

        /** Sequential view ID: 0, 1, ... */
        int vid;
        /** Node IDs of members in the current view, indexed by their SST rank. */
        std::vector<node_id_t> members;
        /** IP addresses of members in the current view, indexed by their SST rank. */
        std::vector<ip_addr> member_ips;
        /** failed[i] is true if members[i] is considered to have failed.
         * Once a member is failed, it will be removed from the members list in a future view. */
        std::vector<bool> failed;
        /** Number of current outstanding failures in this view. After
         * transitioning to a new view that excludes a failed member, this count
         * will decrease by one. */
        int nFailed;
        /** ID of the node that joined or departed since the prior view; null if this is the first view */
        std::shared_ptr<node_id_t> who;
        /** Number of members in this view */
        int num_members;
        /** For member p, returns rankOf(p) */
        int my_rank;
        /** RDMC manager object containing one RDMC group for each sender */
        std::unique_ptr<DerechoGroup<MAX_MEMBERS>> rdmc_sending_group;
        std::shared_ptr<DerechoSST> gmsSST;

        /** Creates a completely empty new View. Vectors such as members will
         * be empty (size 0), so the client will need to resize them. */
        View();
        /** Creates an empty new View with num_members members.
         * The vectors will have room for num_members elements. */
        View(int num_members);
        void newView(const View& Vc);
        /** When constructing a View piecemeal, call this after num_members has been set. */
        void init_vectors();

        int rank_of(const ip_addr& who) const;
        int rank_of(const node_id_t& who) const;
        int rank_of_leader() const;

        bool IKnowIAmLeader = false; // I am the leader (and know it)

        bool IAmLeader() const;

        /** Returns a pointer to the (IP address of the) member who recently joined,
         * or null if the most recent change was not a join. */
        std::shared_ptr<node_id_t> Joined() const;
        /** Returns a pointer to the (IP address of the) member who recently departed,
         * or null if the most recent change was not a departure. */
        std::shared_ptr<node_id_t> Departed() const;

        std::string ToString() const;
};

}



#endif /* VIEW_H_ */
