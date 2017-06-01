/*
 * view_utils.cpp
 *
 *  Created on: Apr 25, 2016
 *      Author: edward
 */

#include <vector>
#include <cstring>

#include "view_utils.h"
#include "view.h"

namespace derecho {

bool IAmTheNewLeader(View& Vc) {
    if (Vc.IKnowIAmLeader) {
        return false; // I am the OLD leader
    }

    for (int n = 0; n < Vc.my_rank; n++) {
        for (int row = 0; row < Vc.my_rank; row++) {
            if (!Vc.failed[n] && !(*Vc.gmsSST)[row].suspected[n]) {
                return false; // I'm not the new leader, or some failure suspicion hasn't fully propagated
            }
        }
    }
    Vc.IKnowIAmLeader = true;
    return true;
}

void merge_changes(View& Vc) {
    int myRank = Vc.my_rank;
    // Merge the change lists
    for (int n = 0; n < Vc.num_members; n++) {
        if ((*Vc.gmsSST)[myRank].nChanges < (*Vc.gmsSST)[n].nChanges) {
            gmssst::set((*Vc.gmsSST)[myRank].changes, (*Vc.gmsSST)[n].changes);
            gmssst::set((*Vc.gmsSST)[myRank].nChanges, (*Vc.gmsSST)[n].nChanges);
        }

        if ((*Vc.gmsSST)[myRank].nCommitted < (*Vc.gmsSST)[n].nCommitted) // How many I know to have been committed
        {
            gmssst::set((*Vc.gmsSST)[myRank].nCommitted, (*Vc.gmsSST)[n].nCommitted);
        }
    }
    bool found = false;
    for (int n = 0; n < Vc.num_members; n++) {
        if (Vc.failed[n]) {
            // Make sure that the failed process is listed in the Changes vector as a proposed change
            for (int c = (*Vc.gmsSST)[myRank].nCommitted; c < (*Vc.gmsSST)[myRank].nChanges && !found; c++) {
                if ((*Vc.gmsSST)[myRank].changes[c % View::MAX_MEMBERS] == Vc.members[n]) {
                    // Already listed
                    found = true;
                }
            }
        } else {
            // Not failed
            found = true;
        }

        if (!found) {
            gmssst::set((*Vc.gmsSST)[myRank].changes[(*Vc.gmsSST)[myRank].nChanges % View::MAX_MEMBERS], Vc.members[n]);
            gmssst::increment((*Vc.gmsSST)[myRank].nChanges);
        }
    }
    Vc.gmsSST->put();
//    Vc.gmsSST->Push(Vc.myRank, Vc.vid);
}

void wedge_view(View& Vc) {
    Vc.rdmc_sending_group->wedge(); // RDMC finishes sending, stops new sends or receives in Vc
    gmssst::set((*Vc.gmsSST)[Vc.my_rank].wedged, true);
    Vc.gmsSST->put();
}

} //namespace derecho

