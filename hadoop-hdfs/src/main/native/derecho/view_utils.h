/*
 * view_utils.h
 *
 *  Created on: Apr 25, 2016
 *      Author: edward
 */

#ifndef VIEW_UTILS_H_
#define VIEW_UTILS_H_

#include <vector>

#include "view.h"

namespace derecho {

bool IAmTheNewLeader(View& Vc);

void merge_changes(View& Vc);

void wedge_view(View& Vc);

} //namespace derecho


#endif /* VIEW_UTILS_H_ */
