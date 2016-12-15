#ifndef TCP_H
#define TCP_H

/** 
 * @file tcp.h
 * Declares public functions for setting up and using TCP connections between
 * the nodes in the %SST group.
 */

#include <map>
#include <string>

namespace sst {

/**
 * Everything related to setting up and using TCP connections between the nodes
 * in the %SST group.
 */
namespace tcp {

  /** Gets the TCP socket used to communicate with a given node. */
  int get_socket(uint32_t rank);
  /** Initializes the TCP subsystem of %SST, which is necessary for out-of-band communications. */
  void tcp_initialize(uint32_t node_rank, const std::map <uint32_t, std::string> & ip_addrs);
  /** Exchanges a byte of data with the given node over TCP; blocks until the remote node responds with its own sync(). */
  void sync (uint32_t r_index);
  /** Exchanges data with a remote node using a TCP socket. */
  int sock_sync_data(int sock, int xfer_size, char *local_data, char *remote_data);

} //namespace tcp

} //namespace sst

#endif
