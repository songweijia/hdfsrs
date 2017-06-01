
#include "connection.h"

#include <arpa/inet.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>

namespace tcp {

using namespace std;

socket::socket(string servername, int port) {
    sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if(sock < 0) throw connection_failure();

    hostent *server;
    server = gethostbyname(servername.c_str());
    if(server == nullptr) throw connection_failure();

    char server_ip_cstr[server->h_length];
    inet_ntop(AF_INET, server->h_addr, server_ip_cstr, server->h_length);
    remote_ip = string(server_ip_cstr);

    sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr,
          server->h_length);

    while(connect(sock, (sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        /* do nothing*/;
}
socket::socket(socket &&s) : sock(s.sock), remote_ip(s.remote_ip) {
    s.sock = -1;
    s.remote_ip = std::string();
}

socket &socket::operator=(socket &&s) {
    sock = s.sock;
    s.sock = -1;
    remote_ip = std::move(s.remote_ip);
    return *this;
}

socket::~socket() {
    if(sock >= 0) close(sock);
}

bool socket::is_empty() { return sock == -1; }

bool socket::read(char *buffer, size_t size) {
    if(sock < 0) {
        fprintf(stderr, "WARNING: Attempted to read from closed socket\n");
        return false;
    }

    size_t total_bytes = 0;
    while(total_bytes < size) {
        ssize_t new_bytes =
            ::read(sock, buffer + total_bytes, size - total_bytes);
        if(new_bytes >= 0)
            total_bytes += new_bytes;
        else {
            cout << "Read error! Errno = " << errno << endl;
            return false;
        }
    }
    return true;
}

bool socket::write(const char *buffer, size_t size) {
    if(sock < 0) {
        fprintf(stderr, "WARNING: Attempted to write to closed socket\n");
        return false;
    }

    size_t total_bytes = 0;
    while(total_bytes < size) {
        ssize_t bytes_written =
            ::write(sock, buffer + total_bytes, size - total_bytes);
        if(bytes_written >= 0) {
            total_bytes += bytes_written;
        } else {
            return false;
        }
    }
    return true;
}

connection_listener::connection_listener(int port) {
    sockaddr_in serv_addr;

    int listenfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if(listenfd < 0) throw connection_failure();

    int reuse_addr = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse_addr,
               sizeof(reuse_addr));

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);
    if(bind(listenfd, (sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        fprintf(stderr, "ERROR on binding\n");
    listen(listenfd, 5);

    fd = unique_ptr<int, std::function<void(int *)>>(
        new int(listenfd), [](int *fd) { close(*fd); });
}

socket connection_listener::accept() {
    char client_ip_cstr[INET6_ADDRSTRLEN + 1];
    struct sockaddr_storage client_addr_info;
    socklen_t len = sizeof client_addr_info;

    int sock = ::accept(*fd, (struct sockaddr *)&client_addr_info, &len);
    if(sock < 0) throw connection_failure();

    if(client_addr_info.ss_family == AF_INET) {
        // Client has an IPv4 address
        struct sockaddr_in *s = (struct sockaddr_in *)&client_addr_info;
        inet_ntop(AF_INET, &s->sin_addr, client_ip_cstr, sizeof client_ip_cstr);
    } else {  // AF_INET6
        // Client has an IPv6 address
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&client_addr_info;
        inet_ntop(AF_INET6, &s->sin6_addr, client_ip_cstr,
                  sizeof client_ip_cstr);
    }

    return socket(sock, std::string(client_ip_cstr));
}
}
