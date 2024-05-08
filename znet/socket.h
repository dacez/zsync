#ifndef z_SOCKET_H
#define z_SOCKET_H

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <unistd.h> // close()

#define z_IP_MAX_LEN 64
#define z_KQUEUE_BACKLOG 8
#define z_LISTEN_BACKLOG 1024

#define z_INVALID_SOCKET -1

typedef struct sockaddr_in ipv4_addr;
typedef struct sockaddr sock_addr;

typedef struct {
  int FD;
} z_Socket;

int64_t z_SocketRead();
int64_t z_SocketWrite();
#endif