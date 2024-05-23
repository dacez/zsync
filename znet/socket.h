#ifndef z_SOCKET_H
#define z_SOCKET_H

#include "zerror/error.h"
#include "zutils/assert.h"
#include "zutils/log.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <unistd.h> // close()

#define z_IP_MAX_LEN 64
#define z_KQUEUE_BACKLOG 8
#define z_LISTEN_BACKLOG 1024

#define z_INVALID_SOCKET -1

typedef struct sockaddr z_SockAddr;

void z_SockAddrFromStr(z_SockAddr *addr, const char *ip, uint16_t port) {
  z_assert(addr != nullptr, ip != nullptr, port != 0);
  struct sockaddr_in *ipv4 = (struct sockaddr_in *)addr;
  ipv4->sin_family = AF_INET;
  ipv4->sin_addr.s_addr =
      strncmp("", ip, z_IP_MAX_LEN) == 0 ? INADDR_ANY : inet_addr(ip);
  ipv4->sin_port = htons(port);
}

typedef struct {
  int64_t FD;
} z_Socket;

z_Error z_SocketInit(z_Socket *s) {
  s->FD = socket(AF_INET, SOCK_STREAM, 0);
  if (s->FD < 0) {
    z_error("socket failed %lld", s->FD);
    s->FD = z_INVALID_SOCKET;
    return z_ERR_NET;
  }
  return z_OK;
}

z_Error z_SocketCliInit(z_Socket *s, const char *ip, uint16_t port) {
  z_Error ret = z_SocketInit(s);
  if (ret != z_OK) {
    z_error("z_SocketInit failed %d", ret);
    return ret;
  }

  z_SockAddr addr;
  z_SockAddrFromStr(&addr, ip, port);
  if (connect(s->FD, &addr, sizeof(addr)) != 0) {
    z_error("connect failed %s:%u", ip, port);
    return z_ERR_NET;
  }

  return z_OK;
}

z_Error z_SocketSvrInit(z_Socket *s, const char *ip, uint16_t port) {
  z_Error ret = z_SocketInit(s);
  if (ret != z_OK) {
    z_error("z_SocketInit failed %d", ret);
    return ret;
  }

  z_SockAddr addr;
  z_SockAddrFromStr(&addr, ip, port);
  if (bind(s->FD, &addr, sizeof(addr)) < 0) {
    z_error("bind failed");
    return z_ERR_NET;
  }

  if (listen(s->FD, z_LISTEN_BACKLOG) < 0) {
    z_error("listen failed");
    return z_ERR_NET;
  }

  z_debug("listen IP %s Port %d", ip, port);

  return z_OK;
}

z_Error z_SocketAccept(const z_Socket *svr, z_Socket *cli) {
  z_SockAddr cli_addr;
  socklen_t cli_addr_len = sizeof(cli_addr);
  int64_t cli_socket = accept(svr->FD, &cli_addr, &cli_addr_len);
  if (cli_socket < 0) {
    z_error("cli_socket < 0 %lld", cli_socket);
    return z_ERR_NET;
  }

  cli->FD = cli_socket;
  return z_OK;
}

void z_SocketDestroy(z_Socket *s) {
  if (s != nullptr && s->FD != z_INVALID_SOCKET) {
    close(s->FD);
    s->FD = z_INVALID_SOCKET;
  }
}
#endif