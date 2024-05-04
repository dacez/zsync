#ifndef z_SVR_H
#define z_SVR_H

#include <stdint.h>
#include <string.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <unistd.h>

#include "zepoch/epoch.h"
#include "zerror/error.h"
#include "znet/record.h"
#include "zrecord/record.h"
#include "zutils/defer.h"
#include "zutils/log.h"
#include "zutils/mem.h"

#define z_IP_MAX_LEN 64
#define z_KQUEUE_BACKLOG 8
#define z_LISTEN_BACKLOG 1024

typedef z_Error z_Handle(void *attr, z_Record *record, z_Record **result);

typedef struct sockaddr_in ipv4_addr;
typedef struct sockaddr sock_addr;

typedef struct {
  char IP[z_IP_MAX_LEN];
  uint16_t Port;
  void *Attr;
  z_Handle *Handle;
  int64_t KQueue;
  z_Epoch *Epoch;
} z_Svr;

void *z_IOProcess(void *ptr) {
  if (ptr == nullptr) {
    z_error("ptr == nullptr");
    return nullptr;
  }
  z_Svr *svr = (z_Svr *)ptr;
  z_ThreadIDInit(svr->Epoch->Ts);
  z_defer(z_ThreadIDDestory, svr->Epoch->Ts);

  z_info("IOThread Start %lld", z_ThreadID());
  while (1) {
    struct kevent events[z_KQUEUE_BACKLOG] = {};
    int64_t ev_count =
        kevent(svr->KQueue, NULL, 0, events, z_KQUEUE_BACKLOG, NULL);
    if (ev_count < 0) {
      z_error("kevent failed");
      break;
    }
    for (int64_t i = 0; i < ev_count; ++i) {
      z_Req *req = nullptr;
      z_defer(z_ReqDestory, req);

      int64_t cli_socket = events[i].ident;

      z_Error ret = z_ReqInitFromNet(cli_socket, &req);
      if (ret != z_OK) {
        close(cli_socket);
        continue;
      }

      z_Record *resp = nullptr;
      z_defer(z_RecordFree, resp);
      ret = svr->Handle(svr->Attr, &req->Record, &resp);
      if (ret != z_OK) {
        z_debug("handle error %d", ret);
      }

      ret = z_RecordToNet(cli_socket, ret, resp);
      if (ret != z_OK) {
        close(cli_socket);
        continue;
      }
    }
  }

  return nullptr;
}

z_Error z_SvrRun(z_Svr *svr, const char *ip, uint16_t port, z_Epoch *epoch,
                 void *attr, z_Handle *handle) {
  if (svr == nullptr || port == 0 || epoch == nullptr || handle == nullptr) {
    z_error(
        "svr == nullptr || port == 0 || epoch == nullptr || handle == nullptr");
    return z_ERR_INVALID_DATA;
  }
  memset(svr->IP, 0, z_IP_MAX_LEN);
  strncpy(svr->IP, ip, z_IP_MAX_LEN - 1);
  svr->Port = port;
  svr->Attr = attr;
  svr->Handle = handle;
  svr->Epoch = epoch;

  int64_t svr_sock = socket(AF_INET, SOCK_STREAM, 0);
  if (svr_sock < 0) {
    z_error("svr_sock < 0 %lld", svr_sock);
    return z_ERR_NET;
  }

  ipv4_addr svr_ip;
  memset(&svr_ip, 0, sizeof(svr_ip));
  svr_ip.sin_family = AF_INET;
  svr_ip.sin_addr.s_addr =
      strncmp("", svr->IP, z_IP_MAX_LEN) == 0 ? INADDR_ANY : inet_addr(svr->IP);
  svr_ip.sin_port = htons(svr->Port);

  if (bind(svr_sock, (sock_addr *)&svr_ip, sizeof(svr_ip)) < 0) {
    z_error("bind failed");
    return z_ERR_NET;
  }

  if (listen(svr_sock, z_LISTEN_BACKLOG) < 0) {
    z_error("listen failed");
    return z_ERR_NET;
  }

  // Create kqueue
  svr->KQueue = kqueue();
  if (svr->KQueue < 0) {
    z_error("kqueue creation failed %lld", svr->KQueue);
    return z_ERR_NET;
  }

  z_info("server started on %s:%d", svr->IP, svr->Port);
  pthread_t *pids = z_malloc(sizeof(pthread_t) * svr->Epoch->Ts->Len);
  z_defer(
      ^(pthread_t *ps) {
        if (ps != nullptr) {
          z_free(ps);
        }
      },
      pids);

  for (int64_t i = 0; i < svr->Epoch->Ts->Len; ++i) {
    pthread_create(&pids[i], nullptr, z_IOProcess, svr);
  }

  while (1) {
    sock_addr cli_addr;
    socklen_t cli_addr_len = sizeof(sock_addr);
    int64_t cli_socket = accept(svr_sock, &cli_addr, &cli_addr_len);
    if (cli_socket < 0) {
      z_error("cli_socket < 0 %lld", cli_socket);
      continue;
    }

    ipv4_addr *ipv4 = (ipv4_addr *)&cli_addr;
    z_info("new connection %s:%u", inet_ntoa(ipv4->sin_addr),
           ntohs(ipv4->sin_port));

    struct kevent ev;
    EV_SET(&ev, cli_socket, EVFILT_READ, EV_ADD, 0, 0, NULL);
    if (kevent(svr->KQueue, &ev, 1, NULL, 0, NULL) < 0) {
      z_error("registering client socket for read events failed");
    }
  }

  for (int64_t i = 0; i < svr->Epoch->Ts->Len; ++i) {
    pthread_join(pids[i], nullptr);
  }

  close(svr_sock);
  return z_OK;
}

#endif