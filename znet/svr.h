#ifndef z_SVR_H
#define z_SVR_H

#include <stdint.h>
#include <string.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/_pthread/_pthread_t.h>
#include <sys/_types/_socklen_t.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>

#include "zerror/error.h"
#include "znet/record.h"
#include "zrecord/record.h"
#include "zutils/log.h"
#include "zutils/mem.h"
#include "zepoch/epoch.h"
#include "zutils/defer.h"

#define z_IP_MAX_LEN 64
#define z_KQUEUE_BACKLOG 8
#define z_LISTEN_BACKLOG 1024

typedef z_Error z_Handle(void *attr, z_Record *record, z_Record **result);

typedef struct {
  char IP[z_IP_MAX_LEN];
  uint16_t Port;
  void *Attr;
  z_Handle *Handle;
  int64_t SvrSocket;
  int64_t KQueue;
  z_Epoch *Epoch;
} z_Svr;

void *z_IOProcess(void *ptr) {
  z_Svr *svr = (z_Svr *) ptr;
  z_ThreadIDInit(svr->Epoch->Ts);
  z_defer(z_ThreadIDDestory, svr->Epoch->Ts);
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

z_Error z_SvrInit(z_Svr *svr, const char *ip, uint16_t port, z_Epoch *epoch, void *attr,
                  z_Handle *handle) {
  memset(svr->IP, 0, z_IP_MAX_LEN);
  strncpy(svr->IP, ip, z_IP_MAX_LEN - 1);
  svr->Port = port;
  svr->Attr = attr;
  svr->Handle = handle;
  svr->Epoch = epoch;

  svr->SvrSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (svr->SvrSocket < 0) {
    z_error("svr->SvrSocket < 0 %lld", svr->SvrSocket);
    return z_ERR_NET;
  }

  struct sockaddr_in svr_ipv4;
  memset(&svr_ipv4, 0, sizeof(svr_ipv4));
  svr_ipv4.sin_family = AF_INET;
  svr_ipv4.sin_addr.s_addr = inet_addr(svr->IP);
  svr_ipv4.sin_port = htons(svr->Port);

  if (bind(svr->SvrSocket, (struct sockaddr *)&svr_ipv4, sizeof(svr_ipv4)) <
      0) {
    z_error("bind failed");
    return z_ERR_NET;
  }

  if (listen(svr->SvrSocket, z_LISTEN_BACKLOG) < 0) {
    z_error("listen failed");
    return z_ERR_NET;
  }

  // Create kqueue
  svr->KQueue = kqueue();
  if (svr->KQueue < 0) {
    z_error("kqueue creation failed %lld", svr->KQueue);
    return z_ERR_NET;
  }

  z_info("server started on ip %s port %d\n", svr->IP, svr->Port);
  pthread_t *pids = z_malloc(sizeof(pthread_t) * svr->Epoch->Ts->Len);
  z_defer(^(pthread_t * ps) {
    if (ps != nullptr) {
      z_free(ps);
    }
  }, pids);
  for (int64_t i = 0; i < svr->Epoch->Ts->Len; ++i) {
    pthread_create(&pids[i], nullptr, z_IOProcess, svr);
  }

  while (1) {
    struct sockaddr cli_addr;
    socklen_t cli_addr_len = sizeof(struct sockaddr);
    int64_t cli_socket = accept(svr->SvrSocket, &cli_addr, &cli_addr_len);
    if (cli_socket < 0) {
      z_error("cli_socket < 0 %lld", cli_socket);
      return z_ERR_NET;
    }

    struct sockaddr_in *ipv4 = (struct sockaddr_in *)&cli_addr;
    z_info("new connection %s:%lld\n", inet_ntoa(ipv4->sin_addr),
           ntohs(ipv4->sin_port));

    struct kevent ev;
    EV_SET(&ev, cli_socket, EVFILT_READ, EV_ADD, 0, 0, NULL);
    if (kevent(svr->KQueue, &ev, 1, NULL, 0, NULL) < 0) {
      z_error("registering client socket for read events failed");
    }
  }

  return z_OK;
}

#endif