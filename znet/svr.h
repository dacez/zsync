#ifndef z_SVR_H
#define z_SVR_H

#include <stdint.h>
#include <string.h>
#include <sys/event.h>
#include <unistd.h>

#include "zepoch/epoch.h"
#include "zerror/error.h"
#include "znet/net_record.h"
#include "znet/socket.h"
#include "zrecord/record.h"
#include "zutils/defer.h"
#include "zutils/log.h"
#include "zutils/mem.h"

typedef z_Error z_Handle(void *attr, z_Record *record, z_Record **result);

typedef struct {
  char IP[z_IP_MAX_LEN];
  uint16_t Port;
  void *Attr;
  z_Handle *Handle;
  int64_t *KQs;
  z_Epoch *Epoch;
} z_Svr;

z_Error z_SvrEventAdd(int64_t kq, int64_t cli_socket) {
  struct kevent ev;
  EV_SET(&ev, cli_socket, EVFILT_READ, EV_ADD, 0, 0, NULL);
  if (kevent(kq, &ev, 1, NULL, 0, NULL) < 0) {
    z_error("registering client socket for read events failed");
    return z_ERR_NET;
  }
  return z_OK;
}

z_Error z_SvrEventDel(int64_t kq, int64_t cli_socket) {
  struct kevent ev;
  EV_SET(&ev, cli_socket, EVFILT_READ, EV_DELETE, 0, 0, NULL);
  if (kevent(kq, &ev, 1, NULL, 0, NULL) < 0) {
    z_error("delete client socket for read events failed");
    return z_ERR_NET;
  }
  return z_OK;
}

void z_SvrConnectClose(int64_t kq, int64_t cli_socket) {
  if (z_SvrEventDel(kq, cli_socket) != z_OK) {
    z_error("z_SvrEventDel");
  }
  close(cli_socket);
  return;
}

void *z_IOProcess(void *ptr) {
  if (ptr == nullptr) {
    z_error("ptr == nullptr");
    return nullptr;
  }
  z_Svr *svr = (z_Svr *)ptr;
  z_ThreadIDInit(svr->Epoch->Ts);
  z_defer(z_ThreadIDDestory, svr->Epoch->Ts);

  int64_t kq = svr->KQs[z_ThreadID()];
  z_debug("IOThread Start %lld", z_ThreadID());

  while (1) {
    struct kevent events[z_KQUEUE_BACKLOG] = {};
    int64_t ev_count = kevent(kq, NULL, 0, events, z_KQUEUE_BACKLOG, NULL);
    if (ev_count < 0) {
      z_error("kevent failed");
      break;
    }

    for (int64_t i = 0; i < ev_count; ++i) {
      int64_t cli_socket = events[i].ident;

      if (events[i].flags & EV_EOF) {
        z_debug("client close connection");
        z_SvrConnectClose(kq, cli_socket);
        continue;
      }

      z_Req req = {};
      z_defer(z_ReqDestroy, &req);

      z_Error ret = z_ReqInitFromNet(cli_socket, &req);
      if (ret != z_OK) {
        z_error("z_ReqInitFromNet %d", ret);
        z_SvrConnectClose(kq, cli_socket);
        continue;
      }

      z_Resp resp = {};
      z_defer(z_RespDestroy, &resp);
      ret = svr->Handle(svr->Attr, req.Record, &resp.Record);
      if (ret != z_OK) {
        z_debug("handle error %d", ret);
      }

      resp.Ret.Code = ret;
      resp.Ret.DataLen = resp.Record != nullptr ? z_RecordLen(resp.Record) : 0;

      ret = z_RespToNet(cli_socket, &resp);
      if (ret != z_OK) {
        z_error("z_RespToNet %d", ret);
        z_SvrConnectClose(kq, cli_socket);
        continue;
      }
    }
  }

  return nullptr;
}

z_Error z_SvrRun(z_Svr *svr, const char *ip, uint16_t port, z_Epoch *epoch,
                 void *attr, z_Handle *handle) {
  if (svr == nullptr || ip == nullptr || port == 0 || epoch == nullptr ||
      handle == nullptr) {
    z_error("svr == nullptr || ip == nullptr || port == 0 || epoch == nullptr "
            "|| handle == nullptr");
    return z_ERR_INVALID_DATA;
  }
  memset(svr->IP, 0, z_IP_MAX_LEN);
  strncpy(svr->IP, ip, z_IP_MAX_LEN - 1);
  svr->Port = port;
  svr->Attr = attr;
  svr->Handle = handle;
  svr->Epoch = epoch;

  int64_t svr_sock = socket(AF_INET, SOCK_STREAM, 0);
  z_defer(
      ^(int64_t s) {
        close(s);
      },
      svr_sock);

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

  if (bind(svr_sock, (sock_addr *)&svr_ip, sizeof(ipv4_addr)) < 0) {
    z_error("bind failed");
    return z_ERR_NET;
  }

  if (listen(svr_sock, z_LISTEN_BACKLOG) < 0) {
    z_error("listen failed");
    return z_ERR_NET;
  }

  // Create kqueue
  int64_t kqs_len = svr->Epoch->Ts->Len;
  svr->KQs = z_malloc(kqs_len * sizeof(int64_t));
  z_defer(
      ^(int64_t *kqs) {
        if (kqs != nullptr) {
          z_free(kqs);
        }
      },
      svr->KQs);

  if (svr->KQs == nullptr) {
    z_error("svr->KQs == nullptr");
    return z_ERR_NOSPACE;
  }

  for (int64_t i = 0; i < kqs_len; ++i) {
    svr->KQs[i] = kqueue();
    if (svr->KQs[i] < 0) {
      z_error("svr->KQs[i] < 0");
      return z_ERR_NET;
    }
  }

  z_debug("server started on %s:%d", svr->IP, svr->Port);
  int64_t pids_len = svr->Epoch->Ts->Len;
  pthread_t *pids = z_malloc(sizeof(pthread_t) * pids_len);
  z_defer(
      ^(pthread_t *ps) {
        if (ps != nullptr) {
          z_free(ps);
        }
      },
      pids);

  if (pids == nullptr) {
    z_error("pids == nullptr");
    return z_ERR_NOSPACE;
  }

  for (int64_t i = 0; i < pids_len; ++i) {
    pthread_create(&pids[i], nullptr, z_IOProcess, svr);
  }
  z_defer(
      ^(pthread_t *ps) {
        for (int64_t i = 0; i < pids_len; ++i) {
          pthread_join(ps[i], nullptr);
        }
      },
      pids);

  while (1) {
    sock_addr cli_addr;
    socklen_t cli_addr_len = sizeof(sock_addr);
    int64_t cli_socket = accept(svr_sock, &cli_addr, &cli_addr_len);
    if (cli_socket < 0) {
      z_error("cli_socket < 0 %lld", cli_socket);
      continue;
    }

    ipv4_addr *ipv4 = (ipv4_addr *)&cli_addr;
    z_debug("new connection %s:%u", inet_ntoa(ipv4->sin_addr),
            ntohs(ipv4->sin_port));

    int64_t kq = svr->KQs[cli_socket % kqs_len];
    z_Error ret = z_SvrEventAdd(kq, cli_socket);
    if (ret != z_OK) {
      z_error("z_SvrEventAdd %d", ret);
      continue;
    }
  }

  return z_OK;
}

#endif