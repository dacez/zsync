#ifndef z_SVR_H
#define z_SVR_H

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <sys/event.h>
#include <time.h>
#include <unistd.h>

#include "zepoch/epoch.h"
#include "zerror/error.h"
#include "znet/net_record.h"
#include "znet/socket.h"
#include "zrecord/record.h"
#include "zutils/assert.h"
#include "zutils/defer.h"
#include "zutils/log.h"
#include "zutils/mem.h"
#include "zutils/threads.h"

typedef z_Error z_Handle(void *attr, z_Record *record, z_Record **result);
enum : int8_t {
  z_SVR_STATUS_STOP = 0,
  z_SVR_STATUS_RUNNING = 1,
};

typedef struct {
  char IP[z_IP_MAX_LEN];
  uint16_t Port;
  void *Attr;
  z_Handle *Handle;
  int64_t ThreadCount;
  z_ThreadIDs TIDs;
  z_Epoch Epoch;
  int64_t *KQs;
  z_Socket Socket;
  atomic_int_fast8_t Status;
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

  z_debug("close connection %lld", cli_socket);
  close(cli_socket);
  return;
}

void *z_IOProcess(void *ptr) {
  if (ptr == nullptr) {
    z_error("ptr == nullptr");
    return nullptr;
  }
  z_Svr *svr = (z_Svr *)ptr;
  z_ThreadIDInit(&svr->TIDs);
  z_defer(z_ThreadIDDestroy, &svr->TIDs);

  int64_t kq = svr->KQs[z_ThreadID()];
  z_debug("IOThread Start %lld", z_ThreadID());

  while (1) {
    int8_t status = atomic_load(&svr->Status);
    if (status != z_SVR_STATUS_RUNNING) {
      z_info("z_IOProcess stop");
      break;
    }

    struct kevent events[z_KQUEUE_BACKLOG] = {};
    struct timespec ts = {.tv_sec = 1, .tv_nsec = 0};
    int64_t ev_count = kevent(kq, NULL, 0, events, z_KQUEUE_BACKLOG, &ts);
    if (ev_count < 0) {
      z_error("kevent failed");
      break;
    }

    if (ev_count == 0) {
      z_debug("timeout");
      continue;
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

void z_SvrKQueueDestroy(int64_t *kqs) {
  if (kqs != nullptr) {
    z_free(kqs);
  }
}

void z_SvrDestroy(z_Svr *svr) {
  z_assert(svr != nullptr);

  int8_t status = atomic_load(&svr->Status);
  if (status != z_SVR_STATUS_STOP) {
    z_panic("status invalid %d", status);
  }

  z_ThreadIDsDestroy(&svr->TIDs);
  z_EpochDestroy(&svr->Epoch);
  z_SvrKQueueDestroy(svr->KQs);
  svr->KQs = nullptr;
  z_SocketDestroy(&svr->Socket);
}

z_Error z_SvrKQueueInit(z_Svr *svr) {
  z_assert(svr != nullptr);

  int64_t kqs_len = svr->ThreadCount;
  svr->KQs = z_malloc(kqs_len * sizeof(int64_t));
  if (svr->KQs == nullptr) {
    z_error("svr->KQs == nullptr");
    return z_ERR_NOSPACE;
  }

  for (int64_t i = 0; i < kqs_len; ++i) {
    svr->KQs[i] = kqueue();
    if (svr->KQs[i] < 0) {
      z_error("kqueue failed %lld", svr->KQs[i]);
      return z_ERR_NET;
    }
  }
  return z_OK;
}

z_Error z_SvrInit(z_Svr *svr, const char *ip, uint16_t port,
                  int64_t thread_count, void *attr, z_Handle *handle) {
  z_assert(svr != nullptr, ip != nullptr, port != 0, thread_count != 0,
           attr != nullptr, handle != nullptr);

  memset(svr->IP, 0, z_IP_MAX_LEN);
  strncpy(svr->IP, ip, z_IP_MAX_LEN - 1);
  svr->Port = port;
  svr->Attr = attr;
  svr->Handle = handle;
  svr->ThreadCount = thread_count;
  atomic_store(&svr->Status, z_SVR_STATUS_RUNNING);

  z_Error ret = z_OK;
  do {
    z_ThreadIDsInit(&svr->TIDs, thread_count);
    if (ret != z_OK) {
      z_error("z_ThreadIDsInit %d", ret);
      break;
    }

    ret = z_EpochInit(&svr->Epoch, thread_count, 1024);
    if (ret != z_OK) {
      z_error("z_EpochInit %d", ret);
      break;
    }

    ret = z_SocketSvrInit(&svr->Socket, svr->IP, svr->Port);
    if (ret != z_OK) {
      z_error("z_SocketSvrInit %d", ret);
      break;
    }

    ret = z_SvrKQueueInit(svr);
    if (ret != z_OK) {
      z_error("z_SvrKQueueInit %d", ret);
      break;
    }
  } while (0);

  if (ret != z_OK) {
    z_SvrDestroy(svr);
    return ret;
  }

  return z_OK;
}

z_Error z_SvrRun(z_Svr *svr) {
  atomic_store(&svr->Status, z_SVR_STATUS_RUNNING);

  z_unique(z_Threads) ts;
  z_Error ret = z_ThreadsInit(&ts, svr->ThreadCount, z_IOProcess, svr);
  if (ret != z_OK) {
    z_error("z_ThreadsInit %d", ret);
    return ret;
  }

  int64_t kq = kqueue();
  ret = z_SvrEventAdd(kq, svr->Socket.FD);
  if (ret != z_OK) {
    z_error("z_SvrEventAdd %d", ret);
    return ret;
  }

  while (1) {
    int8_t status = atomic_load(&svr->Status);
    if (status != z_SVR_STATUS_RUNNING) {
      z_info("z_SvrRun stop");
      break;
    }

    struct kevent events[z_KQUEUE_BACKLOG] = {};
    struct timespec ts = {.tv_sec = 1, .tv_nsec = 0};
    int64_t ev_count = kevent(kq, NULL, 0, events, z_KQUEUE_BACKLOG, &ts);
    if (ev_count < 0) {
      z_error("kevent failed");
      break;
    }

    if (ev_count == 0) {
      z_debug("timeout");
      continue;
    }

    for (int64_t i = 0; i < z_KQUEUE_BACKLOG; ++i) {
      z_Socket sock_cli = {};
      z_Error ret = z_SocketAccept(&svr->Socket, &sock_cli);
      if (ret != z_OK) {
        z_error("z_SocketAccept %d", ret);
        continue;
      }

      ret = z_SvrEventAdd(svr->KQs[sock_cli.FD % svr->ThreadCount], sock_cli.FD);
      if (ret != z_OK) {
        z_error("z_SvrEventAdd %d", ret);
        continue;
      }
    }
  }

  close(kq);

  return z_OK;
}

void z_SvrStop(z_Svr *svr) {
  atomic_store(&svr->Status, z_SVR_STATUS_STOP);
}

#endif