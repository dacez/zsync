#ifndef z_SVR_H
#define z_SVR_H

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

#include "zepoch/epoch.h"
#include "zerror/error.h"
#include "znet/net_record.h"
#include "znet/socket.h"
#include "zrecord/record.h"
#include "zutils/assert.h"
#include "zutils/channel.h"
#include "zutils/defer.h"
#include "zutils/log.h"
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
  z_Socket Socket;
  z_Channel AcceptCh;
  int64_t WorkerCount;
  z_ThreadIDs TIDs;
  z_Epoch Epoch;
  z_Channels WorkerChs;
  atomic_int_fast8_t Status;
} z_Svr;

void z_ConnectClose(z_Channel *ch, z_Socket *socket) {
  z_ChannelUnsubscribeSocket(ch, socket);
  z_SocketDestroy(socket);
}

void *z_IOProcess(void *ptr) {
  if (ptr == nullptr) {
    z_error("ptr == nullptr");
    return nullptr;
  }
  z_Svr *svr = (z_Svr *)ptr;
  z_ThreadIDInit(&svr->TIDs);
  z_defer(z_ThreadIDDestroy, &svr->TIDs);

  z_Channel *ch = nullptr;
  z_Error ret = z_ChannelsGet(&svr->WorkerChs, z_ThreadID(), &ch);
  if (ret != z_OK) {
    z_error("z_ChannelsGet %d", ret);
    return nullptr;
  }
  z_debug("IOThread Start %lld", z_ThreadID());

  while (1) {
    int8_t status = atomic_load(&svr->Status);
    if (status != z_SVR_STATUS_RUNNING) {
      z_info("z_IOProcess stop");
      break;
    }

    z_Event events[z_EVENT_LEN] = {};
    int64_t ev_count = 0;
    ret = z_ChannelWait(ch, events, z_EVENT_LEN, &ev_count, 1000);
    if (ret != z_OK) {
      z_error("z_ChannelWait %d", ret);
      continue;
    }

    if (ev_count == 0) {
      z_debug("timeout");
      continue;
    }

    for (int64_t i = 0; i < ev_count; ++i) {
      z_Socket cli_socket = {.FD= events[i].FD};

      if (z_EventIsEnd(&events[i])) {
        z_debug("z_EventIsEnd");
        z_ConnectClose(ch, &cli_socket);
        continue;
      }

      z_unique(z_Req) req = {};

      z_Error ret = z_ReqInitFromNet(cli_socket.FD, &req);
      if (ret != z_OK) {
        z_error("z_ReqInitFromNet %d", ret);
        z_ConnectClose(ch, &cli_socket);
        continue;
      }

      z_unique(z_Resp) resp = {};
      ret = svr->Handle(svr->Attr, req.Record, &resp.Record);
      if (ret != z_OK) {
        z_debug("handle error %d", ret);
      }

      resp.Ret.Code = ret;
      resp.Ret.DataLen = resp.Record != nullptr ? z_RecordLen(resp.Record) : 0;

      ret = z_RespToNet(cli_socket.FD, &resp);
      if (ret != z_OK) {
        z_error("z_RespToNet %d", ret);
        z_ConnectClose(ch, &cli_socket);
        continue;
      }
    }
  }

  return nullptr;
}

void z_SvrDestroy(z_Svr *svr) {
  z_assert(svr != nullptr);

  int8_t status = atomic_load(&svr->Status);
  if (status != z_SVR_STATUS_STOP) {
    z_panic("status invalid %d", status);
  }

  z_ChannelsDestroy(&svr->WorkerChs);
  z_EpochDestroy(&svr->Epoch);
  z_ThreadIDsDestroy(&svr->TIDs);
  z_ChannelDestroy(&svr->AcceptCh);
  z_SocketDestroy(&svr->Socket);
}

z_Error z_SvrInit(z_Svr *svr, const char *ip, uint16_t port,
                  int64_t worker_count, void *attr, z_Handle *handle) {
  z_assert(svr != nullptr, ip != nullptr, port != 0, worker_count != 0,
           attr != nullptr, handle != nullptr);

  memset(svr->IP, 0, z_IP_MAX_LEN);
  strncpy(svr->IP, ip, z_IP_MAX_LEN - 1);
  svr->Port = port;
  svr->Attr = attr;
  svr->Handle = handle;
  svr->WorkerCount = worker_count;
  atomic_store(&svr->Status, z_SVR_STATUS_STOP);

  z_Error ret = z_OK;
  do {
    ret = z_SocketSvrInit(&svr->Socket, svr->IP, svr->Port);
    if (ret != z_OK) {
      z_error("z_SocketSvrInit %d", ret);
      break;
    }

    ret = z_ChannelInit(&svr->AcceptCh);
    if (ret != z_OK) {
      z_error("z_ChannelInit %d", ret);
      break;
    }

    z_ThreadIDsInit(&svr->TIDs, worker_count);
    if (ret != z_OK) {
      z_error("z_ThreadIDsInit %d", ret);
      break;
    }

    ret = z_EpochInit(&svr->Epoch, worker_count, 1024);
    if (ret != z_OK) {
      z_error("z_EpochInit %d", ret);
      break;
    }

    ret = z_ChannelsInit(&svr->WorkerChs, worker_count);
    if (ret != z_OK) {
      z_error("z_ChannelsInit %d", ret);
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
  z_Error ret = z_ThreadsInit(&ts, svr->WorkerCount, z_IOProcess, svr);
  if (ret != z_OK) {
    z_error("z_ThreadsInit %d", ret);
    return ret;
  }

  ret = z_ChannelInit(&svr->AcceptCh);
  if (ret != z_OK) {
    z_error("z_ChannelInit %d", ret);
    return ret;
  }

  ret = z_ChannelSubscribeSocket(&svr->AcceptCh, &svr->Socket);
  if (ret != z_OK) {
    z_error("z_ChannelSubscribeSocket %d", ret);
    return ret;
  }

  while (1) {
    int8_t status = atomic_load(&svr->Status);
    if (status != z_SVR_STATUS_RUNNING) {
      z_info("z_SvrRun stop");
      break;
    }

    z_Event events[z_EVENT_LEN] = {};
    int64_t ev_count = 0;
    ret = z_ChannelWait(&svr->AcceptCh, events, z_EVENT_LEN, &ev_count, 1000);
    if (ret != z_OK) {
      z_error("z_ChannelWait %d", ret);
      continue;
    }

    if (ev_count == 0) {
      z_debug("timeout");
      continue;
    }

    for (int64_t i = 0; i < ev_count; ++i) {
      z_Socket sock_cli = {};
      z_Error ret = z_SocketAccept(&svr->Socket, &sock_cli);
      if (ret != z_OK) {
        z_error("z_SocketAccept %d", ret);
        continue;
      }

      z_Channel *ch_cli = nullptr;
      ret = z_ChannelsGet(&svr->WorkerChs, sock_cli.FD % svr->WorkerCount,
                          &ch_cli);
      if (ret != z_OK) {
        z_error("z_ChannelsGet %d", ret);
        continue;
      }

      ret = z_ChannelSubscribeSocket(ch_cli, &sock_cli);
      if (ret != z_OK) {
        z_error("z_ChannelSubscribeSocket %d", ret);
        continue;
      }
    }
  }

  z_ChannelUnsubscribeSocket(&svr->AcceptCh, &svr->Socket);
  z_ChannelDestroy(&svr->AcceptCh);
  return z_OK;
}

void z_SvrStop(z_Svr *svr) { atomic_store(&svr->Status, z_SVR_STATUS_STOP); }

#endif