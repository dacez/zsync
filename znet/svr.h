#ifndef z_SVR_H
#define z_SVR_H

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <sys/syslimits.h>

#include "zepoch/epoch.h"
#include "zerror/error.h"
#include "znet/socket.h"
#include "znet/proto.h"
#include "zutils/assert.h"
#include "zutils/channel.h"
#include "zutils/defer.h"
#include "zutils/log.h"
#include "zutils/mem.h"
#include "zutils/threads.h"

typedef z_Error z_Handle(void *attr, const z_Req *req, z_Resp *resp);

typedef struct {
  z_Handle **Handles;
  int64_t HandlesLen;
} z_Handles;

z_Error z_HandlesInit(z_Handles *hs) {
  z_assert(hs != nullptr);

  hs->HandlesLen = 0;
  hs->Handles = nullptr;
  return z_OK;
}

void z_HandlesDestroy(z_Handles *hs) {
  z_assert(hs != nullptr);

  if (hs->Handles != nullptr) {
    z_free(hs->Handles);
  }

  hs->HandlesLen = 0;
}

z_Error z_HandlesAdd(z_Handles *hs, uint8_t type, z_Handle *handle) {
  z_assert(hs != nullptr);

  if (type >= hs->HandlesLen) {
    z_Handle**src = hs->Handles;
    int64_t src_len = hs->HandlesLen;
    hs->HandlesLen = hs->HandlesLen == 0 ? 1 : hs->HandlesLen * 2;
    hs->Handles = z_malloc(sizeof(z_Handle*) * hs->HandlesLen);
    if (hs->Handles == nullptr) {
      z_error("hs->Handles == nullptr");
      hs->Handles = src;
      hs->HandlesLen = src_len;
      return z_ERR_NOSPACE;
    }
    memset(hs->Handles, 0, hs->HandlesLen);

    if (src != nullptr) {
      memcpy(hs->Handles, src, sizeof(z_Handle*) * src_len);
      z_free(src);
    }

    return z_HandlesAdd(hs, type, handle);
  }

  if (hs->Handles[type] != nullptr) {
    z_error("hs->Handles[type] != nullptr");
    return z_ERR_EXIST;
  }

  hs->Handles[type] = handle;
  return z_OK;
}

z_Error z_HandlesRun(z_Handles *hs, void *arg, const z_Req *req, z_Resp *resp) {
  z_assert(hs != nullptr, arg != nullptr, req != nullptr, resp != nullptr);
  
  if (req->Header.Type >= hs->HandlesLen) {
    z_error("type >= hs->HandlesLen %u", req->Header.Type);
    return z_ERR_NOT_FOUND;
  }

  z_Handle *handle = hs->Handles[req->Header.Type];
  if (handle == nullptr) {
    z_error("handle == nullptr %u", req->Header.Type);
    return z_ERR_NOT_FOUND;
  }

  return handle(arg, req, resp);
}

enum : int8_t {
  z_SVR_STATUS_STOP = 0,
  z_SVR_STATUS_RUNNING = 1,
};

typedef struct {
  char IP[z_IP_MAX_LEN];
  uint16_t Port;
  void *Arg;
  z_Handles *Handles;
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

void *z_IOProcess(z_Svr *svr) {
  if (svr == nullptr) {
    z_error("ptr == nullptr");
    return nullptr;
  }
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
    switch (ret) {
    case z_ERR_TIMEOUT: {
      z_debug("timeout");
      continue;
    }
    case z_OK: {
      break;
    }
    default: {
      z_error("z_ChannelWait failed %d", ret);
      continue;
    }
    }

    for (int64_t i = 0; i < ev_count; ++i) {
      z_Socket cli_socket = {.FD = events[i].FD};

      if (z_EventIsEnd(&events[i])) {
        z_debug("z_EventIsEnd");
        z_ConnectClose(ch, &cli_socket);
        continue;
      }

      z_unique(z_Req) req = {};
      z_Error ret = z_ReqInitBySocket(&req, &cli_socket);
      if (ret != z_OK) {
        z_error("z_ReqInitBySocket %d", ret);
        z_ConnectClose(ch, &cli_socket);
        continue;
      }

      z_unique(z_Resp) resp = {};
      z_Handle *handle;
      ret = z_HandlesRun(svr->Handles, svr->Arg, &req, &resp);
      if (ret != z_OK) {
        z_debug("z_HandlesRun error %d", ret);
      }

      resp.Header.Code = ret;
      ret = z_RespToSocket(&resp, &cli_socket);
      if (ret != z_OK) {
        z_error("z_RespToSocket %d", ret);
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
                  int64_t worker_count, void *arg, z_Handles *handles) {
  z_assert(svr != nullptr, ip != nullptr, port != 0, worker_count != 0,
           arg != nullptr, handles != nullptr);

  memset(svr->IP, 0, z_IP_MAX_LEN);
  strncpy(svr->IP, ip, z_IP_MAX_LEN - 1);
  svr->Port = port;
  svr->Arg = arg;
  svr->Handles = handles;
  svr->WorkerCount = worker_count;
  svr->Socket = (z_Socket) {.FD = z_INVALID_SOCKET};
  svr->AcceptCh = (z_Channel) {.CH = z_INVALID_CHANNEL};
  svr->TIDs = (z_ThreadIDs){};
  svr->Epoch = (z_Epoch){};
  svr->WorkerChs = (z_Channels){};
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

    z_ThreadIDsInit(&svr->TIDs, svr->WorkerCount);
    if (ret != z_OK) {
      z_error("z_ThreadIDsInit %d", ret);
      break;
    }

    ret = z_EpochInit(&svr->Epoch, svr->WorkerCount, 1024);
    if (ret != z_OK) {
      z_error("z_EpochInit %d", ret);
      break;
    }

    ret = z_ChannelsInit(&svr->WorkerChs, svr->WorkerCount);
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
  z_Error ret = z_ThreadsInit(&ts, svr->WorkerCount, (z_ThreadFunc)z_IOProcess, svr);
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
    switch (ret) {
    case z_ERR_TIMEOUT: {
      z_debug("timeout");
      continue;
    }
    case z_OK: {
      break;
    }
    default: {
      z_error("z_ChannelWait failed %d", ret);
      continue;
    }
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