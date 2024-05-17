#ifndef z_NET_RECORD
#define z_NET_RECORD
#include "zerror/error.h"
#include "zrecord/record.h"
#include "zutils/assert.h"
#include "zutils/log.h"
#include "zutils/mem.h"
#include <stdint.h>
#include <sys/socket.h>

typedef struct {
  z_Record *Record;
} z_Req;

typedef struct {
  uint64_t Code : 8;
  uint64_t reserve:24;
  uint32_t DataLen:32;
} z_Ret;
static_assert(sizeof(z_Ret) == 8);

typedef struct {
  z_Ret Ret;
  z_Record *Record;
} z_Resp;

z_Error z_ReqInitFromNet(int64_t cli_socket, z_Req *req) {
  z_assert(req != nullptr);

  if (cli_socket < 0) {
    z_error("invalid socket %lld", cli_socket);
    return z_ERR_INVALID_DATA;
  }

  int64_t req_head_len = 0;
  int64_t bytes = 0;

  if (req_head_len > 0) {
    // if had
  }

  z_Record record_head;
  bytes = recv(cli_socket, &record_head, sizeof(z_Record), 0);
  if (bytes <= 0 || bytes != sizeof(z_Record)) {
    z_error("recv failed %lld", bytes);
    return z_ERR_NET;
  }

  int64_t len = z_RecordLen(&record_head);
  req->Record = z_malloc(len);
  if (req->Record == nullptr) {
    z_error("req->Record == nullptr");
    return z_ERR_NOSPACE;
  }

  *req->Record = record_head;
  int64_t data_len = len - sizeof(z_Record);
  int8_t *data = ((int8_t *)req->Record) + sizeof(z_Record);
  if (data_len > 0) {
    bytes = recv(cli_socket, data, data_len, 0);
    if (bytes <= 0 || bytes != data_len) {
      z_error("recv failed %lld", bytes);
      return z_ERR_NET;
    }
  }

  return z_OK;
}

z_Error z_ReqToNet(int64_t cli_socket, z_Req *req) {
  z_assert(req != nullptr, req->Record != nullptr);

  if (cli_socket < 0) {
    z_error("invalid socket %lld", cli_socket);
    return z_ERR_INVALID_DATA;
  }

  int64_t req_head_len = 0;
  int64_t bytes = 0;

  if (req_head_len > 0) {
    // if had
  }

  int64_t len = z_RecordLen(req->Record);
  bytes = send(cli_socket, req->Record, len, 0);
  if (bytes <= 0 || bytes != len) {
    z_error("send failed %lld %lld", bytes, cli_socket);
    return z_ERR_NET;
  }

  return z_OK;
}

void z_ReqDestroy(z_Req *req) {
  if (req == nullptr) {
    return;
  }

  if (req->Record == nullptr) {
    return;
  }

  z_RecordFree(req->Record);
}

z_Error z_RespInitFromNet(int64_t cli_socket, z_Resp *resp) {
  z_assert(resp != nullptr);

  if (cli_socket < 0) {
    z_error("invalid socket %lld", cli_socket);
    return z_ERR_INVALID_DATA;
  }
  int64_t resp_head_len = sizeof(resp->Ret);
  int64_t bytes = 0;

  if (resp_head_len > 0) {
    bytes = recv(cli_socket, &resp->Ret, resp_head_len, 0);
    if (bytes <= 0 || bytes != resp_head_len) {
      z_error("recv failed %lld", bytes);
      return z_ERR_NET;
    }
  }

  if (resp->Ret.DataLen > 0) {
    resp->Record = z_malloc(resp->Ret.DataLen);
    if (resp->Record == nullptr) {
      z_error("req->Record == nullptr");
      return z_ERR_NOSPACE;
    }
    bytes = recv(cli_socket, resp->Record, resp->Ret.DataLen, 0);
    if (bytes <= 0 || bytes != resp->Ret.DataLen) {
      z_error("recv failed %lld", bytes);
      return z_ERR_NET;
    }
  }

  return z_OK;
}

z_Error z_RespToNet(int64_t cli_socket, z_Resp *resp) {
  z_assert(resp != nullptr);
  if (cli_socket < 0) {
    z_error("invalid socket %lld", cli_socket);
    return z_ERR_INVALID_DATA;
  }

  int64_t resp_head_len = sizeof(resp->Ret);
  int64_t bytes = 0;

  if (resp_head_len > 0) {
    bytes = send(cli_socket, &resp->Ret, resp_head_len, 0);
    if (bytes <= 0 || bytes != resp_head_len) {
      z_error("send failed %lld", bytes);
      return z_ERR_NET;
    }
  }

  if (resp->Ret.DataLen > 0) {
    bytes = send(cli_socket, resp->Record, resp->Ret.DataLen, 0);
    if (bytes <= 0 || bytes != resp->Ret.DataLen) {
      z_error("send failed %lld", bytes);
      return z_ERR_NET;
    }
  }
  return z_OK;
}

void z_RespDestroy(z_Resp *resp) {
  if (resp == nullptr) {
    return;
  }

  if (resp->Record == nullptr) {
    return;
  }

  z_RecordFree(resp->Record);
}
#endif