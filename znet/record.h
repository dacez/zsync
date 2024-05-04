#ifndef z_NET_RECORD
#define z_NET_RECORD
#include "zerror/error.h"
#include "zrecord/record.h"
#include "zutils/mem.h"
#include <stdint.h>
#include <sys/socket.h>

typedef struct {
  z_Record Record;
} z_Req;

int64_t z_ReqLen(z_Req *r) {
  return sizeof(z_Req) - sizeof(z_Record) + z_RecordLen(&r->Record);
}

z_Error z_ReqInitFromNet(int64_t cli_socket, z_Req **req) {
  z_Req head;
  int64_t bytes = recv(cli_socket, &head, sizeof(z_Req), 0);
  if (bytes <= 0 || bytes != sizeof(z_Req)) {
    z_error("recv failed %lld", bytes);
    return z_ERR_NET;
  }

  int64_t len = z_ReqLen(&head);
  *req = z_malloc(len);
  **req = head;
  int64_t data_len = len - sizeof(z_Req);
  int8_t *data = ((int8_t *)*req) + sizeof(z_Req);
  bytes = recv(cli_socket, data, data_len, 0);
  if (bytes <= 0 || bytes != data_len) {
    z_error("recv failed %lld", bytes);
    return z_ERR_NET;
  }

  return z_OK;
}

void z_ReqDestory(z_Req *req) {
  if (req == nullptr) {
    return;
  }

  z_free(req);
}

z_Error z_RecordToNet(int64_t cli_socket, z_Error ret, z_Record *r) {
  int64_t bytes = send(cli_socket, &ret, sizeof(z_Error), 0);
  if (bytes < 0 || bytes != sizeof(z_Error)) {
    z_error("send failed %lld", bytes);
    return z_ERR_NET;
  }

  if (r != nullptr) {
    int64_t data_len = z_RecordLen(r);
    int8_t *data = (int8_t *)r;
    bytes = send(cli_socket, data, data_len, 0);
    if (bytes < 0 || bytes != data_len) {
      z_error("send failed %lld", bytes);
      return z_ERR_NET;
    }
  }

  return z_OK;
}

#endif