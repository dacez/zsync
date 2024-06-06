#ifndef z_PROTO_H
#define z_PROTO_H
#include <stdint.h>

#include "zerror/error.h"
#include "znet/socket.h"

typedef struct {
  uint64_t Type : 8;
  uint64_t reserve : 24;
  uint64_t Size : 32;
} z_ReqHeader;
static_assert(sizeof(z_ReqHeader) == 8);

typedef struct {
  uint64_t Code : 8;
  uint64_t reserve : 24;
  uint64_t Size : 32;
} z_RespHeader;
static_assert(sizeof(z_RespHeader) == 8);

typedef struct {
  z_ReqHeader Header;
  int8_t *Data;
} z_Req;

typedef struct {
  z_RespHeader Header;
  int8_t *Data;
} z_Resp;

#define z_ProtoFromSocket(r, s)                                                \
  ({                                                                           \
    z_assert(r != nullptr, s != nullptr, s->FD >= 0);                          \
    z_Error ret = z_OK;                                                        \
    do {                                                                       \
      int64_t header_len = sizeof(r->Header);                                  \
      ret = z_SocketRead(s, (int8_t *)&r->Header, header_len);                 \
      if (ret != z_OK) {                                                       \
        z_error("z_SocketRead failed %d", ret);                                \
        break;                                                                 \
      }                                                                        \
                                                                               \
      if (r->Header.Size > 0) {                                                \
        r->Data = z_malloc(r->Header.Size);                                    \
        if (r->Data == nullptr) {                                              \
          z_error(#r "->Data == nullptr");                                     \
          break;                                                               \
        }                                                                      \
                                                                               \
        ret = z_SocketRead(s, (int8_t *)r->Data, r->Header.Size);              \
        if (ret != z_OK) {                                                     \
          z_error("z_SocketRead failed %d", ret);                              \
          break;                                                               \
        }                                                                      \
      }                                                                        \
    } while (0);                                                               \
    ret;                                                                       \
  })

#define z_ProtoToSocket(r, s)                                                  \
  ({                                                                           \
    z_assert(r != nullptr, s != nullptr, s->FD >= 0);                          \
    z_Error ret = z_OK;                                                        \
    do {                                                                       \
      ret = z_SocketWrite(s, (int8_t *)&r->Header, sizeof(r->Header));         \
      if (ret != z_OK) {                                                       \
        z_error("z_SocketWrite failed %d", ret);                               \
        return ret;                                                            \
      }                                                                        \
                                                                               \
      if (r->Data != nullptr && r->Header.Size != 0) {                         \
        ret = z_SocketWrite(s, (int8_t *)r->Data, r->Header.Size);             \
        if (ret != z_OK) {                                                     \
          z_error("z_SocketWrite failed %d", ret);                             \
          return ret;                                                          \
        }                                                                      \
      }                                                                        \
    } while (0);                                                               \
    ret;                                                                       \
  })


z_Error z_ReqInitBySocket(z_Req *req, const z_Socket *s) {
  return z_ProtoFromSocket(req, s);
}

z_Error z_ReqToSocket(const z_Req *req, const z_Socket *s) {
  return z_ProtoToSocket(req, s);
}

void z_ReqDestroy(z_Req *req) {
  if (req == nullptr || req->Data == nullptr) {
    return;
  }

  z_free(req->Data);
}

z_Error z_RespInitBySocket(z_Resp *resp, const z_Socket *s) {
  return z_ProtoFromSocket(resp, s);
}

z_Error z_RespToSocket(z_Resp *resp, const z_Socket *s) {
  return z_ProtoToSocket(resp, s);
}

void z_RespDestroy(z_Resp *resp) {
  if (resp == nullptr || resp->Data == nullptr) {
    return;
  }

  z_free(resp->Data);
}
#endif