#ifndef z_NET_RECORD
#define z_NET_RECORD

#include <stdint.h>

#include "zerror/error.h"
#include "znet/socket.h"
#include "zrecord/record.h"
#include "zutils/assert.h"
#include "zutils/log.h"

typedef enum : int8_t {
  z_RT_KV_SET = 1,
  z_RT_KV_GET = 2,
  z_RT_BINLOG_GET = 3,
} z_ReqType;

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
  z_Record *Record;
} z_Req;

typedef struct {
  z_RespHeader Header;
  z_Record *Record;
} z_Resp;

#define z_ReqRespFromSocket(r, s)                                              \
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
        r->Record = z_RecordNewBySize(r->Header.Size);                          \
        if (r->Record == nullptr) {                                            \
          z_error(#r "->Record == nullptr");                                   \
          break;                                                               \
        }                                                                      \
                                                                               \
        ret = z_SocketRead(s, (int8_t *)r->Record, r->Header.Size);            \
        if (ret != z_OK) {                                                     \
          z_error("z_SocketRead failed %d", ret);                              \
          break;                                                               \
        }                                                                      \
      }                                                                        \
    } while (0);                                                               \
    ret;                                                                       \
  })

#define z_ReqRespToSocket(r, s)                                                \
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
      if (r->Record != nullptr && r->Header.Size != 0) {                       \
        ret = z_SocketWrite(s, (int8_t *)r->Record, r->Header.Size);           \
        if (ret != z_OK) {                                                     \
          z_error("z_SocketWrite failed %d", ret);                             \
          return ret;                                                          \
        }                                                                      \
      }                                                                        \
    } while (0);                                                               \
    ret;                                                                       \
  })

z_Error z_ReqInitBySocket(z_Req *req, const z_Socket *s) {
  return z_ReqRespFromSocket(req, s);
}

z_Error z_ReqToSocket(const z_Req *req, const z_Socket *s) {
  return z_ReqRespToSocket(req, s);
}

void z_ReqDestroy(z_Req *req) {
  if (req == nullptr || req->Record == nullptr) {
    return;
  }

  z_RecordFree(req->Record);
}

z_Error z_RespInitBySocket(z_Resp *resp, const z_Socket *s) {
  return z_ReqRespFromSocket(resp, s);
}

z_Error z_RespToSocket(z_Resp *resp, const z_Socket *s) {
  return z_ReqRespToSocket(resp, s);
}

void z_RespDestroy(z_Resp *resp) {
  if (resp == nullptr || resp->Record == nullptr) {
    return;
  }

  z_RecordFree(resp->Record);
}
#endif