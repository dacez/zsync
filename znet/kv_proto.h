#ifndef z_KV_PROTO_H
#define z_KV_PROTO_H
#include <stdint.h>

#include "znet/proto.h" // IWYU pragma: export
#include "zrecord/record.h" // IWYU pragma: export

typedef enum : uint8_t {
  z_KV_REQ_TYPE_SET = 1,
  z_KV_REQ_TYPE_GET = 2,
  z_KV_REQ_TYPE_BINLOG_GET = 3,
} z_KV_REQ_TYPE;

#endif