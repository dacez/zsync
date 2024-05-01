#ifndef z_NET_RECORD_H
#define z_NET_RECORD_H
#include <stdint.h>

typedef struct {
  uint64_t OP : 8;
  uint64_t Sum : 8;
  uint64_t KeyLen : 16;
  uint64_t ValLen : 24;
} z_NetRecord;
#endif