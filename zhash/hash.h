#ifndef z_HASH_H
#define z_HASH_H

#include <stdint.h>

int64_t z_Hash(int8_t *data, int64_t len) {
    int64_t hash = 5381;
    int8_t c;

    for (int64_t i = 0; i < len; ++i) {
      c = data[i];
      hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

int8_t z_Checksum(int8_t *data, int64_t len) {
  int64_t hash64 = z_Hash(data, len);
  int8_t *hs = (int8_t*)&hash64;
  int8_t hash8 = 0;
  for (int8_t i = 0; i < 8; ++i) {
    hash8 = hs[i] ^ hash8;
  } 
  return hash8;
}

#endif