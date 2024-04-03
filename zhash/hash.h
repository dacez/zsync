#ifndef z_HASH_H
#define z_HASH_H

#include <stdint.h>

uint64_t z_Hash(uint8_t *data, uint64_t len) {
    uint64_t hash = 5381;
    uint8_t c;

    for (uint64_t i = 0; i < len; ++i) {
      c = data[i];
      hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

uint8_t z_Checksum(uint8_t *data, uint64_t len) {
  uint64_t hash64 = z_Hash(data, len);
  uint8_t *hs = (uint8_t*)&hash64;
  uint8_t hash8 = 0;
  for (uint8_t i = 0; i < 8; ++i) {
    hash8 = hs[i] ^ hash8;
  } 
  return hash8;
}

#endif