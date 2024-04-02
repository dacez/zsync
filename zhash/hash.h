#ifndef z_HASH_H
#define z_HASH_H

#include <stdint.h>

uint16_t z_checksum(uint8_t *data, uint64_t len) {

  uint8_t sum[2] =  {0, 0};
  
  for (uint64_t i = 0; i < len; ++i) {
    sum[i%2] = sum[i%2] ^ data[i];
  }
  return *(uint16_t*)sum;
}

#endif