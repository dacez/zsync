#ifndef z_HASH_H
#define z_HASH_H

#include <stdint.h>

// FNV-1a
int64_t z_Hash(int8_t *data, int64_t len) {
  int64_t hash = 0xcbf29ce484222325; // FNV-1a 64位初始哈希值
  int64_t fnv_prime = 0x100000001b3; // FNV-1a 64位质数

  for (int64_t i = 0; i < len; i++) {
    hash ^= (int64_t)data[i];
    hash *= fnv_prime;
  }

  return hash;
}

int8_t z_Checksum(int8_t *data, int64_t len) {
  int64_t hash64 = z_Hash(data, len);
  return (int8_t)(hash64 & 0x00000000000000FF);
}

#endif