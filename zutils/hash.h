#ifndef z_HASH_H
#define z_HASH_H

#include <stdint.h>

// FNV-1a
uint64_t z_Hash(const int8_t *data, int64_t size) {
  uint64_t hash = 0xcbf29ce484222325; // FNV-1a 64位初始哈希值
  uint64_t fnv_prime = 0x100000001b3; // FNV-1a 64位质数

  for (int64_t i = 0; i < size; i++) {
    hash ^= (uint64_t)data[i];
    hash *= fnv_prime;
  }

  return hash;
}

uint8_t z_Checksum(const int8_t *data, int64_t size) {
  uint64_t hash64 = z_Hash(data, size);
  return hash64 & 0xFF;
}

#endif