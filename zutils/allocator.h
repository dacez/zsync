#ifndef z_ALLOCATOR_H
#define z_ALLOCATOR_H

#include <stdint.h>

#include "zutils/assert.h"
#include "zutils/mem.h"

#define z_ALLOCATOR_ZERO_LEVEL_SIZE 1024
#define z_ALLOCATOR_MAX_LEVEL 8

static_assert(z_ALLOCATOR_MAX_LEVEL <= UINT8_MAX);
static_assert((UINT64_MAX >> z_ALLOCATOR_MAX_LEVEL) >=
              z_ALLOCATOR_ZERO_LEVEL_SIZE);

typedef struct {
  uint64_t Level : 8;
  uint64_t Offset : 56;
} z_Pos;

typedef struct {
  void *Data[z_ALLOCATOR_MAX_LEVEL];
  z_Pos Pos;
} z_Allocator;

uint64_t z_AllocatorLevelSize(uint8_t level) {
  return z_ALLOCATOR_ZERO_LEVEL_SIZE << level;
}
void *z_AllocatorAlloc(z_Allocator *a, int64_t size) {
  z_assert(a != nullptr);
  if (a->Data[a->Pos.Level] == nullptr) {
    void *data = z_malloc(z_AllocatorLevelSize(a->Pos.Level));
    if (data == nullptr) {
      z_error("z_malloc failed %lld", size);
      return nullptr;
    }
    a->Data[a->Pos.Level] = data;
  }

  if (a->Pos.Offset + size <= z_AllocatorLevelSize(a->Pos.Level)) {
    void *ptr = a->Data[a->Pos.Level] + a->Pos.Offset;
    a->Pos.Offset += size;
    return ptr;
  }

  if (a->Pos.Level + 1 < z_ALLOCATOR_MAX_LEVEL) {
    a->Pos.Level += 1;
    a->Pos.Offset = 0;
    return z_AllocatorAlloc(a, size);
  }
  z_error("no space %u %llu", a->Pos.Level, a->Pos.Offset);
  return nullptr;
}

void z_AllocatorReset(z_Allocator *a) {
  z_assert(a != nullptr);
  a->Pos.Level = 0;
  a->Pos.Offset = 0;
}

void z_AllocatorDestroy(z_Allocator *a) {
  z_assert(a != nullptr);
  for (int64_t i = 0; i < z_ALLOCATOR_MAX_LEVEL; ++i) {
    if (a->Data[i] != nullptr) {
      z_free(a->Data[i]);
    }
  }
}

#endif