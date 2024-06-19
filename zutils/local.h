#ifndef z_LOCAL_H
#define z_LOCAL_H
#include <stdint.h>
#include <string.h>

#include "zerror/error.h"
#include "zutils/allocator.h"
#include "zutils/assert.h"

thread_local z_Allocator z_thread_local_allocator = {.Data = {}, .Pos = {}};

void *z_ThreadLocalAlloc(int64_t size) {
  return z_AllocatorAlloc(&z_thread_local_allocator, size);
}

void z_ThreadLocalReset() {
  return z_AllocatorReset(&z_thread_local_allocator);
}

typedef struct {
  void *Data;
  int64_t Pos;
  int64_t Size;
} z_LocalBuffer;

z_Error z_LocalBufferInit(z_LocalBuffer *b, int64_t size) {
  b->Data = z_ThreadLocalAlloc(size);
  if (b->Data == nullptr) {
    z_error("z_ThreadLocalAlloc failed");
    return z_ERR_NOSPACE;
  }

  b->Pos = 0;
  b->Size = size;
  return z_OK;
}

z_Error z_LocalBufferAppend(z_LocalBuffer *b, void *data, int64_t size) {
  z_assert(b != nullptr);

  if (b->Pos + size <= b->Size) {
    memcpy(b->Data, data, size);
    b->Pos += size;
    return z_OK;
  }

  void *dst = z_ThreadLocalAlloc(b->Size + size);
  if (dst == nullptr) {
    z_error("z_ThreadLocalAlloc failed");
    return z_ERR_NOSPACE;
  }

  memcpy(dst, b->Data, b->Size);
  memcpy(dst + b->Size, data, size);
  b->Data = dst;
  b->Pos = b->Size + size;
  b->Size = b->Size + size;
  return z_OK;
}

#endif