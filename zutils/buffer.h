#ifndef z_BUFFER_H
#define z_BUFFER_H

#include <stdint.h>
#include <string.h>

#include "zerror/error.h"
#include "zutils/assert.h"
#include "zutils/mem.h"

typedef struct {
  const void *Data;
  int64_t Size;
} z_ConstBuffer;

typedef struct {
  void *Data;
  int64_t Size;
} z_Buffer;

z_Error z_BufferInit(z_Buffer *b, const int8_t *data, int64_t size) {
  z_assert(b != nullptr, data != nullptr, size != 0);
  b->Data = z_malloc(size);
  if (b->Data == nullptr) {
    z_error("no space");
    return z_ERR_NOSPACE;
  }
  b->Size = size;

  memcpy(b->Data, data, size);
  return z_OK;
}

z_Error z_BufferAppend(z_Buffer *b, const int8_t *data, int64_t size) {
  z_assert(b != nullptr, data != nullptr, size != 0);
  return z_OK;
}

z_Error z_BufferInitByConstBuffer(z_Buffer *b, const z_ConstBuffer *cb) {
  z_assert(b != nullptr, cb != nullptr);
  return z_BufferInit(b, cb->Data, cb->Size);
}

void z_BufferDestroy(z_Buffer *b) {
  z_assert(b != nullptr);
  if (b->Data != nullptr) {
    z_free(b->Data);
  }
  b->Size = 0;
}

#define z_BufferStr(buffer, cstr)                                              \
  memset(cstr, 0, sizeof(cstr));                                               \
  memcpy(cstr, buffer.Data,                                                    \
         buffer.Size < sizeof(cstr) - 1 ? buffer.Size : sizeof(cstr) - 1);

#define z_BufferIsEqual(a, b)                                                  \
  ({                                                                           \
    bool ret = false;                                                          \
    do {                                                                       \
      if ((a)->Size != (b)->Size) {                                              \
        ret = false;                                                           \
        break;                                                                 \
      }                                                                        \
      if ((a)->Size == 0) {                                                     \
        ret = true;                                                            \
        break;                                                                 \
      }                                                                        \
      ret = memcmp((a)->Data, (b)->Data, (a)->Size) == 0;                       \
    } while (0);                                                               \
    ret;                                                                       \
  })

z_ConstBuffer z_ConstBufferEmpty() {
  z_ConstBuffer buffer = {.Data = nullptr, .Size = 0};
  return buffer;
}

bool z_ConstBufferIsEmpty(const z_ConstBuffer *b) {
  z_assert(b->Size != 0 && b->Data != nullptr ||
           b->Size == 0 && b->Data == nullptr);
  if (b->Size == 0) {
    return true;
  }

  return false;
}

#endif