#ifndef z_BUFFER_H
#define z_BUFFER_H

#include "zerror/error.h"
#include "zutils/assert.h"
#include "zutils/mem.h"
#include <stdint.h>
#include <string.h>

typedef struct {
  const void *Data;
  int64_t Len;
} z_ConstBuffer;

typedef struct {
  void *Data;
  int64_t Len;
} z_Buffer;

z_Error z_BufferInit(z_Buffer *b, const int8_t *data, int64_t len) {
  z_assert(b != nullptr, data != nullptr, len != 0);
  b->Data = z_malloc(len);
  if (b->Data == nullptr) {
    z_error("no space");
    return z_ERR_NOSPACE;
  }
  b->Len = len;

  memcpy(b->Data, data, len);
  return z_OK;
}

z_Error z_BufferInitByConstBuffer(z_Buffer *b, const z_ConstBuffer *cb) {
  z_assert(b != nullptr, cb != nullptr);
  return z_BufferInit(b, cb->Data, cb->Len);
}

void z_BufferDestroy(z_Buffer *b) {
  z_assert(b != nullptr);
  if (b->Data != nullptr) {
    z_free(b->Data);
  }
}

#define z_BufferStr(buffer, cstr)                                              \
  memset(cstr, 0, sizeof(cstr));                                               \
  memcpy(cstr, buffer.Data,                                                    \
         buffer.Len < sizeof(cstr) - 1 ? buffer.Len : sizeof(cstr) - 1);

#define z_BufferIsEqual(a, b)                                                  \
  ({                                                                           \
    bool ret = false;                                                          \
    do {                                                                       \
      if ((a)->Len != (b)->Len) {                                              \
        ret = false;                                                           \
        break;                                                                 \
      }                                                                        \
      if ((a)->Len == 0) {                                                     \
        ret = true;                                                            \
        break;                                                                 \
      }                                                                        \
      ret = memcmp((a)->Data, (b)->Data, (a)->Len) == 0;                       \
    } while (0);                                                               \
    ret;                                                                       \
  })

z_ConstBuffer z_ConstBufferEmpty() {
  z_ConstBuffer buffer = {.Data = nullptr, .Len = 0};
  return buffer;
}

bool z_ConstBufferIsEmpty(const z_ConstBuffer *b) {
  z_assert(b->Len != 0 && b->Data != nullptr ||
           b->Len == 0 && b->Data == nullptr);
  if (b->Len == 0) {
    return true;
  }

  return false;
}

#endif