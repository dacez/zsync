#ifndef z_BUFFER_H
#define z_BUFFER_H

#include <stdint.h>
#include <string.h>

#include "zerror/error.h"
#include "zutils/assert.h"
#include "zutils/log.h"
#include "zutils/mem.h"

typedef struct {
  int8_t *Data;
  int64_t Len;
} z_Buffer;

#define z_BufferStr(z_buffer, cstr)                                            \
  memset(cstr, 0, sizeof(cstr));                                               \
  memcpy(cstr, z_buffer.Data,                                                  \
         z_buffer.Len < sizeof(cstr) - 1 ? z_buffer.Len : sizeof(cstr) - 1);

bool z_BufferIsEqual(z_Buffer a, z_Buffer b) {
  if (a.Len != b.Len) {
    return false;
  }

  if (a.Len == 0) {
    return true;
  }

  return memcmp(a.Data, b.Data, a.Len) == 0;
}

void z_BufferDestroy(z_Buffer *s) {
  if (s == nullptr || s->Data == nullptr) {
    return;
  }

  z_free(s->Data);
  s->Data = nullptr;
  s->Len = 0;
}

z_Error z_BufferReset(z_Buffer *s, int8_t *d, int64_t len) {
  if (s == nullptr || d == nullptr || len == 0 || len > 0xFFFF) {
    z_error("s == nullptr || d == nullptr || len == 0 || len > 0xFFFF");
    return z_ERR_INVALID_DATA;
  }

  int8_t *src = s->Data;
  s->Data = z_malloc(len);
  if (s->Data == nullptr) {
    z_error("s->Data == nullptr");
    s->Data = src;
    return z_ERR_NOSPACE;
  }

  s->Len = len;
  memcpy(s->Data, d, len);

  if (src != nullptr) {
    z_free(src);
  }
  return z_OK;
}

z_Error z_BufferResetByBuffer(z_Buffer *des, z_Buffer src) {
  return z_BufferReset(des, src.Data, src.Len);
}

z_Error z_BufferInit(z_Buffer *s, int8_t *data, int64_t len) {
  s->Data = nullptr;
  s->Len = 0;
  return z_BufferReset(s, data, len);
}

z_Error z_BufferInitByBuffer(z_Buffer *des, z_Buffer src) {
  des->Data = nullptr;
  des->Len = 0;
  return z_BufferResetByBuffer(des, src);
}

z_Buffer z_BufferEmpty() {
  z_Buffer buffer = {.Data = nullptr, .Len = 0};
  return buffer;
}

bool z_BufferIsEmpty(z_Buffer *b) {
  z_assert(b->Len != 0 && b->Data != nullptr || b->Len == 0 && b->Data == nullptr);
  if (b->Len == 0) {
    return true;
  }

  return false;
}

#endif