#ifndef z_BUFFER_H
#define z_BUFFER_H

#include <stdint.h>
#include <string.h>

#include "zerror/error.h"
#include "zutils/mem.h"
#include "zutils/log.h"

typedef struct {
  int8_t *Data;
  int64_t Len;
} z_Buffer;

#define z_CStr(z_str, cstr) memset(cstr, 0, sizeof(cstr));memcpy(cstr, z_str.Data, z_str.Len < sizeof(cstr) - 1 ? z_str.Len : sizeof(cstr) - 1);

bool z_BufferIsEqual(z_Buffer a, z_Buffer b) {
  if (a.Len != b.Len) {
    return false;
  }

  return memcmp(a.Data, b.Data, a.Len) == 0;
}

void z_BufferDestory(z_Buffer *s) {
  if (s == nullptr || s->Data == nullptr) {
    z_debug("s == nullptr || s->Data == nullptr");
    return;
  }

  z_free(s->Data);
  s->Len = 0;
}

z_Error z_BufferReset(z_Buffer *s, int8_t *d, int64_t len) {
  if (s == nullptr || d == nullptr || len == 0) {
    z_error("s == nullptr || d == nullptr || len == 0");
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

z_Error z_BufferResetByBuffer(z_Buffer *s, z_Buffer str) {
  return z_BufferReset(s, s->Data, s->Len);
}

#endif