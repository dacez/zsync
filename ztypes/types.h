#ifndef z_TYPES_H
#define z_TYPES_H

#include <stdint.h>

typedef struct {
  int8_t *Data;
  int64_t Len;
} z_String;

#define z_CStr(z_str, cstr) memset(cstr, 0, sizeof(cstr));memcpy(cstr, z_str.Data, z_str.Len < sizeof(cstr) - 1 ? z_str.Len : sizeof(cstr) - 1);

bool z_StringIsEqual(z_String a, z_String b) {
  if (a.Len != b.Len) {
    return false;
  }

  return memcmp(a.Data, b.Data, a.Len) == 0;
}

typedef enum {
  z_OK = 0,
  z_ERR_INVALID_DATA = 1,
  z_ERR_NOSPACE = 2,
  z_ERR_SYS = 3,
  z_ERR_EXIST = 4,
  z_ERR_NOT_FOUND = 5,
  z_ERR_CONFLICT = 6,
} z_Error;

#endif