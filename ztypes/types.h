#ifndef z_TYPES_H
#define z_TYPES_H

#include <stdint.h>

typedef struct {
  uint8_t *Data;
  uint64_t Len;
} z_String;


typedef enum {
  z_OK = 0,
  z_ERR_INVALID_DATA = 1,
  z_ERR_NOSPACE = 2,
  z_ERR_SYS = 3,
  z_ERR_EXIST = 4,
  z_ERR_NEED_CHECK = 5,
  z_ERR_NOT_FOUND = 6,
} z_Error;

#endif