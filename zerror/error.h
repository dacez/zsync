#ifndef z_ERROR_H
#define z_ERROR_H

#include <stdint.h>
typedef enum int8_t {
  z_OK = 0,
  z_ERR_INVALID_DATA = 1,
  z_ERR_NOSPACE = 2,
  z_ERR_FS = 3,
  z_ERR_NET = 4,

  z_ERR_EXIST = 32,
  z_ERR_NOT_FOUND = 33,
  z_ERR_CONFLICT = 34,

  z_ERR_CACHE_MISS = 64,
} z_Error;

#endif