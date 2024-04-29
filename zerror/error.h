#ifndef z_ERROR_H
#define z_ERROR_H

typedef enum {
  z_OK = 0,
  z_ERR_INVALID_DATA = 1,
  z_ERR_NOSPACE = 2,
  z_ERR_FS = 3,

  z_ERR_EXIST = 1024,
  z_ERR_NOT_FOUND = 1025,
  z_ERR_CONFLICT = 1026,

  z_ERR_CACHE_MISS = 2048,
} z_Error;

#endif