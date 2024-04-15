#ifndef z_ERROR_H
#define z_ERROR_H

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