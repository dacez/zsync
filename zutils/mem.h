#ifndef z_MEM_H
#define z_MEM_H

#include <stdlib.h> // IWYU pragma: export
#include "zutils/log.h"

#define z_malloc(s) malloc(s);
#define z_free(ptr)                                                            \
  {                                                                            \
    if (ptr == nullptr) {                                                      \
      z_panic("ptr == nullptr");                                               \
    }                                                                          \
    free(ptr);                                                                 \
  }

#endif