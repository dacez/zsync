#ifndef z_MEM_H
#define z_MEM_H

#include <stdlib.h> // IWYU pragma: export

#define z_malloc(s) malloc(s);
#define z_free(ptr) free(ptr);

#endif