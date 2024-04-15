#ifndef z_MEM_H
#define z_MEM_H

#define z_malloc(s) malloc(s);
#define z_free(ptr) {if (ptr == nullptr) {z_panic("ptr == nullptr");}free(ptr);}

#endif