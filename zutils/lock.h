#ifndef z_LOCK_H
#define z_LOCK_H

#include <stdatomic.h>

typedef struct {
  atomic_flag Lock;
  atomic_uint_fast64_t Count;
} z_Lock;

void z_LockDestroy(z_Lock *l) {
  return;
}

void z_LockInit(z_Lock *l) {
  atomic_store(&l->Count, 0);
  atomic_flag_clear(&l->Lock);
}

void z_LockLock(z_Lock *l) {
  while (atomic_flag_test_and_set(&l->Lock)) {
    atomic_fetch_add(&l->Count, 1);
  }
}

void z_LockUnLock(z_Lock *l) {
  atomic_flag_clear(&l->Lock);
}


#endif