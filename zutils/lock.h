#ifndef z_LOCK_H
#define z_LOCK_H

#include <stdatomic.h>

typedef atomic_flag z_Lock;

void z_LockDestroy(z_Lock *l) {
  return;
}

void z_LockInit(z_Lock *l) {
  atomic_flag_clear(l);
}

void z_LockLock(z_Lock *l) {
  while (atomic_flag_test_and_set(l)) {
    return;
  }
}

void z_LockUnLock(z_Lock *l) {
  atomic_flag_clear(l);
}


#endif