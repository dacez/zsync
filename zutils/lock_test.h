#include <pthread.h>
#include <stdio.h>

#include "ztest/test.h"
#include "zutils/utils.h"

z_Lock z_lock;
int64_t z_lock_test_count = 0;
int64_t z_lock_loop_count = 1000;

void *z_LockThreadFunc(void *) {
  for (int i = 0; i < z_lock_loop_count; i++) {
    z_LockLock(&z_lock);
    z_lock_test_count++;
    z_LockUnLock(&z_lock);
  }

  return nullptr;
}

void z_LockTest() {
  int64_t thread_count = 128;

  z_LockInit(&z_lock);

  pthread_t *ts = z_malloc(sizeof(pthread_t) * thread_count);
  for (int i = 0; i < thread_count; i++) {
    pthread_create(&ts[i], nullptr, z_LockThreadFunc, nullptr);
  }

  for (int i = 0; i < thread_count; i++) {
    pthread_join(ts[i], nullptr);
  }

  z_LockDestroy(&z_lock);

  z_ASSERT(atomic_load(&z_lock.Count) > 0);
  z_ASSERT(z_lock_test_count == thread_count * z_lock_loop_count);

  z_free(ts);
}