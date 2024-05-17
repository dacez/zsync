#ifndef z_THREADS_H
#define z_THREADS_H

#include "zerror/error.h"
#include "zutils/lock.h"
#include "zutils/mem.h"

#define z_INVALID_THREAD_ID -1
#define z_INVALID_EPOCH -1

thread_local int64_t z_thread_id = z_INVALID_THREAD_ID;

typedef struct {
  z_Lock *Locks;
  int64_t Len;
} z_Threads;

z_Error z_ThreadsInit(z_Threads *t, int64_t len) {
  if (t == nullptr) {
    return z_ERR_INVALID_DATA;
  }
  t->Len = len;
  t->Locks = z_malloc(sizeof(z_Lock) * t->Len);
  if (t->Locks == nullptr) {
    return z_ERR_NOSPACE;
  }
  for (int64_t i = 0; i < t->Len; ++i) {
    z_LockInit(&t->Locks[i]);
  }

  return z_OK;
}

void z_ThreadsDestroy(z_Threads *t) {
  if (t->Locks == nullptr) {
    return;
  }

  for (int64_t i = 0; i < t->Len; ++i) {
    z_LockDestroy(&t->Locks[i]);
  }

  z_free(t->Locks);
}

z_Error z_ThreadIDInit(z_Threads *t) {
  if (t == nullptr) {
    return z_ERR_INVALID_DATA;
  }

  if (z_thread_id != z_INVALID_THREAD_ID) {
    return z_ERR_INVALID_DATA;
  }

  for (int64_t i = 0; i < t->Len; ++i) {
    if (z_LockTryLock(&t->Locks[i])) {
      z_thread_id = i;
      break;
    }
  }

  if (z_thread_id == z_INVALID_THREAD_ID) {
    return z_ERR_NOSPACE;
  }

  return z_OK;
}

void z_ThreadIDDestory(z_Threads *t) {
  if (t == nullptr || z_thread_id == z_INVALID_THREAD_ID) {
    return;
  }

  z_LockUnLock(&t->Locks[z_thread_id]);
}

int64_t z_ThreadID() { return z_thread_id; }
#endif