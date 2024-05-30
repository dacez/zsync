#ifndef z_THREADS_H
#define z_THREADS_H

#include <pthread.h>
#include <stdint.h>

#include "zerror/error.h"
#include "zutils/lock.h"
#include "zutils/mem.h"

#define z_INVALID_THREAD_ID -1
#define z_INVALID_EPOCH -1

#define z_Thread pthread_t
#define z_ThreadCreate(thread, func, arg) pthread_create(thread, nullptr, func, arg)
#define z_ThreadJion(thread) pthread_join(thread, nullptr)
typedef void* (*z_ThreadFunc) (void *);

thread_local int64_t z_thread_id = z_INVALID_THREAD_ID;

typedef struct {
  z_Lock *Locks;
  int64_t LocksLen;
} z_ThreadIDs;

z_Error z_ThreadIDsInit(z_ThreadIDs *t, int64_t thread_count) {
  if (t == nullptr) {
    return z_ERR_INVALID_DATA;
  }
  t->LocksLen = thread_count;
  t->Locks = z_malloc(sizeof(z_Lock) * t->LocksLen);
  if (t->Locks == nullptr) {
    return z_ERR_NOSPACE;
  }
  for (int64_t i = 0; i < t->LocksLen; ++i) {
    z_LockInit(&t->Locks[i]);
  }

  return z_OK;
}

void z_ThreadIDsDestroy(z_ThreadIDs *t) {
  if (t->Locks == nullptr) {
    return;
  }

  for (int64_t i = 0; i < t->LocksLen; ++i) {
    z_LockDestroy(&t->Locks[i]);
  }

  z_free(t->Locks);
}

z_Error z_ThreadIDInit(z_ThreadIDs *t) {
  if (t == nullptr) {
    return z_ERR_INVALID_DATA;
  }

  if (z_thread_id != z_INVALID_THREAD_ID) {
    return z_ERR_INVALID_DATA;
  }

  for (int64_t i = 0; i < t->LocksLen; ++i) {
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

void z_ThreadIDDestroy(z_ThreadIDs *t) {
  if (t == nullptr || z_thread_id == z_INVALID_THREAD_ID) {
    return;
  }

  z_LockUnLock(&t->Locks[z_thread_id]);
  z_thread_id = z_INVALID_THREAD_ID;
}

int64_t z_ThreadID() { return z_thread_id; }

typedef struct {
  z_Thread *Ts;
  int64_t TsLen;
} z_Threads;

z_Error z_ThreadsInit(z_Threads *ts, int64_t tc, z_ThreadFunc func, void *arg) {
  ts->TsLen = tc;
  ts->Ts = z_malloc(sizeof(z_Thread) * tc);
  for (int64_t i = 0; i < tc; ++i) {
    z_ThreadCreate(&ts->Ts[i], func, arg);
  }
  return z_OK;
}

void z_ThreadsDestroy(z_Threads *ts) {
  if (ts->Ts != nullptr) {
    for (int64_t i = 0; i < ts->TsLen; ++i) {
      z_ThreadJion(ts->Ts[i]);
    }
    z_free(ts->Ts);
  }
}
#endif