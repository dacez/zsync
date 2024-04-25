#ifndef z_EPOCH_H
#define z_EPOCH_H

#include <stdatomic.h>
#include <stdint.h>

#include <zutils/utils.h>
#include <zerror/error.h>

#define z_MAX_THREAD_COUNT 1024
#define z_INVALID_THREAD_ID -1

thread_local int64_t z_thread_id = z_INVALID_THREAD_ID;

typedef struct {
  z_Lock *Locks;
} z_Thread;

z_Error z_ThreadInit(z_Thread *t) {
  if (t == nullptr) {
    z_error("ts == nullptr");
    return z_ERR_INVALID_DATA;
  }
  t->Locks = z_malloc(sizeof(z_Lock) * z_MAX_THREAD_COUNT);
  if (t->Locks == nullptr) {
    z_error("ts->Locks == nullptr");
    return z_ERR_NOSPACE;
  }
  for (int64_t i = 0; i < z_MAX_THREAD_COUNT; ++i) {
    z_LockInit(&t->Locks[i]);
  }

  return z_OK;
}

void z_ThreadDestory(z_Thread *t) {
  if (t->Locks == nullptr) {
    z_debug("ts->Locks == nullptr");
    return;
  }

  for (int64_t i = 0; i < z_MAX_THREAD_COUNT; ++i) {
    z_LockDestroy(&t->Locks[i]);
  }

  z_free(t->Locks);
}

z_Error z_ThreadIDInit(z_Thread *t) {
  if (t == nullptr) {
    z_error("t == nullptr");
    return z_ERR_INVALID_DATA;
  }

  if (z_thread_id != z_INVALID_THREAD_ID) {
    z_error("z_thread_id(%lld) != z_INVALID_THREAD_ID", z_thread_id);
    return z_ERR_INVALID_DATA;    
  }

  for (int64_t i = 0; i < z_MAX_THREAD_COUNT; ++i) {
    if (z_LockTryLock(&t->Locks[i])) {
      z_thread_id = i;
      break;
    }
  }

  if (z_thread_id == z_INVALID_THREAD_ID) {
    z_error("no more threadid");
    return z_ERR_NOSPACE;
  }
  
  return z_OK;
}

void z_ThreadIDDestory(z_Thread *t) {
  if (t == nullptr || z_thread_id == z_INVALID_THREAD_ID) {
    z_error("t == nullptr || z_thread_id == z_INVALID_THREAD_ID");
    return;    
  }

  z_LockUnLock(&t->Locks[z_thread_id]);
}

int64_t z_ThreadID() {
  return z_thread_id;
}

typedef struct {
} z_Epoch;
#endif