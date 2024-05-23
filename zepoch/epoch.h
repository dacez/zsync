#ifndef z_EPOCH_H
#define z_EPOCH_H

#include <stdatomic.h>
#include <stdint.h>

#include "zerror/error.h"
#include "zutils/assert.h"
#include "zutils/log.h"
#include "zutils/mem.h"
#include "zutils/threads.h"

typedef z_Error z_EpochFunc(void *, uint64_t);

typedef struct {
  atomic_int_fast64_t Epoch;
  void *Attr;
  uint64_t Addr;
  z_EpochFunc *Func;
} z_EpochAction;

typedef struct {
  atomic_int_fast64_t CurrentEpoch;
  atomic_int_fast64_t *LocalEpochs;
  int64_t LocalEpochsLen;
  z_EpochAction *Actions;
  int64_t ActionsLen;
} z_Epoch;

void z_EpochDestroy(z_Epoch *e) {
  if (e->LocalEpochs != nullptr) {
    z_free(e->LocalEpochs);
    e->LocalEpochsLen = 0;
  }

  if (e->Actions != nullptr) {
    z_free(e->Actions);
    e->ActionsLen = 0;
  }
  return;
}

z_Error z_EpochInit(z_Epoch *e, int64_t local_epochs_len, int64_t actions_len) {
  z_assert(e != nullptr);

  e->LocalEpochsLen = local_epochs_len;
  e->ActionsLen = actions_len;
  atomic_store(&e->CurrentEpoch, 0);

  e->LocalEpochs = z_malloc(sizeof(atomic_int_fast64_t) * e->LocalEpochsLen);
  if (e->LocalEpochs == nullptr) {
    z_error("e->LocalEpochs == nullptr");
    return z_ERR_NOSPACE;
  }

  e->Actions = z_malloc(sizeof(z_EpochAction) * e->ActionsLen);
  if (e->Actions == nullptr) {
    z_error("e->Actions == nullptr");
    z_free(e->LocalEpochs);
    return z_ERR_NOSPACE;
  }

  for (int64_t i = 0; i < e->LocalEpochsLen; ++i) {
    atomic_store(&e->LocalEpochs[i], z_INVALID_EPOCH);
  }

  for (int64_t i = 0; i < e->ActionsLen; ++i) {
    atomic_store(&e->Actions[i].Epoch, z_INVALID_EPOCH);
    e->Actions[i].Attr = nullptr;
    e->Actions[i].Addr = 0;
    e->Actions[i].Func = nullptr;
  }

  return z_OK;
}

void z_EpochProtect(z_Epoch *e) {
  z_assert(e != nullptr);
  z_assert(z_ThreadID() != z_INVALID_THREAD_ID);
  atomic_store(&e->LocalEpochs[z_ThreadID()], atomic_load(&e->CurrentEpoch));
}

void z_EpochUnProtect(z_Epoch *e) {
  z_assert(e != nullptr);
  z_assert(z_ThreadID() != z_INVALID_THREAD_ID);
  atomic_store(&e->LocalEpochs[z_ThreadID()], z_INVALID_EPOCH);
}

int64_t z_EpochSafe(z_Epoch *e) {
  z_assert(e != nullptr);

  int64_t se = INT64_MAX;
  for (int64_t i = 0; i < e->LocalEpochsLen; ++i) {
    int64_t le = atomic_load(&e->LocalEpochs[i]);
    if (le != z_INVALID_EPOCH && le < se) {
      se = le;
    }
  }

  return se;
}

void z_EpochRunActions(z_Epoch *e) {
  z_assert(e != nullptr);
  int64_t safe_epoch =  z_EpochSafe(e);

  for (int64_t i = 0; i < e->ActionsLen; ++i) {
    int64_t action_epoch = atomic_load(&e->Actions[i].Epoch);
    if (action_epoch != z_INVALID_EPOCH && action_epoch < safe_epoch) {
      void *Attr = e->Actions[i].Attr;
      uint64_t Addr = e->Actions[i].Addr;
      z_EpochFunc *Func = e->Actions[i].Func;
      z_Error ret = Func(Attr, Addr);
      if (ret != z_OK) {
        z_error("action run failed %d", ret);
        continue;
      }
      atomic_store(&e->Actions[i].Epoch, z_INVALID_EPOCH);
    }
  }
}

z_Error z_EpohBump(z_Epoch *e, z_EpochAction action) {
  z_assert(e != nullptr, action.Func != nullptr);

  int64_t current_epoch = atomic_fetch_add(&e->CurrentEpoch, 1);
  z_EpochRunActions(e);
  for (int64_t i = 0; i < e->ActionsLen; ++i) {
    int64_t action_epoch = atomic_load(&e->Actions[i].Epoch);
    if (action_epoch == z_INVALID_EPOCH) {
      if (atomic_compare_exchange_strong(&e->Actions[i].Epoch, &action_epoch,
                                         current_epoch) == true) {
        e->Actions[i].Attr = action.Attr;
        e->Actions[i].Addr = action.Addr;
        e->Actions[i].Func = action.Func;
        return z_OK;
      }
    }
  }

  z_error("no more free actions");
  return z_ERR_NOSPACE;
}

#endif