#ifndef z_EPOCH_H
#define z_EPOCH_H

#include <stdatomic.h>
#include <stdint.h>

#include "zerror/error.h"
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
  z_EpochAction *Actions;
  int64_t ActionsLen;
  z_Threads *Ts;
} z_Epoch;

void z_EpochDestory(z_Epoch *e) {
  if (e->LocalEpochs != nullptr) {
    z_free(e->LocalEpochs);
    e->LocalEpochs = nullptr;
  } else {
    z_debug("e->LocalEpochs == nullptr");
  }

  if (e->Actions != nullptr) {
    z_free(e->Actions);
    e->Actions = nullptr;
  } else {
    z_debug("e->Actions = nullptr");
  }

  return;
}

z_Error z_EpochInit(z_Epoch *e, int64_t action_len, z_Threads *ts) {
  if (e == nullptr) {
    z_error("e == nullptr") return z_ERR_INVALID_DATA;
  }
  e->ActionsLen = action_len;
  e->Ts = ts;
  atomic_store(&e->CurrentEpoch, 0);
  e->LocalEpochs = z_malloc(sizeof(atomic_int_fast64_t) * e->Ts->Len);
  if (e->LocalEpochs == nullptr) {
    z_error("e->LocalEpochs == nullptr") return z_ERR_NOSPACE;
  }

  e->Actions = z_malloc(sizeof(z_EpochAction) * e->ActionsLen);
  if (e->Actions == nullptr) {
    z_error("e->Actions == nullptr") z_EpochDestory(e);
    return z_ERR_NOSPACE;
  }

  for (int64_t i = 0; i < e->Ts->Len; ++i) {
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

z_Error z_EpochProtect(z_Epoch *e) {
  if (e == nullptr) {
    z_error("e == nullptr");
    return z_ERR_INVALID_DATA;
  }
  atomic_store(&e->LocalEpochs[z_ThreadID()], atomic_load(&e->CurrentEpoch));
  return z_OK;
}

z_Error z_EpochUnProtect(z_Epoch *e) {
  if (e == nullptr) {
    z_error("e == nullptr");
    return z_ERR_INVALID_DATA;
  }
  atomic_store(&e->LocalEpochs[z_ThreadID()], z_INVALID_EPOCH);
  return z_OK;
}

z_Error z_EpochSafe(z_Epoch *e, int64_t *se) {
  if (e == nullptr || se == nullptr) {
    z_error("e == nullptr || se == nullptr");
    return z_ERR_INVALID_DATA;
  }

  *se = INT64_MAX;
  for (int64_t i = 0; i < e->Ts->Len; ++i) {
    int64_t le = atomic_load(&e->LocalEpochs[i]);
    if (le != -1 && le < *se) {
      *se = le;
    }
  }

  return z_OK;
}

z_Error z_EpochRunActions(z_Epoch *e) {
  if (e == nullptr) {
    z_error("e == nullptr");
    return z_ERR_INVALID_DATA;
  }

  int64_t safe_epoch = z_INVALID_EPOCH;
  z_Error ret = z_EpochSafe(e, &safe_epoch);
  if (ret != z_OK) {
    return ret;
  }

  for (int64_t i = 0; i < e->ActionsLen; ++i) {
    int64_t action_epoch = atomic_load(&e->Actions[i].Epoch);
    if (action_epoch != z_INVALID_EPOCH && action_epoch < safe_epoch) {
      void *Attr = e->Actions[i].Attr;
      uint64_t Addr = e->Actions[i].Addr;
      z_EpochFunc *Func = e->Actions[i].Func;
      ret = Func(Attr, Addr);
      if (ret != z_OK) {
        continue;
      }
      atomic_store(&e->Actions[i].Epoch, z_INVALID_EPOCH);
    }
  }

  return z_OK;
}

z_Error z_EpohBump(z_Epoch *e, z_EpochAction action) {
  if (e == nullptr || action.Func == nullptr) {
    z_error("e == nullptr || se == nullptr");
    return z_ERR_INVALID_DATA;
  }

  int64_t current_epoch = atomic_fetch_add(&e->CurrentEpoch, 1);
  atomic_store(&action.Epoch, current_epoch);

  z_Error ret = z_EpochRunActions(e);
  if (ret != z_OK) {
    return ret;
  }

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

  z_error("no more space");
  return z_ERR_NOSPACE;
}

#endif