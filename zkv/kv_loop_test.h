#ifndef z_KV_LOOP_TEST_H
#define z_KV_LOOP_TEST_H

#include "zkv/kv.h"
#include "zutils/buffer.h"
#include <string.h>

bool z_Insert(z_KV *kv, int64_t i) {
  char key[32] = {};
  char value[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(value, "value%lld", i);

  z_ConstBuffer k = {.Data = key, .Size = strlen(key)};
  z_ConstBuffer v = {.Data = value, .Size = strlen(value)};
  z_Error ret = z_KVInsert(kv, k, v);
  return ret == z_OK;
}

bool z_ForceUpdate(z_KV *kv, int64_t i, int64_t iv) {
  char key[32] = {};
  char value[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(value, "value%lld", iv);

  z_ConstBuffer k = {.Data = key, .Size = strlen(key)};
  z_ConstBuffer v = {.Data = value, .Size = strlen(value)};

  z_Error ret = z_KVForceUpdate(kv, k, v);

  return ret == z_OK;
}

bool z_Update(z_KV *kv, int64_t i, int64_t vi, int64_t src_i) {
  char key[32] = {};
  char value[32] = {};
  char src_value[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(value, "value%lld", vi);
  sprintf(src_value, "value%lld", src_i);

  z_ConstBuffer k = {.Data = key, .Size = strlen(key)};
  z_ConstBuffer v = {.Data = value, .Size = strlen(value)};
  z_ConstBuffer src_v = {.Data = src_value, .Size = strlen(src_value)};

  z_Error ret = z_KVUpdate(kv, k, v, src_v);

  return ret == z_OK;
}

bool z_UpdateConflict(z_KV *kv, int64_t i, int64_t vi, int64_t src_i) {
  char key[32] = {};
  char value[32] = {};
  char src_value[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(value, "value%lld", vi);
  sprintf(src_value, "value%lld", src_i);

  z_ConstBuffer k = {.Data = key, .Size = strlen(key)};
  z_ConstBuffer v = {.Data = value, .Size = strlen(value)};
  z_ConstBuffer src_v = {.Data = src_value, .Size = strlen(src_value)};

  z_Error ret = z_KVUpdate(kv, k, v, src_v);

  return ret == z_ERR_CONFLICT;
}

bool z_Find(z_KV *kv, int64_t i, int64_t iv) {
  char key[32] = {};
  char value[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(value, "value%lld", iv);

  z_ConstBuffer k = {.Data = key, .Size = strlen(key)};
  z_ConstBuffer v = {.Data = value, .Size = strlen(value)};

  z_unique(z_Buffer) buffer = {};
  z_Error ret = z_KVFind(kv, k, &buffer);
  bool isEqual = false;
  if (ret == z_OK) {
    isEqual = z_BufferIsEqual(&buffer, &v);
  }

  return ret == z_OK && isEqual == true;
}

bool z_Delete(z_KV *kv, int64_t i) {
  char key[32] = {};
  sprintf(key, "key%lld", i);

  z_ConstBuffer k = {.Data = key, .Size = strlen(key)};

  z_Error ret = z_KVDelete(kv, k);

  return ret == z_OK;
}

bool z_FindNotFound(z_KV *kv, int64_t i) {
  char key[32] = {};
  sprintf(key, "key%lld", i);

  z_ConstBuffer k = {.Data = key, .Size = strlen(key)};
  z_unique(z_Buffer) v = {};

  z_Error ret = z_KVFind(kv, k, &v);

  return ret == z_ERR_NOT_FOUND;
}

bool z_DeleteNotFound(z_KV *kv, int64_t i) {
  char key[32] = {};
  sprintf(key, "key%lld", i);

  z_ConstBuffer k = {.Data = key, .Size = strlen(key)};

  z_Error ret = z_KVDelete(kv, k);

  return ret == z_ERR_NOT_FOUND;
}

bool z_Loop(z_KV *kv, int64_t start, int64_t count) {
  bool final_ret = true;
  bool ret = false;

  for (int64_t i = start; i < count; ++i) {
    ret = z_Insert(kv, i);
    if (ret == false) {
      final_ret = false;
    }
  }

  for (int64_t i = start; i < count; ++i) {
    ret = z_Find(kv, i, i);
    if (ret == false) {
      final_ret = false;
    }
  }

  for (int64_t i = start; i < count; ++i) {
    ret = z_ForceUpdate(kv, i, count - 1 - i);
    if (ret == false) {
      final_ret = false;
    }
  }

  for (int64_t i = start; i < count; ++i) {
    ret = z_Find(kv, i, count - 1 - i);
    if (ret == false) {
      final_ret = false;
    }
  }

  for (int64_t i = start; i < count; ++i) {
    ret = z_Update(kv, i, i, count - 1 - i);
    if (ret == false) {
      final_ret = false;
    }
  }

  for (int64_t i = start; i < count; ++i) {
    ret = z_UpdateConflict(kv, i, i, count - 1 - i);
    if (ret == false) {
      final_ret = false;
    }
  }

  for (int64_t i = start; i < count; ++i) {
    ret = z_Find(kv, i, i);
    if (ret == false) {
      final_ret = false;
    }
  }

  for (int64_t i = start; i < count; ++i) {
    ret = z_Delete(kv, i);
    if (ret == false)
      final_ret = false;
  }

  for (int64_t i = start; i < count; ++i) {
    ret = z_FindNotFound(kv, i);
    if (ret == false)
      final_ret = false;
  }

  for (int64_t i = start; i < count; ++i) {
    ret = z_DeleteNotFound(kv, i);
    if (ret == false)
      final_ret = false;
  }

  return final_ret;
}

#endif