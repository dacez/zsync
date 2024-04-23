#include <pthread.h>

#include "zkv/kv.h"
#include "ztest/test.h"

bool z_Insert(z_KV *kv, int64_t i) {
  z_Buffer k, v;
  char key[32] = {};
  char value[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(value, "value%lld", i);

  z_BufferInit(&k, (int8_t *)key, strlen(key));
  z_BufferInit(&v, (int8_t *)value, strlen(value));

  z_Error ret = z_KVInsert(kv, k, v);

  z_BufferDestroy(&k);
  z_BufferDestroy(&v);

  return ret == z_OK;
}

bool z_ForceUpdate(z_KV *kv, int64_t i, int64_t iv) {
  z_Buffer k, v;
  char key[32] = {};
  char value[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(value, "value%lld", iv);

  z_BufferInit(&k, (int8_t *)key, strlen(key));
  z_BufferInit(&v, (int8_t *)value, strlen(value));

  z_Error ret = z_KVForceUpdate(kv, k, v);

  z_BufferDestroy(&k);
  z_BufferDestroy(&v);

  return ret == z_OK;
}

bool z_Update(z_KV *kv, int64_t i, int64_t vi, int64_t src_i) {
  z_Buffer k, v, src_v;
  char key[32] = {};
  char value[32] = {};
  char src_value[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(value, "value%lld", vi);
  sprintf(src_value, "value%lld", src_i);

  z_BufferInit(&k, (int8_t *)key, strlen(key));
  z_BufferInit(&v, (int8_t *)value, strlen(value));
  z_BufferInit(&src_v, (int8_t *)src_value, strlen(src_value));

  z_Error ret = z_KVUpdate(kv, k, v, src_v);

  z_BufferDestroy(&k);
  z_BufferDestroy(&v);
  z_BufferDestroy(&src_v);

  return ret == z_OK;
}

bool z_UpdateConflict(z_KV *kv, int64_t i, int64_t vi, int64_t src_i) {
  z_Buffer k, v, src_v;
  char key[32] = {};
  char value[32] = {};
  char src_value[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(value, "value%lld", vi);
  sprintf(src_value, "value%lld", src_i);

  z_BufferInit(&k, (int8_t *)key, strlen(key));
  z_BufferInit(&v, (int8_t *)value, strlen(value));
  z_BufferInit(&src_v, (int8_t *)src_value, strlen(src_value));

  z_Error ret = z_KVUpdate(kv, k, v, src_v);

  z_BufferDestroy(&k);
  z_BufferDestroy(&v);
  z_BufferDestroy(&src_v);

  return ret == z_ERR_CONFLICT;
}

bool z_Find(z_KV *kv, int64_t i, int64_t iv) {
  z_Buffer k, v, vv;
  char key[32] = {};
  char value[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(value, "value%lld", iv);

  z_BufferInit(&k, (int8_t *)key, strlen(key));
  v = z_BufferEmpty();
  z_BufferInit(&vv, (int8_t *)value, strlen(value));

  z_Error ret = z_KVFind(kv, k, &v);
  bool isEqual = false;
  if (ret == z_OK) {
    isEqual = z_BufferIsEqual(v, vv);
  }

  z_BufferDestroy(&k);
  z_BufferDestroy(&v);
  z_BufferDestroy(&vv);

  return ret == z_OK && isEqual == true;
}

bool z_Delete(z_KV *kv, int64_t i) {
  z_Buffer k;
  char key[32] = {};
  sprintf(key, "key%lld", i);

  z_BufferInit(&k, (int8_t *)key, strlen(key));

  z_Error ret = z_KVDelete(kv, k);

  z_BufferDestroy(&k);

  return ret == z_OK;
}

bool z_FindNotFound(z_KV *kv, int64_t i) {
  z_Buffer k, v;
  char key[32] = {};
  sprintf(key, "key%lld", i);

  z_BufferInit(&k, (int8_t *)key, strlen(key));
  v = z_BufferEmpty();

  z_Error ret = z_KVFind(kv, k, &v);

  z_BufferDestroy(&k);
  z_BufferDestroy(&v);

  return ret == z_ERR_NOT_FOUND;
}

bool z_DeleteNotFound(z_KV *kv, int64_t i) {
  z_Buffer k;
  char key[32] = {};
  sprintf(key, "key%lld", i);

  z_BufferInit(&k, (int8_t *)key, strlen(key));

  z_Error ret = z_KVDelete(kv, k);

  z_BufferDestroy(&k);

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

bool z_LoopCheck(z_KV *kv, int64_t start, int64_t count) {
  bool final_ret = true;
  bool ret = false;
  for (int64_t i = start; i < count; ++i) {
    ret = z_FindNotFound(kv, i);
    if (ret == false) {
      final_ret = false;
    }
  }

  return final_ret;
}

void z_OneThread() {
  char *binlog_path = "./bin/binlog.log";
  remove(binlog_path);

  z_KV kv;
  z_Error ret =
      z_KVInit(&kv, binlog_path, 1024LL * 1024LL * 1024LL, 1024 * 1024);
  z_ASSERT(ret == z_OK);

  bool loop_ret = z_Loop(&kv, 0, 10000);
  z_ASSERT(loop_ret == true);

  z_KVDestroy(&kv);
  return;
}

typedef struct {
  z_KV *kv;
  int64_t start;
  int64_t count;
  bool ret;
  pthread_t tid;
} z_Arg;

void* z_KVThreadFunc(void *ptr) {
  z_Arg *arg = (z_Arg*)ptr;
  arg->ret = z_Loop(arg->kv, arg->start, arg->count);
  return nullptr;
}

void z_MutilThread() {

  int64_t thread_count = 64;
  int64_t step = 100000;

  char *binlog_path = "./bin/binlog.log";
  remove(binlog_path);
  z_KV kv;
  z_Error ret =
      z_KVInit(&kv, binlog_path, 1024LL * 1024LL * 1024LL, 10240);
  z_ASSERT(ret == z_OK);

  z_Arg *args = z_malloc(sizeof(z_Arg) * thread_count);
  for (int i = 0; i < thread_count; i++) {
    args[i].kv = &kv;
    args[i].start = i * step;
    args[i].count = step;
  }
  for (int i = 0; i < thread_count; i++) {
    int p_ret = pthread_create(&args[i].tid, nullptr, z_KVThreadFunc, &args[i]);
    if (p_ret != 0) {
      z_ASSERT("p_ret != 0");
    }
  }

  for (int i = 0; i < thread_count; i++) {
    int p_ret = pthread_join(args[i].tid, nullptr);
    if (p_ret != 0) {
      z_ASSERT("p_ret != 0");
    }
  }

  bool func_ret = true;
  for (int i = 0; i < thread_count; i++) {
    if (args[i].ret == false) func_ret = false;
  }

  z_ASSERT(func_ret == true);
  z_free(args);
  z_KVDestroy(&kv);
  return;
}

void z_FromFile() {
  char *binlog_path = "./bin/binlog.log";
  remove(binlog_path);
  int64_t count = 10000;

  z_KV kv;
  z_Error ret =
      z_KVInit(&kv, binlog_path, 1024LL * 1024LL * 1024LL, 1024 * 1024);
  z_ASSERT(ret == z_OK);

  bool loop_ret = z_Loop(&kv, 0, count);
  z_ASSERT(loop_ret == true);

  z_KVDestroy(&kv);

  ret = z_KVInit(&kv, binlog_path, 1024LL * 1024LL * 1024LL, 1024 * 128);
  z_ASSERT(ret == z_OK);
  
  //loop_ret = z_LoopCheck(&kv, 0, count);
  //z_ASSERT(loop_ret == true);

  z_KVDestroy(&kv);
  return;
} 
void z_KVLoopTest() {
  z_OneThread();
  z_MutilThread();
  z_FromFile();
}