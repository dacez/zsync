#include <pthread.h>

#include "zkv/kv_loop_test.h"
#include "ztest/test.h"

void z_OneThread() {
  char *binlog_path = "./bin/binlog.log";
  remove(binlog_path);

  z_KV kv;
  z_Error ret =
      z_KVInit(&kv, binlog_path, 1024LL * 1024LL * 1024LL, 1024 * 1024);
  z_ASSERT_TRUE(ret == z_OK);

  bool loop_ret = z_Loop(&kv, 0, 10000);
  z_ASSERT_TRUE(loop_ret == true);

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

void *z_KVThreadFunc(void *ptr) {
  z_Arg *arg = (z_Arg *)ptr;
  arg->ret = z_Loop(arg->kv, arg->start, arg->count);
  return nullptr;
}

void z_MutilThread() {

  int64_t thread_count = 64;
  int64_t step = 100000;

  char *binlog_path = "./bin/binlog.log";
  remove(binlog_path);
  z_KV kv;
  z_Error ret = z_KVInit(&kv, binlog_path, 1024LL * 1024LL * 1024LL, 10240);
  z_ASSERT_TRUE(ret == z_OK);

  z_Arg *args = z_malloc(sizeof(z_Arg) * thread_count);
  for (int i = 0; i < thread_count; i++) {
    args[i].kv = &kv;
    args[i].start = i * step;
    args[i].count = step;
  }
  for (int i = 0; i < thread_count; i++) {
    int p_ret = pthread_create(&args[i].tid, nullptr, z_KVThreadFunc, &args[i]);
    if (p_ret != 0) {
      z_ASSERT_TRUE("p_ret != 0");
    }
  }

  for (int i = 0; i < thread_count; i++) {
    int p_ret = pthread_join(args[i].tid, nullptr);
    if (p_ret != 0) {
      z_ASSERT_TRUE("p_ret != 0");
    }
  }

  bool func_ret = true;
  for (int i = 0; i < thread_count; i++) {
    if (args[i].ret == false)
      func_ret = false;
  }

  z_ASSERT_TRUE(func_ret == true);
  z_free(args);
  z_KVDestroy(&kv);
  return;
}

void z_KVCocurrentTest() {
  z_OneThread();
  z_MutilThread();
}