#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#include "zerror/error.h"
#include "znet/client_test.h"
#include "znet/svr_kv.h"
#include "ztest/test.h"
#include "zutils/defer.h"
#include "zutils/mem.h"
#include "zutils/threads.h"

void *SvrRun(void *arg) {
  z_SvrKV* svr_kv = (z_SvrKV*)arg;
  z_SvrKVRun(svr_kv);
  return nullptr;
}

void *CliRun(void *ptr) {
  z_ClientTest((z_ClientTestArgs *)ptr);
  return nullptr;
}

void z_KVSvrCliTest() {
  int64_t thread_count = 2;
  int64_t test_count = 1024 * 32;

  const char *bp = "./bin/binlog.log";
  remove(bp);

  z_unique(z_SvrKV) svr_kv;
  z_Error ret = z_SvrKVInit(&svr_kv, bp, 1024*1024*1024, 1024, "127.0.0.1", 12301, 16);
  z_ASSERT_TRUE(ret == z_OK);

  z_Thread t;
  z_ThreadCreate(&t, SvrRun, &svr_kv);
  sleep(1);

  z_ClientTestArgs *args = z_malloc(sizeof(z_ClientTestArgs) * thread_count);
  z_defer(
      ^(z_ClientTestArgs **ptr) {
        z_free(*ptr);
      },
      &args);

  for (int64_t i = 0; i < thread_count; ++i) {
    args[i].Start = i * test_count / thread_count;
    args[i].End = args[i].Start + test_count / thread_count;
    z_ThreadCreate(&args[i].Tid, CliRun, &args[i]);
  }

  for (int64_t i = 0; i < thread_count; ++i) {
    z_ThreadJion(args[i].Tid);
  }
}