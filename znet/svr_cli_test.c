#include "znet/client_test.h"
#include "znet/svr_test.h"
#include "ztest/test.h"
#include "zutils/defer.h"
#include "zutils/log.h"
#include "zutils/mem.h"
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

void *SvrRun(void *) {
  z_SvrTest();
  return nullptr;
}

void *CliRun(void * ptr) {
  z_ClientTest((z_ClientTestArgs*)ptr);
  return nullptr;
}

int main() {
  int64_t thread_count = 1;
  int64_t test_count = 1;
  z_Error ret = z_LogInit("./bin/log.txt", 2);
  z_ASSERT_TRUE(ret == z_OK);
  z_defer(z_LogDestroy);

  pthread_t tid;
  pthread_create(&tid, nullptr, SvrRun, nullptr);
  sleep(1);

  
  z_TEST_START();
  z_ClientTestArgs *args = z_malloc(sizeof(z_ClientTestArgs) * thread_count); 
  z_defer(^{
    z_free(args);
  });

  for (int64_t i = 0; i < thread_count; ++i) {
    args[i].Start = i * test_count / thread_count;
    args[i].End = args[i].Start + test_count / thread_count;
    pthread_create(&args[i].Tid, nullptr, CliRun, &args[i]);
  }

  for (int64_t i = 0; i < thread_count; ++i) {
    pthread_join(args[i].Tid, nullptr);
  }

  z_TEST_END();
  return 0;
}