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

void *CliRun(void *) {
  z_ClientTest();
  return nullptr;
}

int main() {
  z_Error ret = z_LogInit("./bin/log.txt", 2);
  z_ASSERT_TRUE(ret == z_OK);
  z_defer(z_LogDestroy);

  z_TEST_START();

  int64_t count = 1;
  pthread_t *cli_tids = z_malloc(sizeof(pthread_t) * count);
  z_defer(
      ^(pthread_t *ts) {
        z_free(cli_tids);
      },
      cli_tids);

  pthread_t tid;
  pthread_create(&tid, nullptr, SvrRun, nullptr);
  sleep(1);

  for (int64_t i = 0; i < count; ++i) {
    pthread_create(&cli_tids[i], nullptr, CliRun, nullptr);
  }

  for (int64_t i = 0; i < count; ++i) {
    pthread_join(cli_tids[i], nullptr);
  }

  z_TEST_END();
  return 0;
}