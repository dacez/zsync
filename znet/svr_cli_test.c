#include "znet/client_test.h"
#include "znet/svr_test.h"
#include "zutils/defer.h"
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
  int64_t count = 10;
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
  z_defer(
      ^(pthread_t *ts) {
        for (int64_t i = 0; i < count; ++i) {
          pthread_join(ts[i], nullptr);
        }
      },
      cli_tids);

  return 0;
}