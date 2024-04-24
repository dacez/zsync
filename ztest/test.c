#define NDEBUG

#include "ztest/test.h"

#include "zbinlog/record_test.h"
#include "zkv/kv_loop_test.h"
#include "zkv/kv_test.h"
#include "zutils/utils_test.h"
#include "zkv/kv_cocurrent_test.h"
#include "zkv/kv_restore_test.h"

int main() {
  z_LogInit("./bin/log.txt");

  z_ASSERT(1);
  z_CHECK(1);
  z_FileRecordTest();
  z_TimeTest();
  z_LockTest();
  z_KVTest();
  z_KVCocurrentTest();
  z_KVRestoreTest();

  z_LogDestroy();
}