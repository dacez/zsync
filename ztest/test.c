#include "ztest/test.h"

#include "zbinlog/record_test.h"
#include "zepoch/epoch_test.h"
#include "zkv/kv_cocurrent_test.h"
#include "zkv/kv_restore_test.h"
#include "zkv/kv_seq_test.h"
#include "zkv/kv_test.h"
#include "zutils/lock_test.h"
#include "zutils/time_test.h"
#include "zutils/defer_test.h"
#include "zutils/micro_test.h"

int main() {
  z_LogInit("./bin/log.txt", 2);

  z_ASSERT(1);
  z_CHECK(1);
  z_MicroTest();
  z_DeferTest();

  z_FileRecordTest();
  z_TimeTest();
  z_LockTest();
  z_KVTest();
  z_KVCocurrentTest();
  z_KVRestoreTest();
  z_KVSeqTestCheck();
  z_EpochTest();

  z_LogDestroy();
}