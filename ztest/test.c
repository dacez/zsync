#include "ztest/test.h"

#include "zepoch/epoch_test.h"
#include "zkv/kv_cocurrent_test.h"
#include "zkv/kv_restore_test.h"
#include "zkv/kv_seq_test.h"
#include "zkv/kv_test.h"
#include "zutils/defer.h"
#include "zutils/lock_test.h"
#include "zutils/time_test.h"
#include "zutils/defer_test.h"
#include "zutils/macro_test.h"
#include "znet/kv_svr_cli_test.h"
#include "zutils/local_test.h"

int main() {
  z_LogInit("", 2);
  z_defer(z_LogDestroy);

  z_TEST_START();
  
  z_LocalTest();
  z_MacroTest();
  z_DeferTest();
  z_TimeTest();
  z_LockTest();
  z_KVTest();
  z_KVCocurrentTest();
  z_KVRestoreTest();
  z_KVSeqTestCheck();
  z_EpochTest();
  z_KVSvrCliTest();

  z_TEST_END();
}