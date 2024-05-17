#include "zepoch/epoch.h"
#include "ztest/test.h"

void z_EpochTest() {
  z_Threads ts;
  z_Error ret = z_ThreadsInit(&ts, 1024);
  z_ASSERT_TRUE(ret == z_OK);

  z_Epoch e;
  ret = z_EpochInit(&e, 32, &ts);
  z_ASSERT_TRUE(ret == z_OK);

  ret = z_ThreadIDInit(&ts);
  z_ASSERT_TRUE(ret == z_OK);

  int64_t tid = z_ThreadID();
  z_ASSERT_TRUE(tid != z_INVALID_THREAD_ID);

  z_ThreadIDDestory(&ts);
  z_ThreadsDestroy(&ts);
  z_EpochDestroy(&e);
}
