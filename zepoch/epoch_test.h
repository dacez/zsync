#include "zepoch/epoch.h"
#include "ztest/test.h"

void z_EpochTest() {
  z_Epoch e;
  z_Error ret = z_EpochInit(&e);
  z_ASSERT(ret == z_OK);

  z_Threads ts;
  ret = z_ThreadsInit(&ts);
  z_ASSERT(ret == z_OK);

  ret = z_ThreadIDInit(&ts);
  z_ASSERT(ret == z_OK);

  int64_t tid = z_ThreadID();
  z_ASSERT(tid != z_INVALID_THREAD_ID);

  z_ThreadIDDestory(&ts);
  z_ThreadsDestory(&ts);
  z_EpochDestory(&e);
}
