
#include "zerror/error.h"
#include "zrecord/record.h"
#include "ztest/test.h"
#include "znet/svr.h"
#include "zepoch/epoch.h"
#include "zutils/buffer.h"

z_Error z_TestHandle(void *attr, z_Record *req, z_Record **resp) {
  z_Buffer k = {};
  z_Buffer v = {};
  *resp = z_RecordNew(z_ROP_INSERT, k, v);
  return z_OK;
}

void z_SvrTest() {

  z_Threads ts;
  z_Error ret = z_ThreadsInit(&ts, 8);
  z_defer(z_ThreadsDestory, &ts);
  z_ASSERT_TRUE(ret == z_OK);

  z_Epoch epoch;
  ret = z_EpochInit(&epoch, 32, &ts);
  z_defer(z_EpochDestory, &epoch);
  z_ASSERT_TRUE(ret == z_OK);

  z_Svr svr;
  ret = z_SvrRun(&svr, "127.0.0.1", 12301, &epoch, nullptr, z_TestHandle);
  z_ASSERT_TRUE(ret == z_OK);
}