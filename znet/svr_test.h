
#include "zerror/error.h"
#include "zrecord/record.h"
#include "ztest/test.h"
#include "znet/svr.h"
#include "zepoch/epoch.h"
#include "zutils/log.h"

z_Error z_TestHandle(void *attr, z_Record *req, z_Record **resp) {
  z_info("tid %lld", z_ThreadID());
  return z_OK;
}

void z_SvrTest() {

  z_Threads ts;
  z_Error ret = z_ThreadsInit(&ts, 8);
  z_defer(z_ThreadsDestory, &ts);
  z_ASSERT(ret == z_OK);

  z_Epoch epoch;
  ret = z_EpochInit(&epoch, 32, &ts);
  z_defer(z_EpochDestory, &epoch);
  z_ASSERT(ret == z_OK);

  z_Svr svr;
  ret = z_SvrRun(&svr, "", 10226, &epoch, nullptr, z_TestHandle);
  z_ASSERT(ret == z_OK);
}