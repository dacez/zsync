
#include "zerror/error.h"
#include "zkv/kv.h"
#include "zrecord/record.h"
#include "ztest/test.h"
#include "znet/svr.h"
#include "zepoch/epoch.h"
#include "zutils/buffer.h"

z_Error z_TestHandle(void *attr, z_Record *req, z_Record **resp) {
  z_KV *kv = (z_KV*)attr;
  if (req->OP != z_ROP_FIND) {
    return z_KVFromRecord(kv, req);
  }

  z_Buffer key = {};
  z_Error ret = z_RecordKey(req, &key);
  if (ret != z_OK) {
    return ret;
  }

  z_Buffer val = {};
  ret = z_KVFind(kv, key, &val);
  if (ret != z_OK) {
    return ret;
  }

  z_Buffer empty = {};
  *resp = z_RecordNewByKV(req->OP, empty, val);
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

  z_KV kv;
  ret = z_KVInit(&kv, "./bin/binlog.log", 1024*1024*1024, 1024);
  z_ASSERT_TRUE(ret == z_OK);

  z_Svr svr;
  ret = z_SvrRun(&svr, "127.0.0.1", 12301, &epoch, &kv, z_TestHandle);
  z_ASSERT_TRUE(ret == z_OK);
  z_KVDestroy(&kv);

  return;
}