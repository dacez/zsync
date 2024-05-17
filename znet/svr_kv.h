
#ifndef z_SVR_KV_H
#define z_SVR_KV_H
#include "zepoch/epoch.h"
#include "zerror/error.h"
#include "zkv/kv.h"
#include "znet/svr.h"
#include "zrecord/record.h"
#include "zutils/buffer.h"
#include "zutils/log.h"
#include <stdint.h>

z_Error z_KVHandle(void *attr, z_Record *req, z_Record **resp) {
  z_KV *kv = (z_KV *)attr;
  z_RecordPrint(req);
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

void z_SvrKV(char *binlog_path, int64_t thread_count) {

  z_unique(z_Threads) ts;
  z_Error ret = z_ThreadsInit(&ts, thread_count);
  if (ret != z_OK) {
    z_panic("z_ThreadsInit");
  }

  z_unique(z_Epoch) epoch;
  ret = z_EpochInit(&epoch, 32, &ts);
  if (ret != z_OK) {
    z_panic("z_EpochInit");
  }

  z_unique(z_KV) kv;
  ret = z_KVInit(&kv, binlog_path, 1024 * 1024 * 1024, 1024);
  if (ret != z_OK) {
    z_panic("z_KVInit");
  }

  z_Svr svr;
  ret = z_SvrRun(&svr, "127.0.0.1", 12301, &epoch, &kv, z_KVHandle);
  if (ret != z_OK) {
    z_panic("z_SvrRun");
  }
  return;
}
#endif