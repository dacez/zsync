#ifndef z_SVR_KV_H
#define z_SVR_KV_H
#include <stdint.h>

#include "zerror/error.h"
#include "zkv/kv.h"
#include "znet/svr.h"
#include "zrecord/record.h"
#include "zutils/assert.h"
#include "zutils/buffer.h"
#include "zutils/log.h"

z_Error z_KVHandle(void *attr, z_Record *req, z_Record **resp) {
  z_assert(attr != nullptr, resp != nullptr);

  if (req == nullptr) {
    z_error("req == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_KV *kv = (z_KV *)attr;
  if (req->OP != z_ROP_FIND) {
    return z_KVFromRecord(kv, req);
  }

  z_ConstBuffer key = {};
  z_Error ret = z_RecordKey(req, &key);
  if (ret != z_OK) {
    z_error("z_RecordKey failed %d", ret);
    return ret;
  }

  z_unique(z_Buffer) val = {};
  ret = z_KVFind(kv, key, &val);
  if (ret != z_OK) {
    z_debug("z_KVFind failed %d", ret);
    return ret;
  }

  z_ConstBuffer empty = {};
  z_ConstBuffer v = {.Data = val.Data, .Size = val.Size};
  *resp = z_RecordNewByKV(req->OP, empty, v);
  if (*resp == nullptr) {
    z_error("*resp == nullptr");
    return z_ERR_NOSPACE;
  }
 return z_OK;
}

typedef struct {
  z_KV KV;
  z_Svr Svr;
} z_SvrKV;

z_Error z_SvrKVInit(z_SvrKV *svr, const char *binlog_path, int64_t binlog_max_size, int64_t buckets_len, const char *ip, int64_t port, int64_t thread_count) {
  z_Error ret = z_KVInit(&svr->KV, binlog_path, binlog_max_size, buckets_len);
  if (ret != z_OK) {
    z_error("z_KVInit %d", ret);
    return ret;
  }

  ret = z_SvrInit(&svr->Svr, ip, port, thread_count, &svr->KV, z_KVHandle);
  if (ret != z_OK) {
    z_error("z_SvrInit %d", ret);
    return ret;
  }

  return ret;
}

z_Error z_SvrKVRun(z_SvrKV *svr) {
  z_Error ret = z_SvrRun(&svr->Svr);
  if (ret != z_OK) {
    z_error("z_SvrRun failed %d", ret);
    return ret;
  }
  return z_OK;
}

void z_SvrKVStop(z_SvrKV *svr) {
  z_SvrStop(&svr->Svr);
}

void z_SvrKVDestroy(z_SvrKV *svr) {
  z_KVDestroy(&svr->KV);
  z_SvrDestroy(&svr->Svr);
}
#endif