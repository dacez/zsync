#ifndef z_SVR_KV_H
#define z_SVR_KV_H
#include <stdint.h>

#include "zbinlog/file.h"
#include "zbinlog/file_record.h"
#include "zerror/error.h"
#include "zkv/kv.h"
#include "znet/kv_proto.h"
#include "znet/proto.h"
#include "znet/svr.h"
#include "zrecord/record.h"
#include "zutils/assert.h"
#include "zutils/buffer.h"
#include "zutils/log.h"

z_Error z_KVHandleSet(void *arg, const z_Req *req, z_Resp *resp) {
  z_assert(arg != nullptr, req != nullptr, resp != nullptr,
           req->Data != nullptr);

  if (req->Header.Type != z_KV_REQ_TYPE_SET) {
    z_error("invalid type %u", req->Header.Type);
    return z_ERR_INVALID_DATA;
  }

  z_KV *kv = (z_KV *)arg;
  z_Error ret = z_KVFromRecord(kv, (z_Record *)req->Data);
  if (ret != z_OK) {
    z_debug("z_KVFromRecord %d", ret);
  }
  return ret;
}

z_Error z_KVHandleGet(void *arg, const z_Req *req, z_Resp *resp) {
  z_assert(arg != nullptr, req != nullptr, resp != nullptr,
           req->Data != nullptr);

  if (req->Header.Type != z_KV_REQ_TYPE_GET) {
    z_error("invalid type %u", req->Header.Type);
    return z_ERR_INVALID_DATA;
  }

  z_KV *kv = (z_KV *)arg;
  z_ConstBuffer key = {};
  z_Error ret = z_RecordKey((z_Record *)req->Data, &key);
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
  z_Record *record = z_RecordNewByKV(0, empty, v);
  if (record == nullptr) {
    z_error("record == nullptr");
    return z_ERR_NOSPACE;
  }

  resp->Data = (void *)record;
  resp->Header.Size = z_RecordSize(record);
  return z_OK;
}

z_Error z_KVHandleBinLogGet(void *arg, const z_Req *req, z_Resp *resp) {
  z_assert(arg != nullptr, req != nullptr, resp != nullptr,
           req->Data != nullptr);

  if (req->Header.Type != z_KV_REQ_TYPE_BINLOG_GET) {
    z_error("invalid type %u", req->Header.Type);
    return z_ERR_INVALID_DATA;
  }

  z_KV *kv = (z_KV *)arg;
  z_unique(z_Reader) rd = {};
  z_Error ret = z_ReaderInit(&rd, kv->BinLogPath);
  if (ret != z_OK) {
    z_error("z_ReaderInit %d", ret);
    return ret;
  }

  z_BinlogGetReq *binlogGetReq = (z_BinlogGetReq*)(req->Data); 
  int64_t len = binlogGetReq->Len;
  int64_t minSeq = binlogGetReq->MinSeq;
  while (len > 0) {
    z_FileRecord fr;
    ret = z_ReaderGetRecord(&rd, &fr);
    if (ret != z_OK) {
      z_error("z_ReaderGetRecord %d", ret);
      break;
    }

    if (fr.Seq >= minSeq) {
      --len;
    }
  }


  return z_OK;
}

typedef struct {
  z_KV KV;
  z_Handles HS;
  z_Svr Svr;
} z_SvrKV;

z_Error z_SvrKVInit(z_SvrKV *svr, const char *binlog_path,
                    int64_t binlog_max_size, int64_t buckets_len,
                    const char *ip, int64_t port, int64_t thread_count) {
  z_Error ret = z_KVInit(&svr->KV, binlog_path, binlog_max_size, buckets_len);
  if (ret != z_OK) {
    z_error("z_KVInit %d", ret);
    return ret;
  }

  ret = z_HandlesInit(&svr->HS);
  if (ret != z_OK) {
    z_error("z_HandlesInit %d", ret);
    return ret;
  }

  ret = z_HandlesAdd(&svr->HS, z_KV_REQ_TYPE_SET, z_KVHandleSet);
  if (ret != z_OK) {
    z_error("z_HandlesAdd %d", ret);
    return ret;
  }
  ret = z_HandlesAdd(&svr->HS, z_KV_REQ_TYPE_GET, z_KVHandleGet);
  if (ret != z_OK) {
    z_error("z_HandlesAdd %d", ret);
    return ret;
  }
  ret = z_HandlesAdd(&svr->HS, z_KV_REQ_TYPE_BINLOG_GET, z_KVHandleBinLogGet);
  if (ret != z_OK) {
    z_error("z_HandlesAdd %d", ret);
    return ret;
  }

  ret = z_SvrInit(&svr->Svr, ip, port, thread_count, &svr->KV, &svr->HS);
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

void z_SvrKVStop(z_SvrKV *svr) { z_SvrStop(&svr->Svr); }

void z_SvrKVDestroy(z_SvrKV *svr) {
  z_SvrDestroy(&svr->Svr);
  z_HandlesDestroy(&svr->HS);
  z_KVDestroy(&svr->KV);
}
#endif