#ifndef z_KV_H
#define z_KV_H

#include "zbinlog/binlog.h"
#include "zbinlog/file.h"
#include "zbinlog/file_record.h"
#include "zerror/error.h"
#include "zmap/map.h"
#include "zrecord/record.h"
#include "zutils/assert.h"
#include "zutils/defer.h"
#include <stdint.h>

#define z_MAX_PATH_LENGTH 1024

typedef struct {
  z_BinLog BinLog;
  char BinLogPath[z_MAX_PATH_LENGTH];
  int64_t BinLogFileMaxSize;
  z_Map Map;
  int64_t BucketsLen;
} z_KV;

z_Error z_binLogAfterWrite(void *attr, z_Record *r, int64_t offset) {
  z_assert(attr != nullptr, r != nullptr);

  z_Map *m = (z_Map *)attr;
  z_Error ret = z_OK;
  switch (r->OP) {
  case z_ROP_INSERT: {
    z_Buffer k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapInsert(m, k, offset);
  }
  case z_ROP_DELETE: {
    z_Buffer k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapDelete(m, k);
  }
  case z_ROP_UPDATE: {
    z_Buffer k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    z_Buffer src_v;
    ret = z_RecordSrcValue(r, &src_v);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapUpdate(m, k, offset, src_v);
  }
  case z_ROP_FORCE_UPDATE: {
    z_Buffer k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapForceUpdate(m, k, offset);
  }
  case z_ROP_FORCE_UPSERT: {
    z_Buffer k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapForceUpsert(m, k, offset);
  }
  default:
    z_error("invalid op %d", r->OP);
    return z_ERR_INVALID_DATA;
  }
  return z_OK;
}

bool z_mapIsEqual(void *attr, z_Buffer key, z_Buffer value, int64_t offset) {
  if (attr == nullptr || z_BufferIsEmpty(&key) && z_BufferIsEmpty(&value)) {
    z_error(
        "attr == nullptr || z_BufferIsEmpty(&key) && z_BufferIsEmpty(&value)");
    return false;
  }

  char *path = (char *)attr;
  z_Reader rd;
  z_Error ret = z_ReaderInit(&rd, path);
  if (ret != z_OK) {
    return ret;
  }

  ret = z_ReaderSet(&rd, offset);
  if (ret != z_OK) {
    return false;
  }

  z_FileRecord fr;
  ret = z_ReaderGetRecord(&rd, &fr);
  if (ret != z_OK) {
    return false;
  }

  z_Buffer k;
  ret = z_RecordKey(fr.Record, &k);
  if (ret != z_OK) {
    z_RecordFree(fr.Record);
    z_ReaderDestroy(&rd);
    return false;
  }

  z_Buffer v;
  ret = z_RecordValue(fr.Record, &v);
  if (ret != z_OK) {
    z_RecordFree(fr.Record);
    z_ReaderDestroy(&rd);
    return false;
  }

  bool isEqual = false;

  if (z_BufferIsEmpty(&key) == false && z_BufferIsEmpty(&value) == true) {
    isEqual = z_BufferIsEqual(k, key);
  } else if (z_BufferIsEmpty(&key) == true &&
             z_BufferIsEmpty(&value) == false) {
    isEqual = z_BufferIsEqual(v, value);
  } else if (z_BufferIsEmpty(&key) == false &&
             z_BufferIsEmpty(&value) == false) {
    isEqual = z_BufferIsEqual(k, key) && z_BufferIsEqual(v, value);
  }

  z_RecordFree(fr.Record);
  z_ReaderDestroy(&rd);

  return isEqual;
}

z_Error z_fileGetRecord(z_Reader *rd, z_Record **r, int64_t *offset) {
  z_assert(rd != nullptr, r != nullptr, offset != nullptr);

  z_Error ret = z_ReaderOffset(rd, offset);
  if (ret != z_OK) {
    return ret;
  }

  z_FileRecord fr;
  ret = z_ReaderGetRecord(rd, &fr);
  if (ret != z_OK) {
    return ret;
  }

  *r = fr.Record;
  return z_OK;
}

z_Error z_mapInitFromFile(z_Map *m, char *path, int64_t *last_offset) {
  if (m == nullptr || path == nullptr) {
    z_error("m == nullptr || path == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_unique(z_Reader) rd;
  z_Error ret = z_ReaderInit(&rd, path);
  if (ret != z_OK) {
    return ret;
  }

  int64_t max_offset = 0;
  ret = z_ReaderMaxOffset(&rd, &max_offset);
  if (ret != z_OK) {
    return ret;
  }

  *last_offset = 0;
  while (*last_offset < max_offset) {
    z_Record *r = nullptr;
    z_defer(z_RecordFree, r);
    int64_t offset = 0;

    ret = z_fileGetRecord(&rd, &r, &offset);
    if (ret != z_OK) {
      break;
    }

    ret = z_binLogAfterWrite((void *)m, r, offset);
    if (ret != z_OK && ret != z_ERR_EXIST && ret != z_ERR_NOT_FOUND &&
        ret != z_ERR_CONFLICT) {
      z_error("z_binLogAfterWrite %d", ret);
      break;
    }

    ret = z_ReaderOffset(&rd, last_offset);
    if (ret != z_OK) {
      break;
    }
  }

  return ret;
}

void z_KVDestroy(z_KV *kv) {
  if (kv == nullptr) {
    z_debug("kv == nullptr");
    return;
  }

  z_MapDestroy(&kv->Map);
  z_BinLogDestroy(&kv->BinLog);

  return;
}

z_Error z_KVInit(z_KV *kv, char *path, int64_t binlog_file_max_size,
                 int64_t buckets_len) {
  if (kv == nullptr || strlen(path) >= z_MAX_PATH_LENGTH ||
      binlog_file_max_size == 0 || buckets_len == 0) {
    z_error("kv == nullptr || strlen(path) >= z_MAX_PATH_LENGTH || "
            "binlog_file_max_size == 0 || buckets_len == 0");
    return z_ERR_INVALID_DATA;
  }
  memset(kv->BinLogPath, 0, z_MAX_PATH_LENGTH);
  strncpy(kv->BinLogPath, path, z_MAX_PATH_LENGTH - 1);
  kv->BinLogFileMaxSize = binlog_file_max_size;
  kv->BucketsLen = buckets_len;
  int64_t wr_offset = {};
  z_Error ret = z_BinLogInit(&kv->BinLog, kv->BinLogPath, kv->BinLogFileMaxSize,
                             &kv->Map, z_binLogAfterWrite);
  if (ret != z_OK) {
    return ret;
  }

  ret = z_WriterOffset(&kv->BinLog.Writer, &wr_offset);
  if (ret != z_OK) {
    z_BinLogDestroy(&kv->BinLog);
    return ret;
  }

  ret = z_MapInit(&kv->Map, kv->BucketsLen, kv->BinLogPath, z_mapIsEqual);
  if (ret != z_OK) {
    z_BinLogDestroy(&kv->BinLog);
    return ret;
  }

  if (wr_offset == 0) {
    return z_OK;
  }

  int64_t rd_offset = 0;
  ret = z_mapInitFromFile(&kv->Map, kv->BinLogPath, &rd_offset);
  if (ret != z_OK) {
    z_BinLogDestroy(&kv->BinLog);
    z_MapDestroy(&kv->Map);
    return ret;
  }

  if (wr_offset != rd_offset) {
    z_error("writer_offset(%lld) != reader_offset(%lld)", wr_offset, rd_offset);
    return z_ERR_INVALID_DATA;
  }

  return z_OK;
}

z_Error z_KVFromRecord(z_KV *kv, z_Record *r) {
  z_assert(kv != nullptr, r != nullptr);

  z_Error ret = z_OK;
  z_FileRecord fr = {.Record = r};
  ret = z_BinLogAppendRecord(&kv->BinLog, &fr);
  if (ret != z_OK) {
    return ret;
  }
  return z_OK;
}

z_Error z_KVInsert(z_KV *kv, z_Buffer k, z_Buffer v) {
  z_assert(kv != nullptr, k.Len != 0, k.Data != nullptr);
  z_assert(v.Len != 0, v.Data != nullptr);

  z_Record *r = z_RecordNewByKV(z_ROP_INSERT, k, v);
  if (r == nullptr) {
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_KVFromRecord(kv, r);

  z_RecordFree(r);
  return ret;
}

z_Error z_KVForceUpdate(z_KV *kv, z_Buffer k, z_Buffer v) {
  z_assert(kv != nullptr, k.Len != 0, k.Data != nullptr);
  z_assert(v.Len != 0, v.Data != nullptr);

  z_Record *r = z_RecordNewByKV(z_ROP_FORCE_UPDATE, k, v);
  if (r == nullptr) {
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_KVFromRecord(kv, r);

  z_RecordFree(r);
  return ret;
}

z_Error z_KVForceUpsert(z_KV *kv, z_Buffer k, z_Buffer v) {
  z_assert(kv != nullptr, k.Len != 0, k.Data != nullptr);
  z_assert(v.Len != 0, v.Data != nullptr);

  z_Record *r = z_RecordNewByKV(z_ROP_FORCE_UPDATE, k, v);
  if (r == nullptr) {
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_KVFromRecord(kv, r);

  z_RecordFree(r);
  return ret;
}

z_Error z_KVUpdate(z_KV *kv, z_Buffer k, z_Buffer v, z_Buffer src_v) {
  z_assert(kv != nullptr, k.Len != 0, k.Data != nullptr);
  z_assert(v.Len != 0, v.Data != nullptr);
  z_assert(src_v.Len != 0, src_v.Data != nullptr);

  z_Record *r = z_RecordNewByKVV(z_ROP_UPDATE, k, v, src_v);
  if (r == nullptr) {
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_KVFromRecord(kv, r);

  z_RecordFree(r);
  return ret;
}

z_Error z_KVFind(z_KV *kv, z_Buffer k, z_Buffer *v) {
  z_assert(kv != nullptr, k.Len != 0, k.Data != nullptr);
  z_assert(v != nullptr);

  int64_t offset = 0;
  z_Error ret = z_MapFind(&kv->Map, k, &offset);
  if (ret != z_OK) {
    return ret;
  }

  z_Reader rd;
  ret = z_ReaderInit(&rd, kv->BinLogPath);
  if (ret != z_OK) {
    return ret;
  }

  ret = z_ReaderSet(&rd, offset);
  if (ret != z_OK) {
    z_ReaderDestroy(&rd);
    return ret;
  }

  z_FileRecord fr = {};
  ret = z_ReaderGetRecord(&rd, &fr);
  if (ret != z_OK) {
    z_ReaderDestroy(&rd);
    return ret;
  }

  z_Buffer vv;
  ret = z_RecordValue(fr.Record, &vv);
  if (ret != z_OK) {
    z_error("z_RecordValue %d", ret);
    z_RecordFree(fr.Record);
    z_ReaderDestroy(&rd);
    return ret;
  }

  ret = z_BufferResetByBuffer(v, vv);

  z_RecordFree(fr.Record);
  z_ReaderDestroy(&rd);
  return ret;
}

z_Error z_KVDelete(z_KV *kv, z_Buffer k) {
  z_assert(kv != nullptr, k.Len != 0, k.Data != nullptr);

  z_Buffer v = {};
  z_Record *r = z_RecordNewByKV(z_ROP_DELETE, k, v);
  if (r == nullptr) {
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_KVFromRecord(kv, r);
  z_RecordFree(r);
  return ret;
}
#endif