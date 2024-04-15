#ifndef z_KV_H
#define z_KV_H

#include "zbinlog/binlog.h"
#include "zhash/map.h"

#define z_MAX_PATH_LENGTH 128

typedef struct {
  z_BinLog BinLog;
  char BinLogPath[z_MAX_PATH_LENGTH];
  int64_t BinLogFileMaxSize;
  z_Map Map;
  int64_t BucketsLen;
} z_KV;

z_Error z_recordToMap(void *attr, z_Record *r, int64_t offset) {
  z_Map *m = (z_Map *)attr;
  z_Error ret = z_OK;
  switch (r->OP) {
  case z_RECORD_OP_INSERT: {
    z_Buffer k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapInsert(m, k, offset);
  }
  case z_RECORD_OP_DELETE: {
    z_Buffer k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapDelete(m, k);
  }
  case z_RECORD_OP_UPDATE: {
    z_Buffer k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    z_Buffer v;
    ret = z_RecordValue(r, &v);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapUpdate(m, k, v, offset, false);
  }

  case z_RECORD_OP_FORCE_UPDATE: {
    z_Buffer k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    z_Buffer v;
    ret = z_RecordValue(r, &v);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapUpdate(m, k, v, offset, true);
  }

  default:
    z_error("invalid op %d", r->OP);
    return z_ERR_INVALID_DATA;
  }
  return z_OK;
}

bool z_kvrIsEqual(void *attr, z_MapCmpType type, z_Buffer str, z_ListRecord r) {
  if (attr == nullptr) {
    z_error("attr == nullptr");
    return false;
  }

  z_BinLogFileReader *rd = (z_BinLogFileReader *)attr;
  z_Error ret = z_BinLogFileReaderSet(rd, r.Offset);
  if (ret != z_OK) {
    return false;
  }

  z_Record *record;
  ret = z_BinLogFileReaderGetRecord(rd, &record);
  if (ret != z_OK) {
    return false;
  }

  z_Buffer k, v;
  ret = z_RecordKey(record, &k);
  if (ret != z_OK) {
    z_RecordFree(record);
    return false;
  }

  ret = z_RecordValue(record, &v);
  if (ret != z_OK) {
    z_RecordFree(record);
    return false;
  }

  bool isEqual = false;
  switch (type) {
  case Z_MAP_CMP_TYPE_KEY:
    isEqual = z_BufferIsEqual(k, str);
    break;
  case Z_MAP_CMP_TYPE_VALUE:
    isEqual = z_BufferIsEqual(v, str);
    break;
  }

  z_RecordFree(record);

  return isEqual;
}

z_Error z_KVInit(z_KV *kv, char *path, int64_t binlog_file_max_size, int64_t buckets_len) {
  if (kv == nullptr || strlen(path) >= z_MAX_PATH_LENGTH || binlog_file_max_size == 0 || buckets_len == 0) {
    z_error("kv == nullptr || strlen(path) >= z_MAX_PATH_LENGTH || binlog_file_max_size == 0 || buckets_len == 0");
    return z_ERR_INVALID_DATA;
  }
  strncpy(kv->BinLogPath, path, z_MAX_PATH_LENGTH);
  kv->BinLogFileMaxSize = binlog_file_max_size;
  kv->BucketsLen = buckets_len;
  z_Error ret = z_BinLogInit(&kv->BinLog, kv->BinLogPath, kv->BinLogFileMaxSize, &kv->Map, z_recordToMap);
  if (ret != z_OK) {
    z_error("z_BinLogInit %d", ret);
    return ret;
  }
  ret = z_MapInit(&kv->Map, kv->BucketsLen, &kv->BinLog, z_kvrIsEqual);
  if (ret != z_OK) {
    z_error("z_MapInit %d", ret);
    return ret;
  }

  return z_OK;
}

z_Error z_KVInsert(z_KV *kv, z_Buffer k, z_Buffer v) {
  z_Record *r = z_RecordNew(z_RECORD_OP_INSERT, k, v);
  z_Error ret = z_BinLogAppendRecord(&kv->BinLog, r);
  z_RecordFree(r);

    if (ret != z_OK) {
    z_error("z_BinLogAppendRecord %d", ret);
    return ret;
  }

  return ret;
}

z_Error z_KVUpdate(z_KV *kv, z_Buffer k, z_Buffer v, bool is_force) {
  z_Record *r = z_RecordNew(is_force ? z_RECORD_OP_FORCE_UPDATE : z_RECORD_OP_UPDATE, k, v);
  z_Error ret = z_BinLogAppendRecord(&kv->BinLog, r);
  z_RecordFree(r);

  if (ret != z_OK) {
    z_error("z_BinLogAppendRecord %d", ret);
    return ret;
  }

  return ret;
}

z_Error z_KVFind(z_KV *kv, z_Buffer k, z_Buffer *v) {
  int64_t offset = 0;
  z_Error ret = z_MapFind(&kv->Map, k, &offset);
  if (ret != z_OK) {
    z_error("z_MapFind %d", ret);
    return ret;
  }

  if (ret != z_OK) {
    z_error("z_BinLogAppendRecord %d", ret);
    return ret;
  }

  z_BinLogFileReader rd;
  ret = z_BinLogFileReaderInit(&rd, kv->BinLogPath);
  if (ret != z_OK) {
    z_error("z_BinLogFileReaderInit %d", ret);
    return ret;    
  }

  z_Record *r;
  ret = z_BinLogFileReaderGetRecord(&rd, &r);
  if (ret != z_OK) {
    z_error("z_BinLogFileReaderGetRecord %d", ret);
    z_BinLogFileReaderDestory(&rd);
    return ret;
  }

  z_Buffer vv;
  ret = z_RecordValue(r, &vv);
  if (ret != z_OK) {
    z_error("z_RecordValue %d", ret);
    z_RecordFree(r);
    z_BinLogFileReaderDestory(&rd);
    return ret;
  }

  ret = z_BufferResetByBuffer(v,vv);

  z_RecordFree(r);
  z_BinLogFileReaderDestory(&rd);
  
  return ret;
}

z_Error z_KVDelete(z_KV *kv, z_Buffer k) {
  z_Buffer v;
  z_Record *r = z_RecordNew(z_RECORD_OP_DELETE, k, v);
  z_Error ret = z_BinLogAppendRecord(&kv->BinLog, r);
  z_RecordFree(r);

  if (ret != z_OK) {
    z_error("z_BinLogAppendRecord %d", ret);
    return ret;
  }

  return ret;
}
#endif