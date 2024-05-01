#ifndef z_KV_H
#define z_KV_H

#include "zbinlog/binlog.h"
#include "zbinlog/record.h"
#include "zmap/map.h"

#define z_MAX_PATH_LENGTH 1024

typedef struct {
  z_BinLog BinLog;
  char BinLogPath[z_MAX_PATH_LENGTH];
  int64_t BinLogFileMaxSize;
  z_Map Map;
  int64_t BucketsLen;
} z_KV;

z_Error z_binLogAfterWrite(void *attr, z_FileRecord *r1, int64_t offset1,
                           z_FileRecord *r2, int64_t offset2) {
  if (attr == nullptr || r1 == nullptr) {
    z_error("attr == nullptr || r1 == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_Map *m = (z_Map *)attr;
  z_Error ret = z_OK;
  switch (r1->OP) {
  case z_FILE_RECORD_OP_INSERT: {
    if (r2 != nullptr) {
      z_error("r2 != nullptr");
      return z_ERR_INVALID_DATA;
    }

    z_Buffer k;
    ret = z_FileRecordKey(r1, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapInsert(m, k, offset1);
  }
  case z_FILE_RECORD_OP_DELETE: {
    if (r2 != nullptr) {
      z_error("r2 != nullptr");
      return z_ERR_INVALID_DATA;
    }

    z_Buffer k;
    ret = z_FileRecordKey(r1, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapDelete(m, k);
  }
  case z_FILE_RECORD_OP_UPDATE: {
    if (r2 == nullptr || r2->OP != z_FILE_RECORD_OP_UPDATE_SRC_VALUE) {
      z_error("r2 == nullptr || r2->OP != z_FILE_RECORD_OP_UPDATE_SRC_VALUE");
      return z_ERR_INVALID_DATA;
    }
    z_Buffer k;
    ret = z_FileRecordKey(r1, &k);
    if (ret != z_OK) {
      return ret;
    }
    z_Buffer src_v;
    ret = z_FileRecordKey(r2, &src_v);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapUpdate(m, k, offset1, src_v);
  }

  case z_FILE_RECORD_OP_UPDATE_SRC_VALUE: {
    z_error("invalid op %d", r1->OP);
    return z_ERR_INVALID_DATA;
  }

  case z_FILE_RECORD_OP_FORCE_UPDATE: {
    if (r2 != nullptr) {
      z_error("r2 != nullptr");
      return z_ERR_INVALID_DATA;
    }
    z_Buffer k;
    ret = z_FileRecordKey(r1, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapForceUpdate(m, k, offset1);
  }

  case z_FILE_RECORD_OP_FORCE_UPSERT: {
    if (r2 != nullptr) {
      z_error("r2 != nullptr");
      return z_ERR_INVALID_DATA;
    }
    z_Buffer k;
    ret = z_FileRecordKey(r1, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapForceUpsert(m, k, offset1);
  }

  default:
    z_error("invalid op %d", r1->OP);
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

  z_FileRecord *record;
  ret = z_ReaderGetRecord(&rd, &record);
  if (ret != z_OK) {
    return false;
  }

  z_Buffer k;
  ret = z_FileRecordKey(record, &k);
  if (ret != z_OK) {
    z_FileRecordFree(record);
    z_ReaderDestroy(&rd);
    return false;
  }

  z_Buffer v;
  ret = z_FileRecordValue(record, &v);
  if (ret != z_OK) {
    z_FileRecordFree(record);
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

  z_FileRecordFree(record);
  z_ReaderDestroy(&rd);

  return isEqual;
}

z_Error z_fileGetRecord(z_Reader *rd, z_FileRecord **r1, int64_t *offset1,
                        z_FileRecord **r2, int64_t *offset2) {
  if (rd == nullptr || r1 == nullptr || offset1 == nullptr || r2 == nullptr ||
      offset2 == nullptr) {
    z_error("rd == nullptr || r1 == nullptr || offset1 == nullptr || r2 == "
            "nullptr || offset2 == nullptr");
    return z_ERR_INVALID_DATA;
  }
  z_Error ret = z_ReaderOffset(rd, offset1);
  if (ret != z_OK) {
    return ret;
  }
  ret = z_ReaderGetRecord(rd, r1);
  if (ret != z_OK) {
    return ret;
  }

  if ((*r1)->OP == z_FILE_RECORD_OP_UPDATE) {
    ret = z_ReaderOffset(rd, offset2);
    if (ret != z_OK) {
      return ret;
    }

    ret = z_ReaderGetRecord(rd, r2);
    if (ret != z_OK) {
      return ret;
    }
  }

  return z_OK;
}

z_Error z_mapInitFromFile(z_Map *m, char *path, int64_t *last_offset) {
  if (m == nullptr || path == nullptr) {
    z_error("m == nullptr || path == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_Reader rd;
  z_Error ret = z_ReaderInit(&rd, path);
  if (ret != z_OK) {
    return ret;
  }

  while (1) {
    z_FileRecord *r1 = nullptr;
    z_FileRecord *r2 = nullptr;
    int64_t offset1 = 0;
    int64_t offset2 = 0;
    ret = z_fileGetRecord(&rd, &r1, &offset1, &r2, &offset2);
    if (ret != z_OK) {
      z_FileRecordFree(r1);
      z_FileRecordFree(r2);
      break;
    }

    ret = z_binLogAfterWrite((void *)m, r1, offset1, r2, offset2);
    if (ret != z_OK && ret != z_ERR_EXIST && ret != z_ERR_NOT_FOUND &&
        ret != z_ERR_CONFLICT) {
      z_error("z_binLogAfterWrite %d", ret);
      z_FileRecordFree(r1);
      z_FileRecordFree(r2);
      break;
    }

    z_FileRecordFree(r1);
    z_FileRecordFree(r2);
  }

  ret = z_ReaderOffset(&rd, last_offset);
  z_ReaderDestroy(&rd);
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
  z_Error ret = z_BinLogInit(&kv->BinLog, kv->BinLogPath, kv->BinLogFileMaxSize,
                             &kv->Map, z_binLogAfterWrite);
  if (ret != z_OK) {
    return ret;
  }

  ret = z_MapInit(&kv->Map, kv->BucketsLen, kv->BinLogPath, z_mapIsEqual);
  if (ret != z_OK) {
    z_BinLogDestroy(&kv->BinLog);
    return ret;
  }

  if (z_BinLogIsEmpty(&kv->BinLog) == true) {
    return z_OK;
  }

  int64_t reader_offset = 0;
  ret = z_mapInitFromFile(&kv->Map, kv->BinLogPath, &reader_offset);
  if (ret != z_OK) {
    z_BinLogDestroy(&kv->BinLog);
    z_MapDestroy(&kv->Map);
    return ret;
  }

  int64_t writer_offset = 0;
  ret = z_WriterOffset(&kv->BinLog.Writer, &writer_offset);
  if (ret != z_OK) {
    z_BinLogDestroy(&kv->BinLog);
    z_MapDestroy(&kv->Map);
    return ret;
  }

  if (writer_offset != reader_offset) {
    z_error("writer_offset(%lld) != reader_offset(%lld)", writer_offset,
            reader_offset);
    return z_ERR_INVALID_DATA;
  }

  return z_OK;
}

z_Error z_KVInsert(z_KV *kv, z_Buffer k, z_Buffer v) {
  if (kv == nullptr) {
    z_error("kv == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_FileRecord *r = z_FileRecordNew(z_FILE_RECORD_OP_INSERT, k, v);
  if (r == nullptr) {
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_BinLogAppendRecord(&kv->BinLog, r);
  z_FileRecordFree(r);

  if (ret != z_OK) {
    return ret;
  }

  return ret;
}

z_Error z_KVForceUpdate(z_KV *kv, z_Buffer k, z_Buffer v) {
  if (kv == nullptr) {
    z_error("kv == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_FileRecord *r = z_FileRecordNew(z_FILE_RECORD_OP_FORCE_UPDATE, k, v);
  if (r == nullptr) {
    z_error("z_FileRecordNew == nullptr");
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_BinLogAppendRecord(&kv->BinLog, r);
  z_FileRecordFree(r);

  if (ret != z_OK) {
    return ret;
  }

  return ret;
}

z_Error z_KVForceUpsert(z_KV *kv, z_Buffer k, z_Buffer v) {
  if (kv == nullptr) {
    z_error("kv == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_FileRecord *r = z_FileRecordNew(z_FILE_RECORD_OP_FORCE_UPDATE, k, v);
  if (r == nullptr) {
    z_error("z_FileRecordNew == nullptr");
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_BinLogAppendRecord(&kv->BinLog, r);
  z_FileRecordFree(r);

  if (ret != z_OK) {
    return ret;
  }

  return ret;
}

z_Error z_KVUpdate(z_KV *kv, z_Buffer k, z_Buffer v, z_Buffer src_v) {
  if (kv == nullptr) {
    z_error("kv == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_FileRecord *r1 = z_FileRecordNew(z_FILE_RECORD_OP_UPDATE, k, v);
  if (r1 == nullptr) {
    z_error("z_FileRecordNew == nullptr");
    return z_ERR_NOSPACE;
  }

  z_Buffer vv = z_BufferEmpty();
  z_FileRecord *r2 = z_FileRecordNew(z_FILE_RECORD_OP_UPDATE_SRC_VALUE, src_v, vv);
  if (r2 == nullptr) {
    z_error("z_FileRecordNew == nullptr");
    z_FileRecordFree(r1);
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_BinLogAppendDoubleRecord(&kv->BinLog, r1, r2);

  z_FileRecordFree(r1);
  z_FileRecordFree(r2);

  return ret;
}

z_Error z_KVFind(z_KV *kv, z_Buffer k, z_Buffer *v) {
  if (kv == nullptr || v == nullptr) {
    z_error("kv == nullptr || v == nullptr");
    return z_ERR_INVALID_DATA;
  }

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

  z_FileRecord *r = nullptr;
  ret = z_ReaderGetRecord(&rd, &r);
  if (ret != z_OK) {
    z_ReaderDestroy(&rd);
    return ret;
  }

  z_Buffer vv;
  ret = z_FileRecordValue(r, &vv);
  if (ret != z_OK) {
    z_error("z_FileRecordValue %d", ret);
    z_FileRecordFree(r);
    z_ReaderDestroy(&rd);
    return ret;
  }

  ret = z_BufferResetByBuffer(v, vv);

  z_FileRecordFree(r);
  z_ReaderDestroy(&rd);

  return ret;
}

z_Error z_KVDelete(z_KV *kv, z_Buffer k) {
  if (kv == nullptr) {
    z_error("kv == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_Buffer v = {};
  z_FileRecord *r = z_FileRecordNew(z_FILE_RECORD_OP_DELETE, k, v);
  if (r == nullptr) {
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_BinLogAppendRecord(&kv->BinLog, r);
  z_FileRecordFree(r);

  return ret;
}

z_Error z_KVMerge(char *src_path, char *des_path) { return z_OK; }
#endif