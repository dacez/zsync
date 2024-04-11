#ifndef z_KV_H
#define z_KV_H

#include "zbinlog/binlog.h"
#include "zhash/map.h"

typedef struct {
  z_BinLog BinLog;
  char BinLogPath[128];
  int64_t BinLogFileMaxSize;
  z_Map Map;
} z_KV;

z_Error z_recordToMap(void *attr, z_Record *r, int64_t offset) {
  z_Map *m = (z_Map *)attr;
  z_Error ret = z_OK;
  switch (r->OP) {
  case z_RECORD_OP_INSERT: {
    z_String k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapInsert(m, k, offset);
  }
  case z_RECORD_OP_DELETE: {
    z_String k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapDelete(m, k);
  }
  case z_RECORD_OP_UPDATE: {
    z_String k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    z_String v;
    ret = z_RecordValue(r, &v);
    if (ret != z_OK) {
      return ret;
    }
    return z_MapUpdate(m, k, v, offset, false);
  }

  case z_RECORD_OP_FORCE_UPDATE: {
    z_String k;
    ret = z_RecordKey(r, &k);
    if (ret != z_OK) {
      return ret;
    }
    z_String v;
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

bool z_kvrIsEqual(void *attr, z_MapCmpType type, z_String str, z_ListRecord r) {
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

  z_String k, v;
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
    isEqual = z_StringIsEqual(k, str);
    break;
  case Z_MAP_CMP_TYPE_VALUE:
    isEqual = z_StringIsEqual(v, str);
    break;
  }

  z_RecordFree(record);

  return isEqual;
}

#endif