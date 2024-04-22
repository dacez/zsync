#ifndef z_RECORD_H
#define z_RECORD_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "zerror/error.h"
#include "zutils/utils.h"

#include "zhash/hash.h"

typedef enum {
  z_RECORD_OP_INSERT = 1,
  z_RECORD_OP_DELETE = 2,
  z_RECORD_OP_UPDATE = 3,
  z_RECORD_OP_FORCE_UPDATE = 4,
} z_BinlogRecordOP;

typedef struct {
  int64_t OP : 8;
  int64_t Sum : 8;
  uint64_t KeyLen : 16;
  uint64_t ValLen : 16;
  uint64_t NextLen : 16;
} z_BinlogRecord;

int64_t z_BinlogRecordLen(z_BinlogRecord *r) {
  if (r == nullptr) {
    z_error("r == nullptr");
    return 0;
  }
  return r->KeyLen + r->ValLen + r->NextLen + sizeof(z_BinlogRecord);
}

z_Error z_BinlogRecordKey(z_BinlogRecord *r, z_Buffer *key) {
  if (r == nullptr || r->KeyLen == 0 || key == nullptr) {
    z_error("r == nullptr || r->KeyLen == 0 || key == nullptr");
    return z_ERR_INVALID_DATA;
  }

  key->Data = (int8_t *)(r + 1);
  key->Len = r->KeyLen;

  return z_OK;
}

z_Error z_BinlogRecordValue(z_BinlogRecord *r, z_Buffer *value) {
  if (r == nullptr || r->KeyLen == 0 || value == nullptr) {
    z_error("r == nullptr || r->KeyLen == 0 || value == nullptr");
    return z_ERR_INVALID_DATA;
  }

  value->Data = (int8_t *)(r + 1) + r->KeyLen;
  value->Len = r->ValLen;

  return z_OK;
}

z_Error z_BinlogRecordNext(z_BinlogRecord *r, z_Buffer *value) {
  if (r == nullptr || r->KeyLen == 0 || value == nullptr) {
    z_error("r == nullptr || r->KeyLen == 0 || value == nullptr");
    return z_ERR_INVALID_DATA;
  }

  value->Data = (int8_t *)(r + 1) + r->KeyLen + r->ValLen;
  value->Len = r->NextLen;

  return z_OK;
}

z_Error z_BinlogRecordCheck(z_BinlogRecord *r) {
  if (r == nullptr) {
    z_error("r == nullptr");
    return z_ERR_INVALID_DATA;
  }

  int8_t sum = r->Sum;
  r->Sum = 0;
  int8_t s = (int8_t)z_Checksum((int8_t *)r, z_BinlogRecordLen(r));
  r->Sum = sum;

  if (s == sum) {
    return z_OK;
  }

  return z_ERR_INVALID_DATA;
}

void z_BinlogRecordSum(z_BinlogRecord *r) {
  if (r == nullptr) {
    z_error("r == nullptr");
    return;
  }

  r->Sum = 0;
  int8_t s = (int8_t)z_Checksum((int8_t *)r, z_BinlogRecordLen(r));
  r->Sum = s;

  return;
}

z_BinlogRecord *z_BinlogRecordNewByLen(int64_t len) {
  if (len < sizeof(z_BinlogRecord)) {
    z_error("len %lld record_size %zu", len, sizeof(z_BinlogRecord));
    return nullptr;
  }

  z_BinlogRecord *r = z_malloc(len);
  if (r == nullptr) {
    z_error("r == nullptr");
  }
  return r;
}

z_BinlogRecord *z_BinlogRecordNew(int8_t op, z_Buffer key, z_Buffer val) {
  if (key.Data == nullptr || key.Len == 0) {
    z_error("key.Data == nullptr || key.Len == 0");
    return nullptr;
  }
  z_BinlogRecord record = {
      .OP = op, .KeyLen = key.Len, .ValLen = val.Len, .NextLen = 0};
  int64_t len = z_BinlogRecordLen(&record);
  z_BinlogRecord *ret_record = z_BinlogRecordNewByLen(len);
  if (ret_record == nullptr) {
    z_error("ret_record == nullptr");
    return nullptr;
  }

  *ret_record = record;
  memcpy(ret_record + 1, key.Data, key.Len);
  memcpy((int8_t *)(ret_record + 1) + key.Len, val.Data, val.Len);
  z_BinlogRecordSum(ret_record);

  return ret_record;
}

z_BinlogRecord *z_BinlogRecordNewByNext(int8_t op, z_Buffer key, z_Buffer val,
                            z_Buffer next) {
  if (key.Data == nullptr || key.Len == 0) {
    z_error("key.Data == nullptr || key.Len == 0");
    return nullptr;
  }
  z_BinlogRecord record = {
      .OP = op, .KeyLen = key.Len, .ValLen = val.Len, .NextLen = next.Len};
  int64_t len = z_BinlogRecordLen(&record);
  z_BinlogRecord *ret_record = z_BinlogRecordNewByLen(len);
  if (ret_record == nullptr) {
    z_error("ret_record == nullptr");
    return nullptr;
  }

  *ret_record = record;
  memcpy(ret_record + 1, key.Data, key.Len);
  memcpy((int8_t *)(ret_record + 1) + key.Len, val.Data, val.Len);
  memcpy((int8_t *)(ret_record + 1) + key.Len + val.Len, next.Data, next.Len);
  z_BinlogRecordSum(ret_record);

  return ret_record;
}

void z_BinlogRecordFree(z_BinlogRecord *r) {
  if (r == nullptr) {
    z_debug("r == nullptr");
    return;
  }

  z_free(r);
}

#endif