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
  z_RECORD_OP_UPDATE_SRC_VALUE = 4,
  z_RECORD_OP_FORCE_UPDATE = 5,
} z_FileRecordOP;

typedef struct {
  int64_t Seq;
  uint64_t OP : 8;
  uint64_t Sum : 8;
  uint64_t KeyLen : 16;
  uint64_t ValLen : 24;
} z_FileRecord;

int64_t z_FileRecordLen(z_FileRecord *r) {
  if (r == nullptr) {
    z_error("r == nullptr");
    return 0;
  }
  return r->KeyLen + r->ValLen + sizeof(z_FileRecord);
}

z_Error z_FileRecordKey(z_FileRecord *r, z_Buffer *key) {
  if (r == nullptr || r->KeyLen == 0 || key == nullptr) {
    z_error("r == nullptr || r->KeyLen == 0 || key == nullptr");
    return z_ERR_INVALID_DATA;
  }

  key->Data = (int8_t *)(r + 1);
  key->Len = r->KeyLen;

  return z_OK;
}

z_Error z_FileRecordValue(z_FileRecord *r, z_Buffer *value) {
  if (r == nullptr || r->KeyLen == 0 || value == nullptr) {
    z_error("r == nullptr || r->KeyLen == 0 || value == nullptr");
    return z_ERR_INVALID_DATA;
  }

  value->Data = (int8_t *)(r + 1) + r->KeyLen;
  value->Len = r->ValLen;

  return z_OK;
}

z_Error z_FileRecordCheck(z_FileRecord *r) {
  if (r == nullptr) {
    z_error("r == nullptr");
    return z_ERR_INVALID_DATA;
  }

  uint8_t sum = r->Sum;
  r->Sum = 0;
  uint8_t s = z_Checksum((int8_t *)r, z_FileRecordLen(r));
  r->Sum = sum;

  if (s == sum) {
    return z_OK;
  }

  return z_ERR_INVALID_DATA;
}

void z_FileRecordSum(z_FileRecord *r) {
  if (r == nullptr) {
    z_error("r == nullptr");
    return;
  }

  r->Sum = 0;
  uint8_t s = z_Checksum((int8_t *)r, z_FileRecordLen(r));
  r->Sum = s;

  return;
}

z_FileRecord *z_FileRecordNewByLen(int64_t len) {
  if (len < sizeof(z_FileRecord)) {
    z_error("len %lld record_size %zu", len, sizeof(z_FileRecord));
    return nullptr;
  }

  z_FileRecord *r = z_malloc(len);
  if (r == nullptr) {
    z_error("r == nullptr");
  }
  return r;
}

z_FileRecord *z_FileRecordNew(uint8_t op, z_Buffer key, z_Buffer val) {
  if (key.Data == nullptr || key.Len == 0) {
    z_error("key.Data == nullptr || key.Len == 0");
    return nullptr;
  }
  z_FileRecord record = {
      .OP = op, .KeyLen = key.Len, .ValLen = val.Len};
  int64_t len = z_FileRecordLen(&record);
  z_FileRecord *ret_record = z_FileRecordNewByLen(len);
  if (ret_record == nullptr) {
    z_error("ret_record == nullptr");
    return nullptr;
  }

  *ret_record = record;
  memcpy(ret_record + 1, key.Data, key.Len);
  memcpy((int8_t *)(ret_record + 1) + key.Len, val.Data, val.Len);

  return ret_record;
}

void z_FileRecordFree(z_FileRecord *r) {
  if (r == nullptr) {
    z_debug("r == nullptr");
    return;
  }

  z_free(r);
}

#endif