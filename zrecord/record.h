#ifndef z_RECORD_H
#define z_RECORD_H
#include "zerror/error.h"
#include "zutils/assert.h"
#include "zutils/buffer.h"
#include "zutils/hash.h"
#include "zutils/log.h"
#include "zutils/mem.h"
#include <stdint.h>
#include <string.h>

typedef enum : uint8_t {
  z_ROP_INSERT = 1,
  z_ROP_DELETE = 2,
  z_ROP_UPDATE = 3,
  z_ROP_FORCE_UPDATE = 4,
  z_ROP_FORCE_UPSERT = 5,
  z_ROP_FIND = 6,
} z_RecordOP;

typedef struct {
  uint64_t OP : 8;
  uint64_t Sum : 8;
  uint64_t Reserved : 8;
  uint64_t KeyLen : 16;
  uint64_t ValLen : 24;
} z_Record;

typedef struct {
  uint64_t OP : 8;
  uint64_t Sum : 8;
  uint64_t Reserved : 16;
  uint64_t Len : 32;
} z_UpdateRecord;

static_assert(sizeof(z_UpdateRecord) == sizeof(z_Record));

typedef struct {
  uint64_t KeyLen : 16;
  uint64_t ValLen : 24;
  uint64_t SrcValLen : 24;
} z_UpdateRecordKVV;

bool z_IsUpdateRecord(z_Record *r) {
  if (r->OP == z_ROP_UPDATE) {
    return true;
  }

  return false;
}

int64_t z_UpdateRecordLen(z_UpdateRecord *r) {
  z_assert(r != nullptr);
  return ((z_UpdateRecord *)r)->Len + sizeof(z_UpdateRecord);
}

z_Error z_UpdateRecordKey(z_UpdateRecord *r, z_ConstBuffer *key) {
  z_assert(r != nullptr, r->Len != 0, key != nullptr);
  z_UpdateRecordKVV *kvv =
      (z_UpdateRecordKVV *)((int8_t *)r + sizeof(z_UpdateRecord));

  z_assert(kvv->KeyLen != 0);
  key->Len = kvv->KeyLen;
  key->Data = (int8_t *)r + sizeof(z_UpdateRecord) + sizeof(z_UpdateRecordKVV);
  return z_OK;
}

z_Error z_UpdateRecordValue(z_UpdateRecord *r, z_ConstBuffer *value) {
  z_assert(r != nullptr, r->Len != 0, value != nullptr);
  z_UpdateRecordKVV *kvv =
      (z_UpdateRecordKVV *)((int8_t *)r + sizeof(z_UpdateRecord));
  value->Len = kvv->ValLen;
  value->Data = (int8_t *)r + sizeof(z_UpdateRecord) +
                sizeof(z_UpdateRecordKVV) + kvv->KeyLen;
  return z_OK;
}

z_Error z_UpdateRecordSrcValue(z_UpdateRecord *r, z_ConstBuffer *value) {
  z_assert(r != nullptr, r->Len != 0, value != nullptr);
  z_UpdateRecordKVV *kvv =
      (z_UpdateRecordKVV *)((int8_t *)r + sizeof(z_UpdateRecord));
  value->Len = kvv->SrcValLen;
  value->Data = (int8_t *)r + sizeof(z_UpdateRecord) +
                sizeof(z_UpdateRecordKVV) + kvv->KeyLen + kvv->ValLen;
  return z_OK;
}

int64_t z_RecordLen(z_Record *r) {
  if (r == nullptr) {
    z_error("r == nullptr");
    return 0;
  }

  if (z_IsUpdateRecord(r) == true) {
    return z_UpdateRecordLen((z_UpdateRecord *)r);
  }
  return r->KeyLen + r->ValLen + sizeof(z_Record);
}

z_Error z_RecordKey(z_Record *r, z_ConstBuffer *key) {
  z_assert(r != nullptr, key != nullptr);
  if (z_IsUpdateRecord(r)) {
    return z_UpdateRecordKey((z_UpdateRecord *)r, key);
  }

  z_assert(r->KeyLen != 0);
  key->Data = (int8_t *)(r + 1);
  key->Len = r->KeyLen;

  return z_OK;
}

z_Error z_RecordValue(z_Record *r, z_ConstBuffer *val) {
  z_assert(r != nullptr, val != nullptr);
  if (z_IsUpdateRecord(r)) {
    return z_UpdateRecordValue((z_UpdateRecord *)r, val);
  }

  val->Data = (int8_t *)(r + 1) + r->KeyLen;
  val->Len = r->ValLen;
  return z_OK;
}

z_Error z_RecordSrcValue(z_Record *r, z_ConstBuffer *src_val) {
  z_assert(r != nullptr, r->OP == z_ROP_UPDATE);
  return z_UpdateRecordSrcValue((z_UpdateRecord *)r, src_val);
}

z_Error z_RecordCheck(z_Record *r) {
  if (r == nullptr) {
    z_error("r == nullptr");
    return z_ERR_INVALID_DATA;
  }

  uint8_t sum = r->Sum;
  r->Sum = 0;
  uint8_t s = z_Checksum((int8_t *)r, z_RecordLen(r));
  r->Sum = sum;

  if (s == sum) {
    return z_OK;
  }

  return z_ERR_INVALID_DATA;
}

void z_RecordSum(z_Record *r) {
  if (r == nullptr) {
    z_error("r == nullptr");
    return;
  }

  r->Sum = 0;
  uint8_t s = z_Checksum((int8_t *)r, z_RecordLen(r));
  r->Sum = s;

  return;
}

z_Record *z_RecordNewByLen(int64_t len) {
  if (len < sizeof(z_Record)) {
    z_error("len %lld record_size %zu", len, sizeof(z_Record));
    return nullptr;
  }

  z_Record *r = z_malloc(len);
  if (r == nullptr) {
    z_error("r == nullptr");
  }
  return r;
}

z_Record *z_RecordNewByKV(uint8_t op, z_ConstBuffer key, z_ConstBuffer val) {
  z_Record record = {.OP = op, .KeyLen = key.Len, .ValLen = val.Len};
  int64_t len = z_RecordLen(&record);
  z_Record *ret_record = z_RecordNewByLen(len);
  if (ret_record == nullptr) {
    z_error("ret_record == nullptr");
    return nullptr;
  }

  *ret_record = record;
  if (key.Len > 0) {
    memcpy(ret_record + 1, key.Data, key.Len);
  }
  if (val.Len > 0) {
    memcpy((int8_t *)(ret_record + 1) + key.Len, val.Data, val.Len);
  }

  return ret_record;
}

z_Record *z_RecordNewByKVV(uint8_t op, z_ConstBuffer key, z_ConstBuffer val,
                           z_ConstBuffer src_val) {
  z_assert(op == z_ROP_UPDATE);
  z_UpdateRecord record = {.OP = op,
                           .Len = key.Len + val.Len + src_val.Len +
                                  sizeof(z_UpdateRecordKVV)};
  int64_t len = z_RecordLen((z_Record *)&record);
  z_UpdateRecord *ret_record = (z_UpdateRecord *)z_RecordNewByLen(len);
  if (ret_record == nullptr) {
    z_error("ret_record == nullptr");
    return nullptr;
  }
  z_UpdateRecordKVV *kvv = (z_UpdateRecordKVV *)ret_record + 1;
  kvv->KeyLen = key.Len;
  kvv->ValLen = val.Len;
  kvv->SrcValLen = src_val.Len;

  *ret_record = record;
  if (key.Len > 0) {
    memcpy((int8_t *)ret_record + sizeof(z_Record) + sizeof(z_UpdateRecordKVV),
           key.Data, key.Len);
  }
  if (val.Len > 0) {
    memcpy((int8_t *)ret_record + sizeof(z_Record) + sizeof(z_UpdateRecordKVV) +
               kvv->KeyLen,
           val.Data, val.Len);
  }
  if (src_val.Len > 0) {
    memcpy((int8_t *)ret_record + sizeof(z_Record) + sizeof(z_UpdateRecordKVV) +
               kvv->KeyLen + kvv->ValLen,
           src_val.Data, src_val.Len);
  }

  return (z_Record *)ret_record;
}

void z_RecordFree(z_Record *r) {
  if (r == nullptr) {
    return;
  }

  z_free(r);
}

void z_RecordDebugString(z_Record *r) {
  z_ConstBuffer k, v;
  z_RecordKey(r, &k);
  z_RecordValue(r, &v);
  char ks[32];
  char vs[32];

  z_BufferStr(k, ks);
  z_BufferStr(v, vs);
  z_debug("OP %u Sum %u KeyLen %u ValLen %u key %s val %s", r->OP, r->Sum,
          r->KeyLen, r->ValLen, ks, vs);
}

#endif