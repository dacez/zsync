#ifndef z_RECORD_H
#define z_RECORD_H
#include <stdint.h>
#include <string.h>

#include "zerror/error.h"
#include "zutils/assert.h"
#include "zutils/buffer.h"
#include "zutils/hash.h"
#include "zutils/log.h"
#include "zutils/mem.h"

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
  uint64_t KeySize : 16;
  uint64_t ValSize : 24;
} z_Record;

typedef struct {
  uint64_t OP : 8;
  uint64_t Sum : 8;
  uint64_t Reserved : 16;
  uint64_t Size : 32;
} z_UpdateRecord;

static_assert(sizeof(z_UpdateRecord) == sizeof(z_Record));

typedef struct {
  uint64_t KeySize : 16;
  uint64_t ValSize : 24;
  uint64_t SrcValSize : 24;
} z_UpdateRecordKVV;

bool z_IsUpdateRecord(z_Record *r) {
  if (r->OP == z_ROP_UPDATE) {
    return true;
  }

  return false;
}

int64_t z_UpdateRecordSize(z_UpdateRecord *r) {
  z_assert(r != nullptr);
  return ((z_UpdateRecord *)r)->Size + sizeof(z_UpdateRecord);
}

z_Error z_UpdateRecordKey(z_UpdateRecord *r, z_ConstBuffer *key) {
  z_assert(r != nullptr, r->Size != 0, key != nullptr);
  z_UpdateRecordKVV *kvv =
      (z_UpdateRecordKVV *)((int8_t *)r + sizeof(z_UpdateRecord));

  z_assert(kvv->KeySize != 0);
  key->Size = kvv->KeySize;
  key->Data = (int8_t *)r + sizeof(z_UpdateRecord) + sizeof(z_UpdateRecordKVV);
  return z_OK;
}

z_Error z_UpdateRecordValue(z_UpdateRecord *r, z_ConstBuffer *value) {
  z_assert(r != nullptr, r->Size != 0, value != nullptr);
  z_UpdateRecordKVV *kvv =
      (z_UpdateRecordKVV *)((int8_t *)r + sizeof(z_UpdateRecord));
  value->Size = kvv->ValSize;
  value->Data = (int8_t *)r + sizeof(z_UpdateRecord) +
                sizeof(z_UpdateRecordKVV) + kvv->KeySize;
  return z_OK;
}

z_Error z_UpdateRecordSrcValue(z_UpdateRecord *r, z_ConstBuffer *value) {
  z_assert(r != nullptr, r->Size != 0, value != nullptr);
  z_UpdateRecordKVV *kvv =
      (z_UpdateRecordKVV *)((int8_t *)r + sizeof(z_UpdateRecord));
  value->Size = kvv->SrcValSize;
  value->Data = (int8_t *)r + sizeof(z_UpdateRecord) +
                sizeof(z_UpdateRecordKVV) + kvv->KeySize + kvv->ValSize;
  return z_OK;
}

int64_t z_RecordSize(z_Record *r) {
  z_assert(r != nullptr);

  if (z_IsUpdateRecord(r) == true) {
    return z_UpdateRecordSize((z_UpdateRecord *)r);
  }
  return r->KeySize + r->ValSize + sizeof(z_Record);
}

z_Error z_RecordKey(z_Record *r, z_ConstBuffer *key) {
  z_assert(r != nullptr, key != nullptr);
  if (z_IsUpdateRecord(r)) {
    return z_UpdateRecordKey((z_UpdateRecord *)r, key);
  }

  z_assert(r->KeySize != 0);
  key->Data = (int8_t *)(r + 1);
  key->Size = r->KeySize;

  return z_OK;
}

z_Error z_RecordValue(z_Record *r, z_ConstBuffer *val) {
  z_assert(r != nullptr, val != nullptr);
  if (z_IsUpdateRecord(r)) {
    return z_UpdateRecordValue((z_UpdateRecord *)r, val);
  }

  val->Data = (int8_t *)(r + 1) + r->KeySize;
  val->Size = r->ValSize;
  return z_OK;
}

z_Error z_RecordSrcValue(z_Record *r, z_ConstBuffer *src_val) {
  z_assert(r != nullptr, r->OP == z_ROP_UPDATE);
  return z_UpdateRecordSrcValue((z_UpdateRecord *)r, src_val);
}

z_Error z_RecordCheck(z_Record *r) {
  z_assert(r != nullptr);

  uint8_t sum = r->Sum;
  r->Sum = 0;
  uint8_t s = z_Checksum((int8_t *)r, z_RecordSize(r));
  r->Sum = sum;

  if (s == sum) {
    return z_OK;
  }

  return z_ERR_INVALID_DATA;
}

void z_RecordSum(z_Record *r) {
  z_assert(r != nullptr);

  r->Sum = 0;
  uint8_t s = z_Checksum((int8_t *)r, z_RecordSize(r));
  r->Sum = s;

  return;
}

z_Record *z_RecordNewBySize(int64_t size) {
  if (size < sizeof(z_Record)) {
    z_error("size %lld record_size %zu", size, sizeof(z_Record));
    return nullptr;
  }

  z_Record *r = z_malloc(size);
  if (r == nullptr) {
    z_error("r == nullptr");
  }
  return r;
}

z_Record *z_RecordNewByKV(uint8_t op, z_ConstBuffer key, z_ConstBuffer val) {
  z_Record record = {.OP = op, .KeySize = key.Size, .ValSize = val.Size};
  int64_t size = z_RecordSize(&record);
  z_Record *ret_record = z_RecordNewBySize(size);
  if (ret_record == nullptr) {
    z_error("ret_record == nullptr");
    return nullptr;
  }

  *ret_record = record;
  if (key.Size > 0) {
    memcpy(ret_record + 1, key.Data, key.Size);
  }
  if (val.Size > 0) {
    memcpy((int8_t *)(ret_record + 1) + key.Size, val.Data, val.Size);
  }

  return ret_record;
}

z_Record *z_RecordNewByKVV(uint8_t op, z_ConstBuffer key, z_ConstBuffer val,
                           z_ConstBuffer src_val) {
  if (op != z_ROP_UPDATE) {
    z_error("op != z_ROP_UPDATE");
    return nullptr;
  }
  z_UpdateRecord record = {.OP = op,
                           .Size = key.Size + val.Size + src_val.Size +
                                   sizeof(z_UpdateRecordKVV)};
  int64_t size = z_RecordSize((z_Record *)&record);
  z_UpdateRecord *ret_record = (z_UpdateRecord *)z_RecordNewBySize(size);
  if (ret_record == nullptr) {
    z_error("ret_record == nullptr");
    return nullptr;
  }
  z_UpdateRecordKVV *kvv = (z_UpdateRecordKVV *)ret_record + 1;
  kvv->KeySize = key.Size;
  kvv->ValSize = val.Size;
  kvv->SrcValSize = src_val.Size;

  *ret_record = record;
  if (key.Size > 0) {
    memcpy((int8_t *)ret_record + sizeof(z_Record) + sizeof(z_UpdateRecordKVV),
           key.Data, key.Size);
  }
  if (val.Size > 0) {
    memcpy((int8_t *)ret_record + sizeof(z_Record) + sizeof(z_UpdateRecordKVV) +
               kvv->KeySize,
           val.Data, val.Size);
  }
  if (src_val.Size > 0) {
    memcpy((int8_t *)ret_record + sizeof(z_Record) + sizeof(z_UpdateRecordKVV) +
               kvv->KeySize + kvv->ValSize,
           src_val.Data, src_val.Size);
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
  char ks[32] = {};
  char vs[32] = {};

  z_BufferStr(k, ks);
  z_BufferStr(v, vs);
  z_debug("OP %u Sum %u KeySize %u ValSize %u key %s val %s", r->OP, r->Sum,
          r->KeySize, r->ValSize, ks, vs);
}

#define z_RecordTo(type, fd, record)                                           \
  ({                                                                           \
    z_Error ret = z_OK;                                                        \
    ret = z_cat(type, Write)(fd, (int8_t *)record, z_RecordSize(record));      \
    if (ret != z_OK) {                                                         \
      z_error(#type "Write");                                                  \
    }                                                                          \
    ret;                                                                       \
  })

#define z_RecordFrom(type, fd, record)                                         \
  ({                                                                           \
    z_Error ret = z_OK;                                                        \
    z_Record *ret_record = nullptr;                                            \
    do {                                                                       \
      z_Record head;                                                           \
      z_Error ret = z_cat(type, Read)(fd, (int8_t *)&head, sizeof(z_Record));  \
      if (ret != z_OK) {                                                       \
        z_error(#type "Read");                                                 \
        break;                                                                 \
      }                                                                        \
      int64_t size = z_RecordSize(&head);                                      \
      ret_record = z_RecordNewBySize(size);                                    \
      *ret_record = head;                                                      \
      ret = z_cat(type, Read)(fd, (int8_t *)ret_record + sizeof(z_Record),     \
                              size - sizeof(z_Record));                        \
      if (ret != z_OK) {                                                       \
        z_error(#type "Read");                                                 \
        break;                                                                 \
      }                                                                        \
      ret = z_RecordCheck(ret_record);                                         \
      if (ret != z_OK) {                                                       \
        z_error("z_RecordCheck");                                              \
        break;                                                                 \
      }                                                                        \
    } while (0);                                                               \
    if (ret != z_OK) {                                                         \
      z_free(ret_record);                                                      \
    }                                                                          \
    *record = ret_record;                                                      \
    ret;                                                                       \
  })

#endif