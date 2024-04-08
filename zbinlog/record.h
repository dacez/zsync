#ifndef z_RECORD_H
#define z_RECORD_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "ztypes/types.h"
#include "zhash/hash.h"
#include "zutils/utils.h"

typedef struct {
  uint64_t Seq;
  
  uint64_t OP : 8;
  uint64_t Sum : 8;
  uint64_t Reverse : 48;

  uint64_t KeyLen : 16;
  uint64_t ValLen : 48;
} z_Record;

uint64_t z_RecordLen(z_Record *r) {
  return r->KeyLen + r->ValLen + sizeof(z_Record);
}

z_Error z_RecordKey(z_Record *r, z_String *key) {
  if (r->KeyLen == 0) {
    return z_ERR_INVALID_DATA;
  }

  key->Data = (uint8_t*)(r + sizeof(z_Record));
  key->Len = r->KeyLen;

  return z_OK;
}

z_Error z_RecordValue(z_Record *r, z_String *value) {
  if (r->KeyLen == 0) {
    return z_ERR_INVALID_DATA;
  }

  value->Data = (uint8_t*)(r + sizeof(z_Record)) + r->KeyLen;
  value->Len = r->ValLen;

  return z_OK;
}

z_Error z_RecordCheck(z_Record *r) {
  uint8_t sum = r->Sum;
  r->Sum = 0;
  uint8_t s = z_Checksum((uint8_t*)r, z_RecordLen(r));
  r->Sum = sum;
  
  if (s == sum) {
    return z_OK;
  }

  return z_ERR_INVALID_DATA;
}

void z_RecordSum(z_Record *r) {
  r->Sum = 0;
  uint8_t s = z_Checksum((uint8_t*)r, z_RecordLen(r));
  r->Sum = s;

  return;
}

z_Record *z_RecordNew(uint64_t seq, uint8_t op, z_String key, z_String val) {
  z_Record record = {.Seq = seq, .OP = op, .KeyLen = key.Len, .ValLen = val.Len};
  uint64_t len = z_RecordLen(&record);
  z_Record *ret_record = z_malloc(len);
  *ret_record = record;
  memcpy(ret_record+1, key.Data, key.Len);
  memcpy((uint8_t*)(ret_record+1) + key.Len, val.Data, val.Len);
  z_RecordSum(ret_record);

  return ret_record;
}

void z_RecordFree(z_Record *r) {
  free((uint8_t*)r);
}

#endif