#ifndef z_MAP_H
#define z_MAP_H

#include <stdint.h>

#include "zerror/error.h"
#include "zutils/buffer.h"
#include "zutils/lock.h"
#include "zhash/hash.h"

#define z_LIST_MAX_LEN 1024*16

typedef struct {
  int64_t Offset;
  int64_t Hash;
} z_MapRecord;

typedef bool z_MapIsEqual(void *attr, z_Buffer key, z_Buffer value, int64_t offset);

typedef struct {
  int16_t Pos;
  int16_t Len;
  z_MapRecord *Records;
} z_List;

void z_ListDestroy(z_List *l) {
  if (l == nullptr || l->Records == nullptr) {
    z_debug("l == nullptr || l->List == nullptr");
    return;
  }

  z_free(l->Records);
  l->Records = nullptr;
  l->Len = 0;
  l->Pos = 0;
}

z_Error z_ListInit(z_List *l) {
  if (l == nullptr) {
    z_error("l == nullptr");
    return z_ERR_INVALID_DATA;
  }

  l->Records = nullptr;
  l->Len = 0;
  l->Pos = 0;
  return z_OK;
}

z_Error z_ListGrow(z_List *l) {
  z_MapRecord *src_rs = l->Records;
  int16_t src_len = l->Len;

  int16_t len = src_len == 0 ? 2 : src_len * 2;
  if (len >= z_LIST_MAX_LEN) {
    z_error("len == %u max %u", len, z_LIST_MAX_LEN);
    return z_ERR_NOSPACE;
  }

  l->Records = z_malloc(sizeof(z_MapRecord) * len);
  if (l->Records == nullptr) {
    z_error("len == %d", len);
    l->Records = src_rs;
    return z_ERR_NOSPACE;
  }
  l->Len = len;
  memset(l->Records, 0, sizeof(z_MapRecord) * l->Len);

  if (src_rs != nullptr) {
    memcpy(l->Records, src_rs, sizeof(z_MapRecord) * src_len);
    z_free(src_rs);
  }

  return z_OK;
}

z_Error z_ListFind(z_List *l, z_Buffer k, z_MapRecord r, void *attr,
                   z_MapIsEqual *isEqual, z_MapRecord **record) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr || record == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr || record == nullptr");
    return z_ERR_INVALID_DATA;
  }

  for (int16_t i = 0; i < l->Pos; ++i) {
    if (l->Records[i].Hash == r.Hash) {
      if (l->Records[i].Offset == r.Offset) {
        z_debug("i %d hash %lld offset %lld", i, r.Hash, r.Offset);
        *record = &l->Records[i];
        return z_OK;
      } else {
        z_debug("i %d hash %lld arg_offset %lld list_offset %lld", i, r.Hash,
                r.Offset, l->Records[i].Offset);
        if (isEqual(attr, k, z_BufferEmpty(), l->Records[i].Offset) == true) {
          *record = &l->Records[i];
          return z_OK;
        }
      }
    }
  }

  *record = nullptr;
  return z_ERR_NOT_FOUND;
}

z_Error z_ListInsert(z_List *l, z_Buffer k, z_MapRecord r, void *attr,
                     z_MapIsEqual *isEqual) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_Error ret = z_OK;
  z_MapRecord *record;
  ret = z_ListFind(l, k, r, attr, isEqual, &record);
  if (ret == z_OK) {
    return z_ERR_EXIST;
  } else if (ret != z_ERR_NOT_FOUND) {
    return ret;
  }

  if (l->Pos >= l->Len) {
    ret = z_ListGrow(l);
    if (ret != z_OK) {
      return ret;
    }
    return z_ListInsert(l, k, r, attr, isEqual);
  }

  l->Records[l->Pos++] = r;
  return z_OK;
}


z_Error z_ListForceUpdate(z_List *l, z_Buffer k, z_MapRecord r, void *attr,
                          z_MapIsEqual *isEqual) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_MapRecord *record;
  z_Error ret = z_ListFind(l, k, r, attr, isEqual, &record);
  if (ret != z_OK) {
    return ret;
  }

  *record = r;
  return z_OK;
}

z_Error z_ListForceUpsert(z_List *l, z_Buffer k, z_MapRecord r, void *attr,
                          z_MapIsEqual *isEqual) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_MapRecord *record;
  z_Error ret = z_ListFind(l, k, r, attr, isEqual, &record);
  if (ret != z_OK && ret != z_ERR_NOT_FOUND) {
    return ret;
  }

  if (ret == z_OK) {
    *record = r;
    return z_OK;
  }

  if (l->Pos >= l->Len) {
    ret = z_ListGrow(l);
    if (ret != z_OK) {
      return ret;
    }
    return z_ListForceUpsert(l, k, r, attr, isEqual);
  }

  l->Records[l->Pos++] = r;
  return z_OK;
}

z_Error z_ListUpdate(z_List *l, z_Buffer k, z_MapRecord r, z_Buffer src_v,
                     void *attr, z_MapIsEqual *isEqual) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_MapRecord *record;
  z_Error ret = z_ListFind(l, k, r, attr, isEqual, &record);
  if (ret != z_OK) {
    return ret;
  }

  if (isEqual(attr, z_BufferEmpty(), src_v, record->Offset) == false) {
    return z_ERR_CONFLICT;
  }

  *record = r;
  return z_OK;
}

z_Error z_ListDelete(z_List *l, z_Buffer k, int64_t hash, void *attr,
                     z_MapIsEqual *isEqual) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  int16_t ii = 0;
  for (int16_t i = 0; i < l->Pos; ++i) {
    if (l->Records[i].Hash != hash ||
        isEqual(attr, k, z_BufferEmpty(), l->Records[i].Offset) != true) {
      l->Records[ii] = l->Records[i];
      ++ii;
    }
  }

  if (l->Pos == ii) {
    return z_ERR_NOT_FOUND;
  }
  l->Pos = ii;
  return z_OK;
}

typedef struct {
  z_Lock Lock;
  z_List List;
} z_Bucket;

z_Error z_BucketInit(z_Bucket *b) {
  if (b == nullptr) {
    z_error("b == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_LockInit(&b->Lock);

  return z_ListInit(&b->List);
}

void z_BucketDestroy(z_Bucket *b) {
  if (b == nullptr) {
    z_debug("b == nullptr");
    return;
  }

  z_ListDestroy(&b->List);

  z_LockDestroy(&b->Lock);

  return;
}

z_Error z_BucketInsert(z_Bucket *b, z_Buffer k, int64_t hash, int64_t offset,
                       void *attr, z_MapIsEqual *isEqual) {

  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_LockLock(&b->Lock);

  z_MapRecord r = {.Hash = hash, .Offset = offset};
  z_Error ret = z_ListInsert(&b->List, k, r, attr, isEqual);

  z_LockUnLock(&b->Lock);

  return ret;
}

z_Error z_BucketFind(z_Bucket *b, z_Buffer k, int64_t hash, void *attr,
                     z_MapIsEqual *isEqual, int64_t *offset) {
  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr || offset == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr || res == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_LockLock(&b->Lock);

  z_MapRecord *record;
  z_MapRecord find_req = {.Hash = hash, .Offset = -1};
  z_Error ret = z_ListFind(&b->List, k, find_req, attr, isEqual, &record);
  if (ret == z_OK) {
    *offset = record->Offset;
  }

  z_LockUnLock(&b->Lock);

  return ret;
}

z_Error z_BucketForceUpdate(z_Bucket *b, z_Buffer k, int64_t hash,
                            int64_t offset, void *attr, z_MapIsEqual *isEqual) {
  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_LockLock(&b->Lock);

  z_MapRecord r = {.Hash = hash, .Offset = offset};
  z_Error ret = z_ListForceUpdate(&b->List, k, r, attr, isEqual);

  z_LockUnLock(&b->Lock);

  return ret;
}

z_Error z_BucketForceUpsert(z_Bucket *b, z_Buffer k, int64_t hash,
                            int64_t offset, void *attr, z_MapIsEqual *isEqual) {
  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_LockLock(&b->Lock);

  z_MapRecord r = {.Hash = hash, .Offset = offset};
  z_Error ret = z_ListForceUpsert(&b->List, k, r, attr, isEqual);

  z_LockUnLock(&b->Lock);

  return ret;
} 

z_Error z_BucketUpdate(z_Bucket *b, z_Buffer k, int64_t hash, int64_t offset,
                       z_Buffer src_v, void *attr, z_MapIsEqual *isEqual) {
  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_LockLock(&b->Lock);

  z_MapRecord r = {.Hash = hash, .Offset = offset};
  z_Error ret = z_ListUpdate(&b->List, k, r, src_v, attr, isEqual);

  z_LockUnLock(&b->Lock);

  return ret;
}

z_Error z_BucketDelete(z_Bucket *b, z_Buffer k, int64_t hash, void *attr,
                       z_MapIsEqual *isEqual) {
  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr ");
    return z_ERR_INVALID_DATA;
  }

  z_LockLock(&b->Lock);

  z_Error ret = z_ListDelete(&b->List, k, hash, attr, isEqual);

  z_LockUnLock(&b->Lock);

  return ret;
}

typedef struct {
  void *Attr;
  z_MapIsEqual *IsEqual;
  z_Bucket *Buckets;
  int64_t BucketsLen;
} z_Map;

void z_MapDestroy(z_Map *m) {
  if (m == nullptr || m->Buckets == nullptr) {
    z_debug("m == nullptr || m->Buckets == nullptr");
    return;
  }

  for (int64_t i = 0; i < m->BucketsLen; ++i) {
    z_BucketDestroy(&m->Buckets[i]);
  }

  z_free(m->Buckets);
  m->Buckets = nullptr;
  m->BucketsLen = 0;
  return;
}

z_Error z_MapInit(z_Map *m, int64_t buckets_len, void *attr,
                  z_MapIsEqual *isEqual) {
  if (m == nullptr || buckets_len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("m == nullptr || buckets_len == 0 || attr == nullptr || isEqual == "
            "nullptr");
    return z_ERR_INVALID_DATA;
  }

  m->Attr = attr;
  m->IsEqual = isEqual;
  m->BucketsLen = buckets_len;
  m->Buckets = z_malloc(sizeof(z_Bucket) * m->BucketsLen);
  if (m->Buckets == nullptr) {
    z_error("m->Buckets == nullptr");
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_OK;
  for (int64_t i = 0; i < m->BucketsLen; ++i) {
    ret = z_BucketInit(&m->Buckets[i]);
    if (ret != z_OK) {
      z_error("buckets_len %lld i %lld", buckets_len, i);
      z_MapDestroy(m);
    }
  }

  return ret;
}

z_Error z_MapInsert(z_Map *m, z_Buffer k, int64_t offset) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0) {
    z_error("m == nullptr || k.Data == nullptr || k.Len == 0");
    return z_ERR_INVALID_DATA;
  }

  uint64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketInsert(b, k, hash, offset, m->Attr, m->IsEqual);
}

z_Error z_MapFind(z_Map *m, z_Buffer k, int64_t *offset) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0 || offset == nullptr) {
    z_error(
        "m == nullptr || k.Data == nullptr || k.Len == 0 || offset == nullptr");
    return z_ERR_INVALID_DATA;
  }

  uint64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketFind(b, k, hash, m->Attr, m->IsEqual, offset);
}

z_Error z_MapForceUpdate(z_Map *m, z_Buffer k, int64_t offset) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0) {
    z_error("m == nullptr || k.Data == nullptr || k.Len == 0");
    return z_ERR_INVALID_DATA;
  }

  uint64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketForceUpdate(b, k, hash, offset, m->Attr, m->IsEqual);
}

z_Error z_MapForceUpsert(z_Map *m, z_Buffer k, int64_t offset) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0) {
    z_error("m == nullptr || k.Data == nullptr || k.Len == 0");
    return z_ERR_INVALID_DATA;
  }

  uint64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketForceUpsert(b, k, hash, offset, m->Attr, m->IsEqual);
}

z_Error z_MapUpdate(z_Map *m, z_Buffer k, int64_t offset, z_Buffer src_v) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0) {
    z_error("m == nullptr || k.Data == nullptr || k.Len == 0");
    return z_ERR_INVALID_DATA;
  }

  uint64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketUpdate(b, k, hash, offset, src_v, m->Attr, m->IsEqual);
}

z_Error z_MapDelete(z_Map *m, z_Buffer k) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0) {
    z_error("m == nullptr || k.Data == nullptr || k.Len == 0 ");
    return z_ERR_INVALID_DATA;
  }

  uint64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketDelete(b, k, hash, m->Attr, m->IsEqual);
}

typedef enum {
  z_MAP_RECORD_OP_INSERT = 1,
  z_MAP_RECORD_OP_DELETE = 2,
  z_MAP_RECORD_OP_UPDATE = 3,
  z_MAP_RECORD_OP_FORCE_UPDATE = 4,
  z_MAP_RECORD_OP_FORCE_UPSERT = 5,
} z_MapRecordOP;

z_Error z_MapCheck(z_Map *m, z_MapRecordOP op, z_Buffer k, z_Buffer src_v) {
  return z_OK;
}
#endif