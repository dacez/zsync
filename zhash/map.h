#ifndef z_MAP_H
#define z_MAP_H

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

#include "zbinlog/record.h"
#include "zhash/hash.h"
#include "zlog/log.h"
#include "ztypes/types.h"

#define z_LIST_INIT_LEN 2

typedef struct {
  int64_t Offset;
  int64_t Hash;
} z_ListRecord;


typedef enum {
  Z_MAP_CMP_TYPE_KEY = 1,
  Z_MAP_CMP_TYPE_VALUE = 2,
} z_MapCmpType;

typedef bool z_MapIsEqual(void *attr, z_MapCmpType type, z_String str, z_ListRecord r);


typedef struct {
  int64_t Pos;
  int64_t Len;
  z_ListRecord *List;
} z_List;

void z_ListDestroy(z_List *l) {
  if (l == nullptr || l->List == nullptr) {
    z_debug("l == nullptr || l->List == nullptr");
    return;
  }

  z_free(l->List);
  l->List = nullptr;
  l->Len = 0;
  l->Pos = 0;
}

z_Error z_ListInit(z_List *l) {
  if (l == nullptr) {
    z_error("l == nullptr");
    return z_ERR_INVALID_DATA;
  }

  l->List = z_malloc(sizeof(z_ListRecord) * z_LIST_INIT_LEN);
  if (l->List == nullptr) {
    z_error("len == %ll", z_LIST_INIT_LEN);
    return z_ERR_NOSPACE;
  }
  l->Len = z_LIST_INIT_LEN;
  l->Pos = 0;
  memset(l->List, 0, sizeof(sizeof(z_ListRecord) * l->Len));
  return z_OK;
}

z_Error z_ListGrow(z_List *l) {
  if (l->List == nullptr || l->Len == 0) {
    z_error("l->List == nullptr || l->Len == 0");
    return z_ERR_INVALID_DATA;
  }

  z_ListRecord *rs = l->List;
  int64_t len = l->Len;
  l->List = z_malloc(sizeof(z_ListRecord) * len * 2);
  if (l->List == nullptr) {
    z_error("len == %ll", len * 2);
    l->List = rs;
    return z_ERR_NOSPACE;
  }
  l->Len = len * 2;
  memset(l->List, 0, sizeof(sizeof(z_ListRecord) * l->Len));
  memcpy(l->List, rs, len);
  z_free(rs);

  return z_OK;
}

z_Error z_ListInsert(z_List *l, z_String k, z_ListRecord r, void *attr,
                     z_MapIsEqual *isEqual) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_Error ret = z_OK;

  for (int64_t i = 0; i < l->Pos; ++i) {
    if (l->List[i].Hash == r.Hash) {
      if (l->List[i].Offset == r.Offset) {
        z_debug("i % ll hash %ll offset %ll", i, r.Hash, r.Offset);
        return z_ERR_EXIST;
      } else {
        z_debug("i % ll hash %ll arg_offset %ll list_offset", i, r.Hash,
                r.Offset, l->List[i].Offset);
        if (isEqual(attr, Z_MAP_CMP_TYPE_KEY, k, r) == true) {
          char short_str[16] = {};
          z_CStr(k, short_str);
          z_debug("key %s", short_str);
          return z_ERR_EXIST;
        }
      }
    }
  }

  if (l->Pos >= l->Len) {
    ret = z_ListGrow(l);
    if (ret != z_OK) {
      return ret;
    }
    return z_ListInsert(l, k, r, attr, isEqual);
  }

  l->List[l->Pos++] = r;
  return ret;
}

z_Error z_ListFind(z_List *l, z_String k, int64_t hash, void *attr,
                   z_MapIsEqual *isEqual, z_ListRecord **res) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr || res == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr || res == nullptr");
    return z_ERR_INVALID_DATA;
  }

  for (int64_t i = 0; i < l->Pos; ++i) {
    if (l->List[i].Hash == hash && isEqual(attr, Z_MAP_CMP_TYPE_KEY, k, l->List[i]) == true) {
      *res = &l->List[i];
      return z_OK;
    }
  }

  return z_ERR_NOT_FOUND;
}

z_Error z_ListUpdate(z_List *l, z_String k, z_String v, z_ListRecord r, bool force,
                     void *attr, z_MapIsEqual *isEqual) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      v.Data == nullptr || v.Len == 0 || isEqual == nullptr) {
    z_error(
        "l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr || "
        "v.Data == nullptr || v.Len == 0 || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_ListRecord *res;
  z_Error ret = z_ListFind(l, k, r.Hash, attr, isEqual, &res);
  if (ret != z_OK) {
    return ret;
  }

  if (force == true || isEqual(attr, Z_MAP_CMP_TYPE_VALUE, v, *res) == true) {
    *res = r;
    return z_OK;
  }

  return z_ERR_CONFLICT;
}

z_Error z_ListDelete(z_List *l, z_String k, int64_t hash, void *attr,
                     z_MapIsEqual *isEqual) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  int64_t ii = 0;
  for (int64_t i = 0; i < l->Pos; ++i) {
    if (l->List[i].Hash != hash || isEqual(attr, Z_MAP_CMP_TYPE_KEY, k, l->List[i]) != true) {
      l->List[ii] = l->List[i];
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
  pthread_mutex_t Mtx;
  z_List List;
} z_Bucket;

z_Error z_BucketInit(z_Bucket *b) {
  if (b == nullptr) {
    z_error("b == nullptr");
    return z_ERR_INVALID_DATA;
  }

  if (pthread_mutex_init(&b->Mtx, nullptr) != 0) {
    z_error("pthread_mutex_init");
    return z_ERR_SYS;
  }

  return z_ListInit(&b->List);
}

void z_BucketDestroy(z_Bucket *b) {
  if (b == nullptr) {
    z_debug("b == nullptr");
    return;
  }

  z_ListDestroy(&b->List);

  if (pthread_mutex_destroy(&b->Mtx) != 0) {
    z_error("pthread_mutex_destroy");
    return;
  }

  return;
}

z_Error z_BucketInsert(z_Bucket *b, z_String k, int64_t hash, int64_t offset, void *attr,
                       z_MapIsEqual *isEqual) {

  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  z_ListRecord r = {.Hash = hash, .Offset = offset};
  z_Error ret = z_ListInsert(&b->List, k, r, attr, isEqual);

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }

  return ret;
}

z_Error z_BucketFind(z_Bucket *b, z_String k, int64_t hash, void *attr,
                     z_MapIsEqual *isEqual, int64_t *offset) {
  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr || offset == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr || res == nullptr");
    return z_ERR_INVALID_DATA;
  }

  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }
  z_ListRecord *record;
  z_Error ret = z_ListFind(&b->List, k, hash, attr, isEqual, &record);
  if (ret == z_OK) {
    *offset = record->Offset;
  }

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }

  return ret;
}

z_Error z_BucketUpdate(z_Bucket *b, z_String k, z_String v, int64_t hash, int64_t offset, bool force, void *attr,
                     z_MapIsEqual *isEqual) {
  if (b == nullptr || k.Data == nullptr || k.Len == 0 || v.Data == nullptr || v.Len == 0 || attr == nullptr || isEqual == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || v.Data == nullptr || v.Len == 0 || attr == nullptr || isEqual == nullptr");
    return z_ERR_INVALID_DATA;
  }

  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  z_ListRecord r = {.Hash = hash, .Offset = offset};
  z_Error ret = z_ListUpdate(&b->List, k, v, r, force, attr, isEqual);

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }

  return ret;
}

z_Error z_BucketDelete(z_Bucket *b, z_String k, int64_t hash, void *attr,
                       z_MapIsEqual *isEqual) {
  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      isEqual == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || isEqual == nullptr ");
    return z_ERR_INVALID_DATA;
  }

  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  z_Error ret = z_ListDelete(&b->List, k, hash, attr, isEqual);

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }

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

z_Error z_MapInit(z_Map *m, int64_t buckets_len, void *attr, z_MapIsEqual *isEqual) {
  if (m == nullptr || buckets_len == 0 || attr == nullptr || isEqual == nullptr) {
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
      z_error("buckets_len %ll i %ll", buckets_len, i);
      // free
      z_MapDestroy(m);
    }
  }

  return ret;
}

z_Error z_MapInsert(z_Map *m, z_String k, int64_t offset) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0) {
    z_error("m == nullptr || k.Data == nullptr || k.Len == 0");
    return z_ERR_INVALID_DATA;
  }

  int64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketInsert(b, k, hash, offset, m->Attr, m->IsEqual);
}

z_Error z_MapFind(z_Map *m, z_String k, int64_t *offset) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0 || offset == nullptr) {
    z_error(
        "m == nullptr || k.Data == nullptr || k.Len == 0 || offset == nullptr");
    return z_ERR_INVALID_DATA;
  }

  int64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketFind(b, k, hash, m->Attr, m->IsEqual, offset);
}

z_Error z_MapUpdate(z_Map *m, z_String k, z_String v, int64_t offset, bool force) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0 || v.Data == nullptr || v.Len == 0 ) {
    z_error(
        "m == nullptr || k.Data == nullptr || k.Len == 0 || v.Data == nullptr || v.Len == 0");
    return z_ERR_INVALID_DATA;
  }

  int64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketUpdate(b, k, v, hash, offset, force, m->Attr, m->IsEqual);
}

z_Error z_MapDelete(z_Map *m, z_String k) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0) {
    z_error("m == nullptr || k.Data == nullptr || k.Len == 0 ");
    return z_ERR_INVALID_DATA;
  }

  int64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketDelete(b, k, hash, m->Attr, m->IsEqual);
}
#endif