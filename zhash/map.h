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
  uint64_t Offset;
  uint64_t Hash;
} z_ListRecord;

typedef int z_MapCmp(void *attr, z_String str, z_ListRecord r);

typedef struct {
  uint64_t Pos;
  uint64_t Len;
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
    z_error("len == %llu", z_LIST_INIT_LEN);
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
  uint64_t len = l->Len;
  l->List = z_malloc(sizeof(z_ListRecord) * len * 2);
  if (l->List == nullptr) {
    z_error("len == %llu", len * 2);
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
                     z_MapCmp *cmp) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      cmp == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || cmp == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_Error ret = z_OK;

  for (uint64_t i = 0; i < l->Pos; ++i) {
    if (l->List[i].Hash == r.Hash) {
      if (l->List[i].Offset == r.Offset) {
        z_debug("i % llu hash %llu offset %llu", i, r.Hash, r.Offset);
        return z_ERR_EXIST;
      } else {
        z_debug("i % llu hash %llu arg_offset %llu list_offset", i, r.Hash,
                r.Offset, l->List[i].Offset);
        if (cmp(attr, k, r) == 0) {
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
    return z_ListInsert(l, k, r, attr, cmp);
  }

  l->List[l->Pos++] = r;
  return ret;
}

z_Error z_ListFind(z_List *l, z_String k, uint64_t hash, void *attr,
                   z_MapCmp *cmp, z_ListRecord **res) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      cmp == nullptr || res == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || cmp == nullptr || res == nullptr");
    return z_ERR_INVALID_DATA;
  }

  for (uint64_t i = 0; i < l->Pos; ++i) {
    if (l->List[i].Hash == hash && cmp(attr, k, l->List[i]) == 0) {
      *res = &l->List[i];
      return z_OK;
    }
  }

  return z_ERR_NOT_FOUND;
}

z_Error z_ListUpdate(z_List *l, z_String k, z_String v, z_ListRecord r, bool force,
                     void *attr, z_MapCmp *cmp) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      v.Data == nullptr || v.Len == 0 || cmp == nullptr) {
    z_error(
        "l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr || "
        "v.Data == nullptr || v.Len == 0 || cmp == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_ListRecord *res;
  z_Error ret = z_ListFind(l, k, r.Hash, attr, cmp, &res);
  if (ret != z_OK) {
    return ret;
  }

  if (force == true || cmp(attr, v, *res) == 0) {
    *res = r;
    return z_OK;
  }

  return z_ERR_CONFLICT;
}

z_Error z_ListDelete(z_List *l, z_String k, uint64_t hash, void *attr,
                     z_MapCmp *cmp) {
  if (l == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      cmp == nullptr) {
    z_error("l == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || cmp == nullptr");
    return z_ERR_INVALID_DATA;
  }

  uint64_t ii = 0;
  for (uint64_t i = 0; i < l->Pos; ++i) {
    if (l->List[i].Hash != hash || cmp(attr, k, l->List[i]) != 0) {
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

  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  z_Error ret = z_ListInit(&b->List);

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }

  return ret;
}

void z_BucketDestroy(z_Bucket *b) {
  if (b == nullptr) {
    z_debug("b == nullptr");
    return;
  }

  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
    return;
  }

  z_ListDestroy(&b->List);

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return;
  }

  if (pthread_mutex_destroy(&b->Mtx) != 0) {
    z_error("pthread_mutex_destroy");
    return;
  }
}

z_Error z_BucketInsert(z_Bucket *b, z_String k, z_ListRecord r, void *attr,
                       z_MapCmp *cmp) {

  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      cmp == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || cmp == nullptr");
    return z_ERR_INVALID_DATA;
  }

  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  z_Error ret = z_ListInsert(&b->List, k, r, attr, cmp);

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }

  return ret;
}

z_Error z_BucketFind(z_Bucket *b, z_String k, uint64_t hash, void *attr,
                     z_MapCmp *cmp, uint64_t *res) {
  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      cmp == nullptr || res == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || cmp == nullptr || res == nullptr");
    return z_ERR_INVALID_DATA;
  }

  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }
  z_ListRecord *record;
  z_Error ret = z_ListFind(&b->List, k, hash, attr, cmp, &record);
  if (ret == z_OK) {
    *res = record->Offset;
  }

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }

  return ret;
}

z_Error z_BucketUpdate(z_Bucket *b, z_String k, z_String v, z_ListRecord r, bool force, void *attr,
                     z_MapCmp *cmp) {
  if (b == nullptr || k.Data == nullptr || k.Len == 0 || v.Data == nullptr || v.Len == 0 || attr == nullptr || cmp == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || v.Data == nullptr || v.Len == 0 || attr == nullptr || cmp == nullptr");
    return z_ERR_INVALID_DATA;
  }

  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  z_Error ret = z_ListUpdate(&b->List, k, v, r, force, attr, cmp);

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }

  return ret;
}

z_Error z_BucketDelete(z_Bucket *b, z_String k, uint64_t hash, void *attr,
                       z_MapCmp *cmp) {
  if (b == nullptr || k.Data == nullptr || k.Len == 0 || attr == nullptr ||
      cmp == nullptr) {
    z_error("b == nullptr || k.Data == nullptr || k.Len == 0 || attr == "
            "nullptr || cmp == nullptr ");
    return z_ERR_INVALID_DATA;
  }

  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  z_Error ret = z_ListDelete(&b->List, k, hash, attr, cmp);

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }

  return ret;
}

typedef struct {
  void *Attr;
  z_MapCmp *Cmp;
  z_Bucket *Buckets;
  uint64_t BucketsLen;
} z_Map;

void z_MapDestroy(z_Map *m) {
  if (m == nullptr || m->Buckets == nullptr) {
    z_debug("m == nullptr || m->Buckets == nullptr");
    return;
  }

  for (uint64_t i = 0; i < m->BucketsLen; ++i) {
    z_BucketDestroy(&m->Buckets[i]);
  }

  z_free(m->Buckets);
  m->Buckets = nullptr;
  m->BucketsLen = 0;
  return;
}

z_Error z_MapInit(z_Map *m, uint64_t buckets_len, void *attr, z_MapCmp *cmp) {
  if (m == nullptr || buckets_len == 0 || attr == nullptr || cmp == nullptr) {
    z_error("m == nullptr || buckets_len == 0 || attr == nullptr || cmp == "
            "nullptr");
    return z_ERR_INVALID_DATA;
  }

  m->Attr = attr;
  m->Cmp = cmp;
  m->BucketsLen = buckets_len;
  m->Buckets = z_malloc(sizeof(z_Bucket) * m->BucketsLen);
  if (m->Buckets == nullptr) {
    z_error("m->Buckets == nullptr");
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_OK;
  for (uint64_t i = 0; i < m->BucketsLen; ++i) {
    ret = z_BucketInit(&m->Buckets[i]);
    if (ret != z_OK) {
      z_error("buckets_len %llu i %llu", buckets_len, i);
      // free
      z_MapDestroy(m);
    }
  }

  return ret;
}

z_Error z_MapInsert(z_Map *m, z_String k, z_ListRecord r) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0) {
    z_error("m == nullptr || k.Data == nullptr || k.Len == 0");
    return z_ERR_INVALID_DATA;
  }

  z_Bucket *b = &m->Buckets[r.Hash % m->BucketsLen];
  return z_BucketInsert(b, k, r, m->Attr, m->Cmp);
}

z_Error z_MapFind(z_Map *m, z_String k, uint64_t *res) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0 || res == nullptr) {
    z_error(
        "m == nullptr || k.Data == nullptr || k.Len == 0 || res == nullptr");
    return z_ERR_INVALID_DATA;
  }

  uint64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketFind(b, k, hash, m->Attr, m->Cmp, res);
}

z_Error z_MapUpdate(z_Map *m, z_String k, z_String v, uint64_t offset, bool force) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0 || v.Data == nullptr || v.Len == 0 ) {
    z_error(
        "m == nullptr || k.Data == nullptr || k.Len == 0 || v.Data == nullptr || v.Len == 0");
    return z_ERR_INVALID_DATA;
  }

  uint64_t hash = z_Hash(k.Data, k.Len);
  z_ListRecord r = {.Hash = hash, .Offset = offset};
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketUpdate(b, k, v, r, force, m->Attr, m->Cmp);
}

z_Error z_MapDelete(z_Map *m, z_String k) {
  if (m == nullptr || k.Data == nullptr || k.Len == 0) {
    z_error("m == nullptr || k.Data == nullptr || k.Len == 0 ");
    return z_ERR_INVALID_DATA;
  }

  uint64_t hash = z_Hash(k.Data, k.Len);
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketDelete(b, k, hash, m->Attr, m->Cmp);
}
#endif