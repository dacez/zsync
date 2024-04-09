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

typedef struct {
  uint64_t Pos;
  uint64_t Len;
  z_ListRecord *List;
} z_List;

z_Error z_ListInit(z_List *l) {
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
  if (l == nullptr || l->List == nullptr || l->Len == 0) {
    z_error("res == nullptr || res->List == nullptr || res->Len == 0");
    return z_ERR_INVALID_DATA;
  }
  z_ListRecord *rs = l->List;
  uint64_t len = l->Len;
  l->List = z_malloc(sizeof(z_ListRecord) * len * 2);
  if (l->List == nullptr) {
    z_error("len == %llu", len * 2);
    return z_ERR_NOSPACE;
  }
  l->Len = len * 2;
  memset(l->List, 0, sizeof(sizeof(z_ListRecord) * l->Len));
  memcpy(l->List, rs, len);
  z_free(rs);

  return z_OK;
}

void z_ListDestroy(z_List *l) {
  z_free(l->List);
  l->List = nullptr;
  l->Len = 0;
  l->Pos = 0;
}

z_Error z_ListInsert(z_List *l, z_ListRecord r) {
  z_Error ret = z_OK;

  for (uint64_t i = 0; i < l->Pos; ++i) {
    if (l->List[i].Hash == r.Hash) {
      if (l->List[i].Offset == r.Offset) {
        z_debug("i % llu hash %llu offset %llu", i, r.Hash, r.Offset);
        return z_ERR_EXIST;
      } else {
        z_debug("i % llu hash %llu arg_offset %llu list_offset", i, r.Hash,
                r.Offset, l->List[i].Offset);
        ret = z_ERR_NEED_CHECK;
        break;
      }
    }
  }

  if (l->Pos >= l->Len) {
    ret = z_ListGrow(l);
    if (ret != z_OK) {
      return ret;
    }
    return z_ListInsert(l, r);
  }

  l->List[l->Pos++] = r;
  return ret;
}

z_Error z_ListFind(z_List *l, uint64_t hash, z_List *res) {
  if (res == nullptr || res->List == nullptr || res->Len == 0) {
    z_error("res == nullptr || res->List == nullptr || res->Len == 0");
    return z_ERR_INVALID_DATA;
  }

  if (l == nullptr) {
    z_error("l == nullptr");
    return z_ERR_INVALID_DATA;
  }

  for (uint64_t i = 0; i < l->Pos; ++i) {
    if (l->List[i].Hash == hash) {
      z_Error ret = z_ListInsert(res, l->List[i]);
      if (ret != z_OK) {
        z_error("z_ListInsert");
        return ret;
      }
    }
  }

  if (res->Pos == 0) {
    return z_ERR_NOT_FOUND;
  }

  return z_OK;
}

typedef struct {
  pthread_mutex_t Mtx;
  z_List List;
} z_Bucket;

z_Error z_BucketInit(z_Bucket *b) {
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
    z_panic("b == nullptr");
  }

  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
  }

  z_ListDestroy(&b->List);

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
  }

  if (pthread_mutex_destroy(&b->Mtx) != 0) {
    z_error("pthread_mutex_destroy");
  }
}

z_Error z_BucketInsert(z_Bucket *b, z_ListRecord r) {
  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
  }

  z_Error ret = z_ListInsert(&b->List, r);

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
  }

  return ret;
}

z_Error z_BucketFind(z_Bucket *b, uint64_t hash, z_List *res) {
  if (pthread_mutex_lock(&b->Mtx) != 0) {
    z_error("pthread_mutex_lock");
  }

  z_Error ret = z_ListFind(&b->List, hash, res);

  if (pthread_mutex_unlock(&b->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
  }

  return ret;
}

typedef struct {
  z_Bucket *Buckets;
  uint64_t BucketsLen;
} z_Map;

z_Error z_MapInit(z_Map *m, uint64_t buckets_len) {
  m->BucketsLen = buckets_len;
  m->Buckets = z_malloc(sizeof(z_Bucket) * m->BucketsLen);

  z_Error ret = z_OK;
  for (uint64_t i = 0; i < m->BucketsLen; ++i) {
    ret = z_BucketInit(&m->Buckets[i]);
    if (ret != z_OK) {
      z_error("buckets_len %llu", buckets_len);
      break;
    }
  }

  return ret;
}

void z_MapDestroy(z_Map *m) {
  if (m == nullptr) {
    z_panic("z_MapFree");
  }

  for (uint64_t i = 0; i < m->BucketsLen; ++i) {
    z_BucketDestroy(&m->Buckets[i]);
  }
  return;
}

z_Error z_MapInsert(z_Map *m, z_ListRecord r) {
  z_Bucket *b = &m->Buckets[r.Hash % m->BucketsLen];
  return z_BucketInsert(b, r);
}

z_Error z_MapFind(z_Map *m, uint64_t hash, z_List *list) {
  z_Bucket *b = &m->Buckets[hash % m->BucketsLen];
  return z_BucketFind(b, hash, list);
}

z_Error z_MapDelete(z_Map *m, z_String key) { return z_OK; }
#endif