#ifndef z_MAP_H
#define z_MAP_H

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

#include "zbinlog/record.h"
#include "zhash/hash.h"
#include "zlog/log.h"
#include "ztypes/types.h"

#define z_BUCKET_LIST_LEN 32

typedef struct {
  uint64_t Offset;
  uint64_t Hash;
} z_ListRecord;

typedef struct {
  pthread_mutex_t Mtx;
  uint64_t Pos;
  uint64_t ListLen;
  z_ListRecord *List;
} z_Bucket;

void z_BucketListNew(z_Bucket *b, uint64_t len) {
  b->ListLen = len;
  b->Pos = 0;
  b->List = z_malloc(sizeof(z_ListRecord) * b->ListLen);
  memset(b->List, 0, sizeof(sizeof(z_ListRecord) * b->ListLen));
}

void z_BucketListFree(z_Bucket *b) {
  free(b->List);
  b->ListLen = 0;
  b->Pos = 0;
}

z_Error z_BucketListInsertNoLock(z_Bucket *b, z_ListRecord r) {
  if (b->List == nullptr) {
    z_BucketListNew(b, z_BUCKET_LIST_LEN);
  }

  z_Error ret = z_OK;

  for (uint64_t i = 0; i < b->Pos; ++i) {
    if (b->List[i].Hash == r.Hash) {
      if (b->List[i].Offset == r.Offset) {
        return z_ERR_EXIST;
      } else {
        ret = z_ERR_NEED_CHECK;
      }
    }
  }

  if (b->Pos < b->ListLen) {
    b->List[b->Pos++] = r;
  } else {
    ret = z_ERR_NOSPACE;
  }

  return ret;
}

z_Error z_BucketListInsert(z_Bucket *b, z_ListRecord r) {
  pthread_mutex_t *mtx = &b->Mtx;
  if (pthread_mutex_lock(mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  z_Error ret = z_OK;

  while (1) {
    ret = z_BucketListInsertNoLock(b, r);

    if (ret == z_ERR_NOSPACE) {
      z_ListRecord *rs = b->List;
      uint64_t len = b->ListLen;
      z_BucketListNew(b, b->ListLen * 2);
      memcpy(b->List, rs, len);
      z_free(rs);
    } else {
      break;
    }
  }

  if (pthread_mutex_unlock(mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }

  return ret;
}

void z_BucketFree(z_Bucket *b) {
  if (pthread_mutex_destroy(&b->Mtx) != 0) {
    z_error("pthread_mutex_destroy");
  }

  if (b->List != nullptr) {
    z_BucketListFree(b);
  }

  free(b);
}

void z_BucketInit(z_Bucket *b) {
  if (pthread_mutex_init(&b->Mtx, nullptr) != 0) {
    z_error("pthread_mutex_init");
  }

  b->List = nullptr;
  b->ListLen = 0;
  b->Pos = 0;
}

typedef struct {
  z_Bucket *Buckets;
  uint64_t BucketsLen;
} z_Map;

z_Map *z_MapNew(uint64_t buckets_len) {
  z_Map *m = z_malloc(sizeof(z_Map));
  m->BucketsLen = buckets_len;
  m->Buckets = z_malloc(sizeof(z_Bucket) * m->BucketsLen);

  for (uint64_t i = 0; i < m->BucketsLen; ++i) {
    z_BucketInit(&m->Buckets[i]);
  }

  return m;
}

void z_MapFree(z_Map *m) {
  for (uint64_t i = 0; i < m->BucketsLen; ++i) {
    z_BucketFree(&m->Buckets[i]);
  }
  free(m);

  return;
}

z_Error z_MapInsert(z_Map *m, z_ListRecord r) {
  z_Bucket *b = &m->Buckets[r.Hash%m->BucketsLen];

  z_Error ret = z_BucketListInsert(b, r);

  return ret;
}

z_Error z_MapFind(z_Map *m, uint64_t hash, z_Record **r) {

  return z_OK;
}

z_Error z_MapDelete(z_Map *m, z_String key) { return z_OK; }
#endif