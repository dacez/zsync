#ifndef z_MAP_H
#define z_MAP_H

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

#include "zhash/hash.h"
#include "zlog/log.h"
#include "ztypes/types.h"
#include "zbinlog/record.h"

#define z_MAX_LIST_LEN 128
typedef struct {
  pthread_mutex_t Mtx;
  uint8_t Pos;
  uint8_t ListLen;
  uint64_t *List;
} z_bucket;

typedef struct {
  z_bucket *buckets;
  uint64_t buckets_len;
} z_Map;

z_Map *z_MapNew(uint64_t buckets_len) {
  z_Map *m = malloc(sizeof(z_Map));
  m->buckets = malloc(sizeof(z_bucket) * buckets_len);
  m->buckets_len = buckets_len;

  for (uint64_t i = 0; i < m->buckets_len; ++i) {
    m->buckets[i].Pos = 0;
    m->buckets[i].ListLen = 0;
    m->buckets[i].List = nullptr;
    if (pthread_mutex_init(&m->buckets[i].Mtx, nullptr) != 0) {
      z_panic("pthread_mutex_init");
    }
  }

  return m;
}

void z_MapFree(z_Map *m) {

  for (uint64_t i = 0; i < m->buckets_len; ++i) {
    if (pthread_mutex_destroy(&m->buckets[i].Mtx) != 0) {
      z_panic("pthread_mutex_destroy");
    }

    if (m->buckets[i].List != nullptr) {
      free(m->buckets[i].List);
      m->buckets[i].List = nullptr;
    }
  }

  free(m);

  return;
}

z_Error z_MapInsert(z_Map *m, z_String key, uint64_t seq) {
  uint64_t i = z_Hash(key.Data, key.Len) % m->buckets_len;
  z_bucket *b = &m->buckets[i];

  pthread_mutex_t *mtx = &b->Mtx;
  if (pthread_mutex_lock(mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  z_Error ret = z_OK;
  if (b->Pos < b->ListLen) {
    b->List[b->Pos++] = seq;
  } else if (b->ListLen < z_MAX_LIST_LEN) {
    if (b->ListLen == 0) {
      b->ListLen = 1;
      b->List = malloc(sizeof(uint64_t));
    } else {
      uint64_t *src = b->List;
      b->List = malloc(sizeof(uint64_t) * b->ListLen * 2);
      memcpy(b->List, src, b->ListLen);
      b->ListLen *= 2;
      free(src);
    }
    b->List[b->Pos++] = seq;
  } else {
    z_error("nospace");
    ret = z_ERR_NOSPACE;
  }

  if (pthread_mutex_unlock(mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }
  return ret;
}

z_Error z_MapFind(z_Map *m, z_String key, z_Record **r) { 
  uint64_t i = z_Hash(key.Data, key.Len) % m->buckets_len;
  z_bucket *b = &m->buckets[i];

  for (uint8_t pos = b->Pos - 1; pos >= 0; --pos) {
    // get record
  }

  return z_OK; 
}

z_Error z_MapDelete(z_Map *m, z_String key) { return z_OK; }
#endif