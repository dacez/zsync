#ifndef z_CACHE_H
#define z_CACHE_H

#include "zerror/error.h"
#include "zutils/log.h"
#include "zutils/mem.h"

#include <stdatomic.h>

typedef struct {
  int64_t Len;
} z_CacheRecord;

typedef bool z_CacheBeforeRemove(void *attr, z_CacheRecord *r);

typedef struct {
  int8_t *Data;
  int64_t Size;
  atomic_uint_fast64_t End;
  atomic_uint_fast64_t Start;
  atomic_uint_least64_t Unused;
  void *Attr;
  z_CacheBeforeRemove *BeforeRemove;
} z_Cache;

z_Error z_CacheInit(z_Cache *cache, int64_t size) {
  if (cache == nullptr || size < sizeof(z_CacheRecord)) {
    z_error("cache == nullptr || size < sizeof(z_CacheRecord)");
    return z_ERR_INVALID_DATA;
  }

  cache->Data = z_malloc(size);
  if (cache->Data == nullptr) {
    z_error("cache->Data == nullptr");
    return z_ERR_NOSPACE;
  }

  cache->Size = size;
  atomic_store(&cache->Start, 0);
  atomic_store(&cache->End, 0);
  atomic_store(&cache->Unused, 0);
  return z_OK;
}

void z_CacheDestory(z_Cache *cache) {
  if (cache == nullptr || cache->Data == nullptr) {
    return;
  }
  atomic_store(&cache->Start, 0);
  atomic_store(&cache->End, 0);
  atomic_store(&cache->Unused, 0);
  z_free(cache->Data);
  cache->Data = nullptr;
  cache->Size = 0;
}

z_Error z_CacheAdd(z_Cache *cache, int8_t *data, int64_t len,
                   uint64_t *offset) {
  if (cache == nullptr || data == nullptr || len == 0 || offset == nullptr) {
    z_error(
        "cache == nullptr || data == nullptr || len == 0 || offset == nullptr");
    return z_ERR_INVALID_DATA;
  }

  uint64_t start = atomic_load(&cache->Start);
  uint64_t end = atomic_load(&cache->End);
  if (start > end) {
    z_panic("add cache panic start(%llu) > end(%llu)", start, end);
    return z_ERR_INVALID_DATA;
  }

  if (end + len + sizeof(z_CacheRecord) - start > cache->Size) {
    z_error("no space start %llu end %llu len %lld", start, end, len);
    return z_ERR_NOSPACE;
  }

  *offset = atomic_fetch_add(&cache->End, len + sizeof(z_CacheRecord));
  z_CacheRecord *cr = (z_CacheRecord *)(cache->Data + *offset % cache->Size);
  cr->Len = len;

  memcpy(cr + 1, data, len);
  return z_OK;
}

z_Error z_CachePtr(z_Cache *cache, uint64_t offset, void **ptr) {
  if (cache == nullptr || ptr == nullptr) {
    z_error("cache == nullptr || ptr == nullptr");
    return z_ERR_INVALID_DATA;
  }

  uint64_t unused = atomic_load(&cache->Unused);
  uint64_t end = atomic_load(&cache->End);
  if (offset < unused) {
    return z_ERR_CACHE_MISS;
  }
  if (offset >= end) {
    z_error("offset(%llu) >= e(%llu)", offset, end);
    return z_ERR_INVALID_DATA;
  }

  *ptr = cache->Data + (offset % cache->Size);
  return z_OK;
}

void z_CacheUnused(z_Cache *cache) {
  if (cache == nullptr) {
    z_error("cache == nullptr");
    return;
  }

  uint64_t unused = atomic_load(&cache->Unused);
  uint64_t end = atomic_load(&cache->End);

  if (unused > end) {
    z_error("unused > end unused %llu end %llu", unused, end);
    return;
  }

  if (unused == end) {
    z_info("unused == end unused %llu end %llu", unused, end);
    return;
  }

  z_CacheRecord *cr = (z_CacheRecord *)(cache->Data + unused % cache->Size);
  uint64_t des = unused + cr->Len + sizeof(z_CacheRecord);
  bool ret = atomic_compare_exchange_strong(&cache->Unused, &unused, des);
  if (ret == false) {
    z_error("unused %llu", unused);
  }

  return;
}

void z_CacheRemove(z_Cache *cache, uint64_t unused) {
  if (cache == nullptr) {
    z_error("cache == nullptr");
    return;
  }

  uint64_t start = atomic_load(&cache->Start);
  uint64_t cache_unused = atomic_load(&cache->Unused);

  if (unused > cache_unused || start > unused) {
    z_error("unused > cache_unused || start > unused start %llu unused %llu "
            "cache_unused %llu",
            start, unused, cache_unused);
    return;
  }

  if (start == unused) {
    z_error("start == unused start %llu unused %llu", start, unused);
    return;
  }

  while (start < unused) {
    z_CacheRecord *cr = (z_CacheRecord *)(cache->Data + start % cache->Size);

    z_Error ret = cache->BeforeRemove(cache->Attr, cr);
    if (ret != z_OK) {
      z_error("BeforeRemove start %llu unused %llu", start, unused);
      break;
    }

    uint64_t des = start + cr->Len + sizeof(z_CacheRecord);
    bool atomic_ret =
        atomic_compare_exchange_strong(&cache->Start, &start, des);
    if (atomic_ret == false) {
      z_error("start %llu", start);
    }
    start = des;
  }

  if (start != unused) {
    z_panic("remove memory panic start(%llu) > unused(%llu)", start, unused);
    return;
  }

  return;
}

#endif