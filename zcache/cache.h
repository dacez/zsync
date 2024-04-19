#ifndef z_CACHE_H
#define z_CACHE_H
#include "zutils/utils.h"
#include <stdatomic.h>

typedef struct {
  int8_t *Data;
  int64_t Size;
  atomic_uint_fast64_t End;
  atomic_uint_fast64_t Start;
  atomic_uint_least64_t Unused;
} z_Cache;

typedef struct {
  int64_t Len;
  int64_t FileOffset;
} z_CacheRecord;

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
    z_debug("cache == nullptr || cache->Data == nullptr");
    return;
  }
  atomic_store(&cache->Start, 0);
  atomic_store(&cache->End, 0);
  atomic_store(&cache->Unused, 0);
  z_free(cache->Data);
  cache->Data = nullptr;
  cache->Size = 0;
}

z_Error z_CacheAdd(z_Cache *cache, int8_t *data, int64_t len, int64_t offset,
                   uint64_t *pos) {
  if (cache == nullptr || data == nullptr || len == 0 || pos == nullptr) {
    z_error(
        "cache == nullptr || data == nullptr || len == 0 || pos == nullptr");
    return z_ERR_INVALID_DATA;
  }

  uint64_t start = atomic_load(&cache->Start);
  uint64_t end = atomic_load(&cache->End);
  if (start > end) {
    z_panic("memory panic start(%llu) > end(%llu)", start, end);
    return z_ERR_SYS;
  }

  if (end + len + sizeof(z_CacheRecord) - start > cache->Size) {
    z_error("no space start %llu end %llu len %lld", start, end, len);
    return z_ERR_NOSPACE;
  }

  *pos = atomic_fetch_add(&cache->End, len + sizeof(z_CacheRecord));
  z_CacheRecord *cr = (z_CacheRecord *)(cache->Data + *pos % cache->Size);
  cr->FileOffset = offset;
  cr->Len = len;

  memcpy(cr + 1, data, len);
  *pos += sizeof(z_CacheRecord);

  return z_OK;
}

z_Error z_CachePtr(z_Cache *cache, uint64_t pos, void **ptr) {
  if (cache == nullptr || ptr == nullptr) {
    z_error("cache == nullptr || ptr == nullptr");
    return z_ERR_INVALID_DATA;
  }

  uint64_t unused = atomic_load(&cache->Unused);
  uint64_t end = atomic_load(&cache->End);
  if (pos < unused) {
    return z_ERR_CACHE_MISS;
  }
  if (pos >= end) {
    z_error("pos(%llu) >= e(%llu)", pos, end);
    return z_ERR_INVALID_DATA;
  }

  *ptr = cache->Data + (pos % cache->Size);
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
  bool ret = atomic_compare_exchange_strong(
      &cache->Unused, &unused, unused + cr->Len + sizeof(z_CacheRecord));
  if (ret == false) {
    z_error("unused %llu", unused);
  }

  return;
}

void z_CacheFree(z_Cache *cache) {
  if (cache == nullptr) {
    z_error("cache == nullptr");
    return;
  }

  uint64_t start = atomic_load(&cache->Start);
  uint64_t unused = atomic_load(&cache->Unused);

  if (start > unused) {
    z_error("start > unused start %llu unused %llu", start, unused);
    return;
  }

  if (start == unused) {
    z_info("start == unused start %llu unused %llu", start, unused);
    return;
  }

  z_CacheRecord *cr = (z_CacheRecord *)(cache->Data + start % cache->Size);
  bool ret = atomic_compare_exchange_strong(
      &cache->Start, &start, start + cr->Len + sizeof(z_CacheRecord));
  if (ret == false) {
    z_error("start %llu", start);
  }
}

#endif