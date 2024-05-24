#include "zerror/error.h"
#include "znet/client.h"
#include "znet/svr_kv.h"
#include "zutils/buffer.h"
#include "zutils/log.h"
#include "zutils/threads.h"
#include "zutils/time.h"
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
16 个线程
1. 插入 100 万个 Key
2. 随机读 100 万个 Key
*/
void *z_BenchmarkSvrRun(void *arg) {
  z_SvrKV* svr_kv = (z_SvrKV*)arg;
  z_SvrKVRun(svr_kv);
  return nullptr;
}

typedef struct {
  int64_t Start;
  int64_t End;
  z_Thread Tid;
} z_BenchmarkArgs;

z_Error z_BenchmarkInsertOne(z_Cli *cli, int64_t i) {
  z_unique(z_Req) req = {};
  z_unique(z_Resp) resp = {};

  char key[32] = {};
  char val[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(val, "value%lld", i);
  z_ConstBuffer k = {.Data = (const int8_t *)key, .Len = strlen(key)};
  z_ConstBuffer v = {.Data = (const int8_t *)val, .Len = strlen(val)};

  req.Record = z_RecordNewByKV(z_ROP_INSERT, k, v);
  if (req.Record == nullptr) {
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_CliCall(cli, &req, &resp);
  if (ret != z_OK) {
    return ret;
  }

  if (resp.Ret.Code != z_OK) {
    return resp.Ret.Code;
  }

  return z_OK;
}

z_Error z_BenchmarkFindOne(z_Cli *cli, int64_t i, int64_t ii) {
  z_unique(z_Req) req = {};
  z_unique(z_Resp) resp = {};

  char key[32] = {};
  char val[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(val, "value%lld", ii);
  z_ConstBuffer k = {.Data = (const int8_t *)key, .Len = strlen(key)};
  z_ConstBuffer v = {.Data = (const int8_t *)val, .Len = strlen(val)};

  req.Record = z_RecordNewByKV(z_ROP_FIND, k, z_ConstBufferEmpty());
  if (req.Record == nullptr) {
    return z_ERR_NOSPACE;
  }

  z_Error ret = z_CliCall(cli, &req, &resp);
  if (ret != z_OK) {
    return ret;
  }

  if (resp.Ret.Code != z_OK) {
    return resp.Ret.Code;
  }

  z_ConstBuffer resp_val;
  ret = z_RecordValue(resp.Record, &resp_val);
  if (ret != z_OK) {
    return ret;
  }

  if (z_BufferIsEqual(&resp_val, &v) == false) {
    return z_ERR_INVALID_DATA;
  }

  return z_OK;
}

void *z_BenchmarkInsert(void *as) {
  z_BenchmarkArgs *args = (z_BenchmarkArgs *)as;
  z_unique(z_Cli) cli = {};
  z_Error ret = z_CliInit(&cli, "127.0.0.1", 12301, 1);
  if (ret != z_OK) {
    z_panic("z_CliInit");
  }

  for (int64_t i = args->Start; i < args->End; ++i) {
    ret = z_BenchmarkInsertOne(&cli, i);
    if (ret != z_OK) {
      z_panic("z_BenchmarkInsertOne");
    }
  }

  return nullptr;
}

void *z_BenchmarkFind(void *as) {
  z_BenchmarkArgs *args = (z_BenchmarkArgs *)as;
  z_unique(z_Cli) cli = {};
  z_Error ret = z_CliInit(&cli, "127.0.0.1", 12301, 1);
  if (ret != z_OK) {
    z_panic("z_CliInit");
  }

  srand((uint32_t)args->Start);
  int64_t count = args->End - args->Start;

  for (int64_t i = args->Start; i < args->End; ++i) {
    int64_t id = rand() % count + args->Start;
    ret = z_BenchmarkFindOne(&cli, id, id);
    if (ret != z_OK) {
      z_panic("z_BenchmarkFindOne");
    }
  }

  return nullptr;
}

int main() {
  int64_t thread_count = 8;
  int64_t key_count = 1024 * 1024;

  const char *bp = "./bin/binlog.log";
  remove(bp);

  z_LogInit("", 2);
  z_defer(z_LogDestroy);

  FILE *bmFile = fopen("./zbenchmark/benchmark.his", "a");
  z_defer(^{
    fclose(bmFile);
  });

  z_unique(z_SvrKV) svr_kv;
  z_Error ret = z_SvrKVInit(&svr_kv, bp, 1024*1024*1024, 1024, "127.0.0.1", 12301, 16);
  if (ret != z_OK) {
    z_panic("z_SvrKVInit %d", ret);
  }

  z_Thread t;
  z_ThreadCreate(&t, z_BenchmarkSvrRun, &svr_kv);
  sleep(1);

  z_BenchmarkArgs *args = z_malloc(sizeof(z_BenchmarkArgs) * thread_count);
  z_defer(^(z_BenchmarkArgs **ptr){
    z_free(*ptr);
  }, &args);

  int64_t start = z_NowMS();
  for (int64_t i = 0; i < thread_count; ++i) {
    args[i].Start = i * key_count / thread_count;
    args[i].End = args[i].Start + key_count / thread_count;
    z_ThreadCreate(&args[i].Tid, z_BenchmarkInsert, &args[i]);
  }
  for (int64_t i = 0; i < thread_count; ++i) {
    z_ThreadJion(args[i].Tid);
  }
  char date[32] = {};
  z_LocalDate(date);
  fprintf(bmFile, "======== %s ========\n", date);
  fprintf(bmFile, "z_BenchmarkInsert: %lld ms key_count %lld\n",
          z_NowMS() - start, key_count);
  printf("z_BenchmarkInsert: %lld ms key_count %lld\n", z_NowMS() - start,
         key_count);

  start = z_NowMS();
  for (int64_t i = 0; i < thread_count; ++i) {
    args[i].Start = i * key_count / thread_count;
    args[i].End = args[i].Start + key_count / thread_count;
    z_ThreadCreate(&args[i].Tid, z_BenchmarkFind, &args[i]);
  }
  for (int64_t i = 0; i < thread_count; ++i) {
    z_ThreadJion(args[i].Tid);
  }
  fprintf(bmFile, "z_BenchmarkFind: %lld ms key_count %lld\n\n",
          z_NowMS() - start, key_count);
  printf("z_BenchmarkFind: %lld ms key_count %lld\n", z_NowMS() - start,
         key_count);

  z_SvrKVStop(&svr_kv);

  z_ThreadJion(t);
  return 0;
}