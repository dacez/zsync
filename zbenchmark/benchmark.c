#include "zerror/error.h"
#include "znet/client.h"
#include "znet/svr_kv.h"
#include "zutils/log.h"
#include "zutils/time.h"
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/*
16 个线程
1. 插入 100 万个 Key
2. 随机读 100 万个 Key
*/
void *z_BenchmarkSvrRun(void *) {
  char *bp = "./bin/binlog.log";
  remove(bp);
  z_SvrKV(bp, 8);
  return nullptr;
}

typedef struct {
  int64_t Start;
  int64_t End;
  pthread_t Tid;
} z_BenchmarkArgs;

z_Error z_BenchmarkInsertOne(z_Cli *cli, int64_t i) {
  z_unique(z_Req) req = {};
  z_unique(z_Resp) resp = {};
  z_unique(z_Buffer) k = {};
  z_unique(z_Buffer) v = {};

  char key[32] = {};
  char val[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(val, "value%lld", i);

  z_Error ret = z_BufferInit(&k, (int8_t *)key, strlen(key));
  if (ret != z_OK) {
    return ret;
  }
  ret = z_BufferInit(&v, (int8_t *)val, strlen(val));
  if (ret != z_OK) {
    return ret;
  }

  req.Record = z_RecordNewByKV(z_ROP_INSERT, k, v);
  if (ret != z_OK) {
    return ret;
  }

  ret = z_CliCall(cli, &req, &resp);
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
  z_unique(z_Buffer) k = {};
  z_unique(z_Buffer) v_ii = {};

  char key[32] = {};
  char val_ii[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(val_ii, "value%lld", ii);

  z_Error ret = z_BufferInit(&k, (int8_t *)key, strlen(key));
  if (ret != z_OK) {
    return ret;
  }

  ret = z_BufferInit(&v_ii, (int8_t *)val_ii, strlen(val_ii));
  if (ret != z_OK) {
    return ret;
  }

  req.Record = z_RecordNewByKV(z_ROP_FIND, k, z_BufferEmpty());
  if (ret != z_OK) {
    return ret;
  }

  ret = z_CliCall(cli, &req, &resp);
  if (ret != z_OK) {
    return ret;
  }

  if (resp.Ret.Code != z_OK) {
    return resp.Ret.Code;
  }

  z_Buffer resp_val;
  ret = z_RecordValue(resp.Record, &resp_val);
  if (ret != z_OK) {
    return ret;
  }

  if (z_BufferIsEqual(resp_val, v_ii) == false) {
    return z_ERR_INVALID_DATA;
  }

  return z_OK;
}

void *z_BenchmarkInsert(void *as) {
  z_BenchmarkArgs *args = (z_BenchmarkArgs *)as;
  z_unique(z_Cli) cli = {};
  z_Error ret = z_CliInit(&cli, "127.0.0.1", 12301, 16);
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
  z_Error ret = z_CliInit(&cli, "127.0.0.1", 12301, 16);
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

  z_LogInit("", 2);
  z_defer(z_LogDestroy);

  FILE *bmFile = fopen("./zbenchmark/benchmark.his", "a");
  z_defer(^{
    fclose(bmFile);
  });

  pthread_t tid;
  pthread_create(&tid, nullptr, z_BenchmarkSvrRun, nullptr);
  sleep(1);

  z_BenchmarkArgs *args = z_malloc(sizeof(z_BenchmarkArgs) * thread_count);
  z_defer(^{
    z_free(args);
  });

  int64_t start = z_NowMS();
  for (int64_t i = 0; i < thread_count; ++i) {
    args[i].Start = i * key_count / thread_count;
    args[i].End = args[i].Start + key_count / thread_count;
    pthread_create(&args[i].Tid, nullptr, z_BenchmarkInsert, &args[i]);
  }
  for (int64_t i = 0; i < thread_count; ++i) {
    pthread_join(args[i].Tid, nullptr);
  }
  char date[32] = {};
  z_LocalDate(date);
  fprintf(bmFile, "======== %s ========\n", date);
  fprintf(bmFile, "z_BenchmarkInsert: %lld ms key_count %lld\n", z_NowMS() - start, key_count);
  printf("z_BenchmarkInsert: %lld ms key_count %lld\n", z_NowMS() - start, key_count);

  start = z_NowMS();
  for (int64_t i = 0; i < thread_count; ++i) {
    args[i].Start = i * key_count / thread_count;
    args[i].End = args[i].Start + key_count / thread_count;
    pthread_create(&args[i].Tid, nullptr, z_BenchmarkFind, &args[i]);
  }
  for (int64_t i = 0; i < thread_count; ++i) {
    pthread_join(args[i].Tid, nullptr);
  }
  fprintf(bmFile, "z_BenchmarkFind: %lld ms key_count %lld\n\n", z_NowMS() - start, key_count);
  printf("z_BenchmarkFind: %lld ms key_count %lld\n", z_NowMS() - start, key_count);

  return 0;
}