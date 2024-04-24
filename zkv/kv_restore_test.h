#include "zkv/kv_loop_test.h"

bool z_KVRestoreTestCheck(z_KV *kv, int64_t start, int64_t count) {
  bool final_ret = true;
  bool ret = false;
  for (int64_t i = start; i < count; ++i) {
    ret = z_FindNotFound(kv, i);
    if (ret == false) {
      final_ret = false;
    }
  }

  return final_ret;
}

void z_KVRestoreTest() {
  char *binlog_path = "./bin/binlog.log";
  remove(binlog_path);
  int64_t count = 2000;

  z_KV kv;
  z_Error ret =
      z_KVInit(&kv, binlog_path, 1024LL * 1024LL * 1024LL, 1024 * 1024);
  z_ASSERT(ret == z_OK);

  bool loop_ret = z_Loop(&kv, 0, count);
  z_ASSERT(loop_ret == true);

  z_KVDestroy(&kv);

  ret = z_KVInit(&kv, binlog_path, 1024LL * 1024LL * 1024LL, 10);
  z_ASSERT(ret == z_OK);
  
  loop_ret = z_KVRestoreTestCheck(&kv, 0, count);
  z_ASSERT(loop_ret == true);

  z_KVDestroy(&kv);
  return;
} 