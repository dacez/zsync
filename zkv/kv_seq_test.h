#include "zbinlog/file.h"
#include "zkv/kv_loop_test.h"
#include "ztest/test.h"
#include <stdint.h>

bool z_KVSeqTestCheck() {
  char *binlog_path = "./bin/binlog.log";
  z_Reader rd;
  z_Error ret = z_ReaderInit(&rd, binlog_path);
  z_ASSERT(ret == z_OK);

  int64_t seq = 1;
  int64_t max_offset = 0;
  int64_t cur_offset = 0;
  ret = z_ReaderMaxOffset(&rd, &max_offset);
  z_ASSERT(ret == z_OK);
  
  while (cur_offset < max_offset) {
    z_FileRecord *r;
    ret = z_ReaderGetRecord(&rd, &r);
    if (ret != z_OK) {
      z_FileRecordFree(r);
      break;
    }

    if (seq != r->Seq) {
      z_FileRecordFree(r);
      return false;
    }

    seq++;
    z_FileRecordFree(r);

    ret = z_ReaderOffset(&rd, &cur_offset);
    if (ret != z_OK) {
      break;
    }
  }

  z_ReaderDestroy(&rd);
  return true;
}

void z_KVSeqTest() {
  char *binlog_path = "./bin/binlog.log";
  remove(binlog_path);
  int64_t count = 2000;

  z_KV kv;
  z_Error ret =
      z_KVInit(&kv, binlog_path, 1024LL * 1024LL * 1024LL, 1024 * 1024);
  z_ASSERT(ret == z_OK);

  bool loop_ret = z_Loop(&kv, 0, count);
  z_ASSERT(loop_ret == true);

  loop_ret = z_KVSeqTestCheck();
  z_ASSERT(loop_ret == true);

  z_KVDestroy(&kv);

  return;
}