#include "zkv/kv.h"
#include "ztest/test.h"
#include "zutils/buffer.h"
#include <string.h>

void z_KVTest() {

  char *binlog_path = "./bin/binlog.log";
  remove(binlog_path);
  z_KV kv;
  z_Error ret = z_KVInit(&kv, binlog_path, 1024 * 1024 * 1024, 1);
  z_ASSERT_TRUE(ret == z_OK);

  z_ConstBuffer k = {.Data = "key", .Size = 3};
  z_ConstBuffer v = {.Data = "value", .Size = 5};

  ret = z_KVInsert(&kv, k, v);
  z_ASSERT_TRUE(ret == z_OK);

  ret = z_KVInsert(&kv, k, v);
  z_ASSERT_TRUE(ret == z_ERR_EXIST);

  z_unique(z_Buffer) vv = {};
  ret = z_KVFind(&kv, k, &vv);
  z_ASSERT_TRUE(ret == z_OK);
  z_ASSERT_TRUE(z_BufferIsEqual(&vv, &v) == true);

  v.Data = "value1";
  v.Size = strlen(v.Data);

  z_ConstBuffer src_v = {.Data = "value", .Size = 5};
  // current = value
  // vv = value
  ret = z_KVUpdate(&kv, k, v, src_v);
  z_ASSERT_TRUE(ret == z_OK);

  z_BufferDestroy(&vv);
  ret = z_KVFind(&kv, k, &vv);
  z_ASSERT_TRUE(ret == z_OK);
  z_ASSERT_TRUE(z_BufferIsEqual(&vv, &v) == true);

  // current = value1
  // vv = value
  ret = z_KVUpdate(&kv, k, v, src_v);
  z_ASSERT_TRUE(ret == z_ERR_CONFLICT);

  ret = z_KVForceUpdate(&kv, k, v);
  z_ASSERT_TRUE(ret == z_OK);

  ret = z_KVDelete(&kv, k);
  z_ASSERT_TRUE(ret == z_OK);

  ret = z_KVFind(&kv, k, &vv);
  z_ASSERT_TRUE(ret == z_ERR_NOT_FOUND);

  ret = z_KVForceUpdate(&kv, k, v);
  z_ASSERT_TRUE(ret == z_ERR_NOT_FOUND);

  ret = z_KVDelete(&kv, k);
  z_ASSERT_TRUE(ret == z_ERR_NOT_FOUND);

  z_KVDestroy(&kv);
}