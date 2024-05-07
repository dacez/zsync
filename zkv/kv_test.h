#include "zkv/kv.h"
#include "ztest/test.h"

void z_KVTest() {

  char *binlog_path = "./bin/binlog.log";
  z_KV kv;
  remove(binlog_path);
  z_Error ret = z_KVInit(&kv, binlog_path, 1024 * 1024 * 1024, 1);
  z_ASSERT_TRUE(ret == z_OK);

  z_Buffer k, v, vv;
  z_BufferInit(&k, (int8_t *)"key", 3);
  z_BufferInit(&v, (int8_t *)"value", 5);
  vv = z_BufferEmpty();

  ret = z_KVInsert(&kv, k, v);
  z_ASSERT_TRUE(ret == z_OK);

  ret = z_KVInsert(&kv, k, v);
  z_ASSERT_TRUE(ret == z_ERR_EXIST);

  ret = z_KVFind(&kv, k, &vv);
  z_ASSERT_TRUE(ret == z_OK);
  z_ASSERT_TRUE(z_BufferIsEqual(vv, v) == true);

  z_BufferReset(&v, (int8_t *)"value1", strlen("value1"));
  z_BufferReset(&vv, (int8_t *)"value", strlen("value"));
  // current = value
  // vv = value
  ret = z_KVUpdate(&kv, k, v, vv);
  z_ASSERT_TRUE(ret == z_OK);

  ret = z_KVFind(&kv, k, &vv);
  z_ASSERT_TRUE(ret == z_OK);
  z_ASSERT_TRUE(z_BufferIsEqual(vv, v) == true);

  z_BufferReset(&vv, (int8_t *)"value", strlen("value"));
  // current = value1
  // vv = value
  ret = z_KVUpdate(&kv, k, v, vv);
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

  z_BufferDestroy(&vv);
  z_BufferDestroy(&v);
  z_BufferDestroy(&k);
  z_KVDestroy(&kv);
}