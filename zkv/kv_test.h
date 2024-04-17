#include "zkv/kv.h"
#include "ztest/test.h"

int z_KVTest() {

  z_KV kv;
  z_Error ret = z_KVInit(&kv, "./bin/binlog.log", 1024 * 1024 * 1024, 1024);
  z_ASSERT(ret == z_OK);

  z_Buffer k, v, vv;
  z_BufferInit(&k, (int8_t *)"key", 3);
  z_BufferInit(&v, (int8_t *)"value", 5);
  vv = z_BufferEmpty();

  ret = z_KVInsert(&kv, k, v);
  z_ASSERT(ret == z_OK);

  ret = z_KVInsert(&kv, k, v);
  z_ASSERT(ret == z_ERR_EXIST);

  ret = z_KVFind(&kv, k, &vv);
  z_ASSERT(ret == z_OK);
  z_ASSERT(z_BufferIsEqual(vv, v) == true);

  z_BufferReset(&vv, (int8_t *)"value", strlen("value"));
  z_BufferReset(&v, (int8_t *)"value1", strlen("value1"));
  // current = value
  // vv = value
  ret = z_KVUpdate(&kv, k, v, vv);
  z_ASSERT(ret == z_OK);

  ret = z_KVFind(&kv, k, &vv);
  z_ASSERT(ret == z_OK);
  z_ASSERT(z_BufferIsEqual(vv, v) == true);

  z_BufferReset(&vv, (int8_t *)"value", strlen("value"));
  // current = value1
  // vv = value
  ret = z_KVUpdate(&kv, k, v, vv);
  z_ASSERT(ret == z_ERR_CONFLICT);

  ret = z_KVForceUpdate(&kv, k, v);
  z_ASSERT(ret == z_OK);

  ret = z_KVDelete(&kv, k);
  z_ASSERT(ret == z_OK);

  ret = z_KVFind(&kv, k, &vv);
  z_ASSERT(ret == z_ERR_NOT_FOUND);

  ret = z_KVForceUpdate(&kv, k, v);
  z_ASSERT(ret == z_ERR_NOT_FOUND);

  ret = z_KVDelete(&kv, k);
  z_ASSERT(ret == z_ERR_NOT_FOUND);

  z_BufferDestory(&vv);
  z_BufferDestory(&v);
  z_BufferDestory(&k);
  z_KVDestory(&kv);
  return 0;
}