#include "zkv/kv.h"
#include "ztest/test.h"

int z_KVTest() {

  z_KV kv;
  z_Error ret =  z_KVInit(&kv, "./bin/binlog.log", 1024*1024*1024, 1024);
  z_ASSERT(ret == z_OK);

  z_Buffer k = {.Data = (int8_t*)"key", .Len = strlen("key")};
  z_Buffer v = {.Data = (int8_t*)"value", .Len = strlen("value")};
  ret = z_KVInsert(&kv, k, v);
  z_ASSERT(ret == z_OK);

  ret = z_KVInsert(&kv, k, v);
  z_ASSERT(ret == z_ERR_EXIST);

  z_Buffer vv = {};
  ret = z_KVFind(&kv, k, &vv);
  z_ASSERT(ret == z_OK);
  z_ASSERT(z_BufferIsEqual(vv, v) == true);
  z_BufferDestory(&vv);

  z_BufferReset(&v, (int8_t*)"valuevalue", strlen("valuevalue"));
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

  z_KVDestory(&kv);
  return 0;
}