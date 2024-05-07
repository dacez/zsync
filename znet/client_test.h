
#include "zerror/error.h"
#include "znet/client.h"
#include "znet/net_record.h"
#include "zrecord/record.h"
#include "ztest/test.h"
#include "zutils/buffer.h"
#include "zutils/defer.h"
#include <stdint.h>
#include <string.h>


void z_ClientTest() {
  int64_t count = 1000;

  z_Cli cli = {};
  z_Error ret = z_CliInit(&cli, "127.0.0.1", 12301, 16);
  z_ASSERT_TRUE(ret == z_OK);

  for (int64_t i = 0; i < count; ++i) {
    z_Req req = {};
    z_Resp resp = {};
    z_Buffer k = {};
    z_Buffer v = {};
    z_defer(z_ReqDestory, &req);
    z_defer(z_RespDestory, &resp);
    z_defer(z_BufferDestroy, &k);
    z_defer(z_BufferDestroy, &v);

    char key[32] = {};
    char val[32] = {};
    sprintf(key, "key%lld", i);
    sprintf(val, "value%lld", i);

    ret = z_BufferInit(&k, (int8_t*)key, strlen(key));
    if (ret != z_OK) {
      break;
    }
    ret = z_BufferInit(&v, (int8_t*)val, strlen(val));
    if (ret != z_OK) {
      break;
    }

    req.Record = z_RecordNew(z_ROP_INSERT, k, v);
    if (ret != z_OK) {
      break;
    }

    ret = z_CliCall(&cli, &req, &resp);
    if (ret != z_OK) {
      break;
    }
  }

  z_ASSERT_TRUE(ret == z_OK);
}