#include <stdint.h>
#include <string.h>

#include "zerror/error.h"
#include "znet/client.h"
#include "znet/net_record.h"
#include "zrecord/record.h"
#include "ztest/test.h"
#include "zutils/buffer.h"
#include "zutils/defer.h"

z_Error z_InsertTest(z_Cli *cli, int64_t i) {
  z_unique(z_Req) req = {};
  z_unique(z_Resp) resp = {};

  char key[32] = {};
  char val[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(val, "value%lld", i);

  z_ConstBuffer k = {.Data = key, .Len = strlen(key)};
  z_ConstBuffer v = {.Data = val, .Len = strlen(val)};

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

z_Error z_DeleteTest(z_Cli *cli, int64_t i) {
  z_unique(z_Req) req = {};
  z_unique(z_Resp) resp = {};

  char key[32] = {};
  sprintf(key, "key%lld", i);

  z_ConstBuffer k = {.Data = key, .Len = strlen(key)};
  z_ConstBuffer v = {};

  req.Record = z_RecordNewByKV(z_ROP_DELETE, k, v);
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

z_Error z_BlindUpdateTest(z_Cli *cli, int64_t i, int64_t ii) {
  z_unique(z_Req) req = {};
  z_unique(z_Resp) resp = {};

  char key[32] = {};
  char val[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(val, "value%lld", ii);

  z_ConstBuffer k = {.Data = key, .Len = strlen(key)};
  z_ConstBuffer v = {.Data = val, .Len = strlen(val)};

  req.Record = z_RecordNewByKV(z_ROP_FORCE_UPDATE, k, v);
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

z_Error z_UpdateTest(z_Cli *cli, int64_t i, int64_t ii, int64_t src_i) {
  z_unique(z_Req) req = {};
  z_unique(z_Resp) resp = {};

  char key[32] = {};
  char val[32] = {};
  char src_val[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(val, "value%lld", ii);
  sprintf(src_val, "value%lld", src_i);

  z_ConstBuffer k = {.Data = key, .Len = strlen(key)};
  z_ConstBuffer v = {.Data = val, .Len = strlen(val)};
  z_ConstBuffer src_v = {.Data = src_val, .Len = strlen(src_val)};

  req.Record = z_RecordNewByKVV(z_ROP_UPDATE, k, v, src_v);
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

z_Error z_FindTest(z_Cli *cli, int64_t i, int64_t ii) {
  z_unique(z_Req) req = {};
  z_unique(z_Resp) resp = {};

  char key[32] = {};
  char val[32] = {};
  sprintf(key, "key%lld", i);
  sprintf(val, "value%lld", ii);

  z_ConstBuffer k = {.Data = key, .Len = strlen(key)};
  z_ConstBuffer v = {.Data = val, .Len = strlen(val)};

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

typedef struct {
  int64_t Start;
  int64_t End;
  z_Thread Tid;
} z_ClientTestArgs;

void z_ClientTest(z_ClientTestArgs *args) {

  z_unique(z_Cli) cli = {};
  z_Error ret = z_CliInit(&cli, "127.0.0.1", 12301, 16);
  z_ASSERT_TRUE(ret == z_OK);

  for (int64_t i = args->Start; i < args->End; ++i) {
    ret = z_InsertTest(&cli, i);
    if (ret != z_OK) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_OK);

  for (int64_t i = args->Start; i < args->End; ++i) {
    ret = z_InsertTest(&cli, i);
    if (ret != z_ERR_EXIST) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_ERR_EXIST);

  for (int64_t i = args->Start; i < args->End; ++i) {
    ret = z_FindTest(&cli, i, i);
    if (ret != z_OK) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_OK);

  for (int64_t i = args->Start; i < args->End; ++i) {
    int64_t reverse = args->End - args->Start - i - 1;
    ret = z_UpdateTest(&cli, i, i, reverse);
    if (ret != z_ERR_CONFLICT) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_ERR_CONFLICT);

  for (int64_t i = args->Start; i < args->End; ++i) {
    int64_t reverse = args->End - args->Start - i - 1;
    ret = z_UpdateTest(&cli, i, reverse, i);
    if (ret != z_OK) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_OK);

  for (int64_t i = args->Start; i < args->End; ++i) {
    int64_t reverse = args->End - args->Start - i - 1;
    ret = z_FindTest(&cli, i, reverse);
    if (ret != z_OK) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_OK);

  for (int64_t i = args->Start; i < args->End; ++i) {
    int64_t reverse = args->End - args->Start - i - 1;
    ret = z_BlindUpdateTest(&cli, i, i);
    if (ret != z_OK) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_OK);

  for (int64_t i = args->Start; i < args->End; ++i) {
    int64_t reverse = args->End - args->Start - i - 1;
    ret = z_FindTest(&cli, i, i);
    if (ret != z_OK) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_OK);

  for (int64_t i = args->Start; i < args->End; ++i) {
    int64_t reverse = args->End - args->Start - i - 1;
    ret = z_DeleteTest(&cli, i);
    if (ret != z_OK) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_OK);

  for (int64_t i = args->Start; i < args->End; ++i) {
    ret = z_DeleteTest(&cli, i);
    if (ret != z_ERR_NOT_FOUND) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_ERR_NOT_FOUND);

  for (int64_t i = args->Start; i < args->End; ++i) {
    ret = z_FindTest(&cli, i, i);
    if (ret != z_ERR_NOT_FOUND) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_ERR_NOT_FOUND);

  for (int64_t i = args->Start; i < args->End; ++i) {
    ret = z_InsertTest(&cli, i);
    if (ret != z_OK) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_OK);

  for (int64_t i = args->Start; i < args->End; ++i) {
    int64_t reverse = args->End - args->Start - i - 1;
    ret = z_FindTest(&cli, i, i);
    if (ret != z_OK) {
      break;
    }
  }
  z_ASSERT_TRUE(ret == z_OK);
}