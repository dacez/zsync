#ifndef z_CLIENT_H
#define z_CLIENT_H
#include <stdint.h>
#include <unistd.h>

#include "zerror/error.h"
#include "znet/kv_proto.h"
#include "zrecord/record.h"
#include "zutils/assert.h"
#include "zutils/buffer.h"
#include "zutils/log.h"
#include "zutils/mem.h"
#include "znet/socket.h"
#include "zutils/lock.h"
#include "zutils/defer.h"
#include "zutils/hash.h"
#include "znet/socket.h"

typedef struct {
  z_Lock Lock;
  z_Socket Socket;
} z_Conn;

typedef struct {
  char IP[z_IP_MAX_LEN];
  uint16_t Port;
  z_Conn *Conns;
  int64_t ConnsLen;
} z_Cli;

z_Error z_CliInit(z_Cli *cli, const char *ip, uint16_t port, int64_t conns_len) {
  z_assert(cli != nullptr, ip != nullptr, port != 0, conns_len != 0);

  memset(cli->IP, 0, z_IP_MAX_LEN);
  strncpy(cli->IP, ip, z_IP_MAX_LEN - 1);
  cli->Port = port; 
  cli->ConnsLen = conns_len;

  cli->Conns = z_malloc(sizeof(z_Conn) * cli->ConnsLen);
  for (int64_t i = 0; i < cli->ConnsLen; ++i) {
    z_LockInit(&cli->Conns[i].Lock);
    cli->Conns[i].Socket.FD = z_INVALID_SOCKET;
  }
  return z_OK;
}

void z_CliConnectClose(z_Cli *cli, int64_t i) {
  z_SocketDestroy(&cli->Conns[i].Socket);
}

z_Error z_CliConnect(z_Cli *cli, int64_t i) {
  if (cli->Conns[i].Socket.FD != z_INVALID_SOCKET) {
    return z_OK;
  }

  z_Error ret = z_SocketCliInit(&cli->Conns[i].Socket, cli->IP, cli->Port);
  if (ret != z_OK) {
    z_error("connect failed %s:%u", cli->IP, cli->Port);
    return z_ERR_NET;
  }

  return z_OK;
}

z_Error z_CliCall(z_Cli *cli, const z_Req *req, z_Resp *resp) {
  z_ConstBuffer key;
  z_Error ret = z_RecordKey((z_Record*)req->Data, &key);
  if (ret != z_OK) {
    z_error("z_RecordKey failed %d", ret);
    return ret;
  }
  uint64_t hash = z_Hash(key.Data, key.Size);
  int64_t i = hash % cli->ConnsLen;

  z_LockLock(&cli->Conns[i].Lock);
  z_defer(z_LockUnLock, &cli->Conns[i].Lock);

  ret = z_CliConnect(cli, i);
  if (ret != z_OK) {
    z_error("z_CliConnect failed %d", ret);
    z_CliConnectClose(cli, i);
    return ret;
  }

  ret = z_ReqToSocket(req, &cli->Conns[i].Socket);
  if (ret != z_OK) {
    z_error("z_ReqToSocket failed %d", ret);
    z_CliConnectClose(cli, i);
    return ret;
  }

  ret = z_RespInitBySocket(resp, &cli->Conns[i].Socket);
  if (ret != z_OK) {
    z_error("z_RespInitBySocket failed %d", ret);
    z_CliConnectClose(cli, i);
    return ret;
  }
  
  return z_OK;
}

void z_CliDestroy(z_Cli *cli) {
  if (cli == nullptr || cli->Conns == nullptr) {
    return;
  }

  for (int64_t i = 0;i < cli->ConnsLen; ++i) {
    z_CliConnectClose(cli, i);
    z_LockDestroy(&cli->Conns[i].Lock);
  }

  z_free(cli->Conns);
  cli->ConnsLen = 0;
}
#endif