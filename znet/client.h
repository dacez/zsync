#ifndef z_CLIENT_H
#define z_CLIENT_H
#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>

#include "zerror/error.h"
#include "znet/net_record.h"
#include "zrecord/record.h"
#include "zutils/buffer.h"
#include "zutils/log.h"
#include "zutils/mem.h"
#include "znet/socket.h"
#include "zutils/lock.h"
#include "zutils/defer.h"
#include "zutils/hash.h"

typedef struct {
  z_Lock Lock;
  int64_t Socket;
} z_Conn;

typedef struct {
  char IP[z_IP_MAX_LEN];
  uint16_t Port;
  z_Conn *Conns;
  int64_t ConnsLen;
} z_Cli;

z_Error z_CliInit(z_Cli *cli, const char *ip, uint16_t port, int64_t conns_len) {
  if (cli == nullptr || ip == nullptr || port == 0 || conns_len == 0) {
    z_error("cli == nullptr || ip == nullptr || port == 0 || conns_len == 0");
    return z_ERR_INVALID_DATA;
  }

  memset(cli->IP, 0, z_IP_MAX_LEN);
  strncpy(cli->IP, ip, z_IP_MAX_LEN - 1);
  cli->Port = port; 
  cli->ConnsLen = conns_len;

  cli->Conns = z_malloc(sizeof(z_Conn) * cli->ConnsLen);
  for (int64_t i = 0; i < cli->ConnsLen; ++i) {
    z_LockInit(&cli->Conns[i].Lock);
    cli->Conns[i].Socket = z_INVALID_SOCKET;
  }
  return z_OK;
}

void z_CliConnectDestory(z_Cli *cli, int64_t i) {
  if (cli->Conns[i].Socket == z_INVALID_SOCKET) {
    return;
  }
  
  close(cli->Conns[i].Socket);
  cli->Conns[i].Socket = z_INVALID_SOCKET;
}

z_Error z_CliConnect(z_Cli *cli, int64_t i) {
  if (cli->Conns[i].Socket != z_INVALID_SOCKET) {
    return z_OK;
  }

  cli->Conns[i].Socket = socket(AF_INET, SOCK_STREAM, 0);
  if (cli->Conns[i].Socket < 0) {
    z_error("cli->Conns[i].Socket < 0 %lld", cli->Conns[i].Socket);
    return z_ERR_NET;
  }

  ipv4_addr svr_ip;
  memset(&svr_ip, 0, sizeof(svr_ip));
  svr_ip.sin_family = AF_INET;
  svr_ip.sin_addr.s_addr = inet_addr(cli->IP);
  svr_ip.sin_port = htons(cli->Port);

  if (connect(cli->Conns[i].Socket, (sock_addr *)&svr_ip, sizeof(ipv4_addr)) != 0) {
    z_error("connect failed %s:%u", cli->IP, cli->Port);
    return z_ERR_NET;
  }

  return z_OK;
}

z_Error z_CliCall(z_Cli *cli, z_Req *req, z_Resp *resp) {
  z_ConstBuffer key;
  z_Error ret = z_RecordKey(req->Record, &key);
  if (ret != z_OK) {
    return ret;
  }
  uint64_t hash = z_Hash(key.Data, key.Len);
  int64_t i = hash % cli->ConnsLen;

  z_LockLock(&cli->Conns[i].Lock);
  z_defer(z_LockUnLock, &cli->Conns[i].Lock);

  ret = z_CliConnect(cli, i);
  if (ret != z_OK) {
    z_CliConnectDestory(cli, i);
    return ret;
  }

  ret = z_ReqToNet(cli->Conns[i].Socket, req);
  if (ret != z_OK) {
    z_CliConnectDestory(cli, i);
    return ret;
  }

  ret = z_RespInitFromNet(cli->Conns[i].Socket, resp);
  if (ret != z_OK) {
    z_CliConnectDestory(cli, i);
    return ret;
  }
  
  return z_OK;
}

void z_CliDestroy(z_Cli *cli) {
  if (cli == nullptr) {
    return;
  }

  if (cli->Conns == nullptr) {
    return;
  }

  for (int64_t i = 0;i < cli->ConnsLen; ++i) {
    z_LockDestroy(&cli->Conns[i].Lock);
    z_CliConnectDestory(cli, i);
  }

  z_free(cli->Conns);
}
#endif