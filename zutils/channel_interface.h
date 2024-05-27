#ifndef z_CHANNEL_INTERFACE_H
#define z_CHANNEL_INTERFACE_H

#include <stdint.h>

#include "zerror/error.h"
#include "znet/socket.h"
#include "zutils/assert.h"
#include "zutils/log.h"
#include "zutils/mem.h"

#define z_EVENT_LEN 16
typedef struct {
  int64_t FD;
  int64_t Flag;
} z_Event;
bool z_EventIsEnd(z_Event *e);

typedef struct {
  int64_t CH;
} z_Channel;
z_Error z_ChannelInit(z_Channel *ch);
z_Error z_ChannelSubscribe(z_Channel *ch, int64_t fd);
void z_ChannelUnsubscribe(z_Channel *ch, int64_t fd);
z_Error z_ChannelWait(z_Channel *ch, z_Event *events, int64_t events_len, int64_t *events_count, int64_t timeout_ms);
void z_ChannelDestroy(z_Channel *ch);

z_Error z_ChannelSubscribeSocket(z_Channel *ch, const z_Socket *s) {
  return z_ChannelSubscribe(ch, s->FD);
}

void z_ChannelUnsubscribeSocket(z_Channel *ch, const z_Socket *s) {
  return z_ChannelUnsubscribe(ch, s->FD);
}

typedef struct {
  z_Channel *Chs;
  int64_t Len;
} z_Channels;

z_Error z_ChannelsInit(z_Channels *chs, int64_t len) {
  z_assert(chs != nullptr);

  chs->Len = len;
  chs->Chs = z_malloc(len * sizeof(z_Channel));
  if (chs->Chs == nullptr) {
    z_error("chs->Chs == nullptr");
    return z_ERR_NOSPACE;
  }

  for (int64_t i = 0; i < len; ++i) {
    z_Error ret = z_ChannelInit(&chs->Chs[i]);
    if (ret != z_OK) {
      z_error("z_ChannelInit %d", ret);
      return ret;
    }
  }
  return z_OK;
}

z_Error z_ChannelsGet(z_Channels *chs, int64_t i, z_Channel **ch) {
  z_assert(chs != nullptr);

  if (i >= chs->Len) {
    z_error("i >= chs->Len %lld %lld", i, chs->Len);
    return z_ERR_INVALID_DATA;
  }

  *ch = &chs->Chs[i];
  return z_OK;
}

void z_ChannelsDestroy(z_Channels *chs) {
  z_assert(chs != nullptr);

  if (chs->Chs == nullptr) {
    return;
  }

  for (int64_t i = 0; i < chs->Len; ++i) {
    z_ChannelDestroy(&chs->Chs[i]);
  }

  z_free(chs->Chs);
  chs->Len = 0;
}
#endif