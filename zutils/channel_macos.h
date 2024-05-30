#ifndef z_CHANNEL_MACOS_H
#define z_CHANNEL_MACOS_H
#include <stdint.h>

#include <sys/event.h>
#include <unistd.h>

#include "zerror/error.h"
#include "zutils/assert.h"
#include "zutils/channel_interface.h"
#include "zutils/log.h"
#include "zutils/mem.h"
#include "zutils/defer.h"

bool z_EventIsEnd(z_Event *e) { return e->Flag & EV_EOF; }

z_Error z_ChannelInit(z_Channel *ch) {
  z_assert(ch != nullptr);

  ch->CH = kqueue();
  if (ch->CH < 0) {
    z_error("kqueue failed %lld", ch->CH);
    return z_ERR_NET;
  }
  return z_OK;
}

z_Error z_ChannelSubscribe(z_Channel *ch, int64_t fd) {
  z_assert(ch != nullptr);

  struct kevent ev;
  EV_SET(&ev, fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
  if (kevent(ch->CH, &ev, 1, NULL, 0, NULL) < 0) {
    z_error("registering client socket for read events failed");
    return z_ERR_NET;
  }
  return z_OK;
}

void z_ChannelUnsubscribe(z_Channel *ch, int64_t fd) {
  z_assert(ch != nullptr);

  struct kevent ev;
  EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
  if (kevent(ch->CH, &ev, 1, NULL, 0, NULL) < 0) {
    z_error("delete client socket for read events failed");
    return;
  }
  return;
}

z_Error z_ChannelWait(z_Channel *ch, z_Event *events, int64_t events_len, int64_t *events_count,
                      int64_t timeout_ms) {
  z_assert(ch != nullptr);

  struct kevent *es = z_malloc(sizeof(struct kevent) * events_len);
  z_defer(
      ^(struct kevent **ptr) {
        z_free(*ptr);
      },
      &es);
  if (es == nullptr) {
    z_error("es == nullptr");
    return z_ERR_NOSPACE;
  }

  struct timespec ts = {.tv_sec = timeout_ms / 1000,
                        .tv_nsec = (timeout_ms % 1000) * 1000000};
  *events_count = kevent(ch->CH, NULL, 0, es, events_len, &ts);
  if (*events_count < 0) {
    z_error("kevent failed");
    return z_ERR_NET;
  }

  if (*events_count == 0) {
    z_debug("timeout");
    return z_ERR_TIMEOUT;
  }

  for (int64_t i = 0; i < *events_count; ++i) {
    events[i].FD = es[i].ident;
    events[i].Flag = es[i].flags;
  }

  return z_OK;
}

void z_ChannelDestroy(z_Channel *ch) {
  z_assert(ch != nullptr);

  if (ch != nullptr && ch->CH != z_INVALID_CHANNEL) {
    close(ch->CH);
    ch->CH = z_INVALID_CHANNEL;
  }
}
#endif