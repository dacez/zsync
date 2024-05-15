#ifndef z_TIME_H
#define z_TIME_H

#include <stdint.h>
#include <string.h>

#include <time.h>

int64_t z_NowNS() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

// Millis
int64_t z_NowMS() { return z_NowNS() / 1000000; }

int64_t z_NowS() { return z_NowNS() / 1000000000; }

void z_LocalTime(char local[16]) {
  time_t now = time(NULL);
  struct tm *lt = localtime(&now);
  strftime(local, 16, "%H:%M:%S", lt);
}

#endif