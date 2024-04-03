#ifndef z_UTILS_H
#define z_UTILS_H

#include <stdint.h>

#include <time.h>

uint64_t z_Now() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

void z_LocalTime(char local[32]) {
    time_t now = time(NULL);
    struct tm *lt = localtime(&now);
    strftime(local, 32, "%Y/%m/%d %H:%M:%S", lt);
}

#endif