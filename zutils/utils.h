#ifndef z_UTILS_H
#define z_UTILS_H

#include <stdint.h>
#include <string.h>

#include <time.h>

#include <zutils/utils.h>

uint64_t z_NowNS() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

// Millis
uint64_t z_NowMS() {
    return z_NowNS() / 1000000;
}

uint64_t z_NowS() {
    return z_NowNS() / 1000000000;
}



void z_LocalTime(char local[32]) {
    time_t now = time(NULL);
    struct tm *lt = localtime(&now);
    strftime(local, 32, "%Y/%m/%d %H:%M:%S", lt);
}

#define z_malloc(s) malloc(s);
#define z_free(ptr) {if (ptr == nullptr) {z_panic("ptr == nullptr");}free(ptr);}

#endif