#ifndef z_LOG_H
#define z_LOG_H

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "zerror/error.h"
#include "zutils/assert.h"
#include "zutils/time.h" // IWYU pragma: export

#define z_LOG_LEN 128
#define z_LOG_TAIL_LEN 64

FILE *z_log_file = nullptr;
int64_t z_log_level = 1;
const char *z_color_yellow = "\033[;33m";
const char *z_color_pink = "\033[;35m";
const char *z_color_red = "\033[;31m";
const char *z_color_end = "\033[;0m";

FILE *z_LogFile() {
  if (z_log_file == nullptr) {
    z_log_file = stdout;
  }

  return z_log_file;
}

#define z_debug(...)                                                           \
  if (z_log_level <= 1) {                                                      \
    char time_str[32];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str), "%s [debug] %s:%d:%s\n", time_str,      \
             __FILE__, __LINE__, tail_str);                                    \
    fprintf(z_LogFile(), "%s", log_str);                                       \
  }

#define z_info(...)                                                            \
  if (z_log_level <= 2) {                                                      \
    char time_str[32];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str), "%s [info] %s:%d:%s\n", time_str,       \
             __FILE__, __LINE__, tail_str);                                    \
    fprintf(z_LogFile(), "%s", log_str);                                       \
  }

#define z_warning(...)                                                         \
  if (z_log_level <= 3) {                                                      \
    char time_str[32];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str), "%s %s [warning] %s:%d:%s %s\n",        \
             z_color_yellow, time_str, __FILE__, __LINE__, tail_str,           \
             z_color_end);                                                     \
    fprintf(z_LogFile(), "%s", log_str);                                       \
  }

#define z_error(...)                                                           \
  if (z_log_level <= 3) {                                                      \
    char time_str[32];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    if (errno != 0) {                                                          \
      snprintf(log_str, sizeof(log_str),                                       \
               "%s %s [error] %s:%d:%s lastsyserror: %s %s\n", z_color_pink,   \
               time_str, __FILE__, __LINE__, tail_str, strerror(errno),        \
               z_color_end);                                                   \
    } else {                                                                   \
      snprintf(log_str, sizeof(log_str), "%s %s [error] %s:%d:%s %s\n",        \
               z_color_pink, time_str, __FILE__, __LINE__, tail_str,           \
               z_color_end);                                                   \
    }                                                                          \
    fprintf(z_LogFile(), "%s", log_str);                                       \
  }

#define z_panic(...)                                                           \
  {                                                                            \
    char time_str[32];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    if (errno != 0) {                                                          \
      snprintf(log_str, sizeof(log_str),                                       \
               "%s %s [panic] %s:%d:%s lastsyserror: %s exit %s \n",           \
               z_color_red, time_str, __FILE__, __LINE__, tail_str,            \
               strerror(errno), z_color_end);                                  \
    } else {                                                                   \
      snprintf(log_str, sizeof(log_str), "%s %s [panic] %s:%d:%s exit %s \n",  \
               z_color_red, time_str, __FILE__, __LINE__, tail_str,            \
               z_color_end);                                                   \
    }                                                                          \
    fprintf(z_LogFile(), "%s", log_str);                                       \
    exit(EXIT_FAILURE);                                                        \
  }

z_Error z_LogInit(char *path, int64_t log_level) {
  z_assert(path != nullptr, log_level <= 3);

  z_log_level = log_level;

  if (*path == '\0') {
    return z_OK;
  }
  z_log_file = fopen(path, "a");
  if (z_log_file == nullptr) {
    z_panic("z_log_file == nullptr %s", path);
    exit(EXIT_FAILURE);
    return z_ERR_FS;
  }
  z_color_yellow = "";
  z_color_pink = "";
  z_color_red = "";
  z_color_end = "";

  return z_OK;
}

void z_LogDestroy() { fclose(z_log_file); }

#endif