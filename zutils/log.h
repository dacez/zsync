#ifndef z_LOG_H
#define z_LOG_H

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "zerror/error.h"
#include "zutils/threads.h"
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
    char time_str[16];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str), "%s (%lld) [debug] %s:%d:%s\n",         \
             time_str, z_ThreadID(), __FILE__, __LINE__, tail_str);            \
    fprintf(z_LogFile(), "%s", log_str);                                       \
  }

#define z_info(...)                                                            \
  if (z_log_level <= 2) {                                                      \
    char time_str[16];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str), "%s (%lld) [info] %s:%d:%s\n",          \
             time_str, z_ThreadID(), __FILE__, __LINE__, tail_str);            \
    fprintf(z_LogFile(), "%s", log_str);                                       \
  }

#define z_warning(...)                                                         \
  if (z_log_level <= 3) {                                                      \
    char time_str[16];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str), "%s %s (%lld) [warning] %s:%d:%s %s\n", \
             z_color_yellow, time_str, z_ThreadID(), __FILE__, __LINE__,       \
             tail_str, z_color_end);                                           \
    fprintf(z_LogFile(), "%s", log_str);                                       \
  }

#define z_error(...)                                                           \
  if (z_log_level <= 3) {                                                      \
    char time_str[16];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    if (errno != 0) {                                                          \
      snprintf(log_str, sizeof(log_str),                                       \
               "%s %s (%lld) [error] %s:%d:%s lastsyserror: %s %s\n",          \
               z_color_pink, time_str, z_ThreadID(), __FILE__, __LINE__,       \
               tail_str, strerror(errno), z_color_end);                        \
    } else {                                                                   \
      snprintf(log_str, sizeof(log_str), "%s %s (%lld) [error] %s:%d:%s %s\n", \
               z_color_pink, time_str, z_ThreadID(), __FILE__, __LINE__,       \
               tail_str, z_color_end);                                         \
    }                                                                          \
    fprintf(z_LogFile(), "%s", log_str);                                       \
  }

#define z_panic_color(color, color_end, fd, ...)                               \
  {                                                                            \
    char time_str[16];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    if (errno != 0) {                                                          \
      snprintf(                                                                \
          log_str, sizeof(log_str),                                            \
          color                                                                \
          " %s (%lld) [panic] %s:%d:%s lastsyserror: %s exit\n" color_end,     \
          time_str, z_ThreadID(), __FILE__, __LINE__, tail_str,                \
          strerror(errno));                                                    \
    } else {                                                                   \
      snprintf(log_str, sizeof(log_str),                                       \
               color " %s (%lld) [panic] %s:%d:%s exit\n" color_end, time_str, \
               z_ThreadID(), __FILE__, __LINE__, tail_str);                    \
    }                                                                          \
    fprintf(fd, "%s", log_str);                                                \
  }

#define z_panic(...)                                                           \
  {                                                                            \
    if (z_LogFile() != stdout) {                                               \
      z_panic_color("", "", z_LogFile(), __VA_ARGS__);                         \
    }                                                                          \
    z_panic_color("\033[;31m", "\033[;0m", stdout, __VA_ARGS__);               \
    exit(1);                                                                   \
  }

z_Error z_LogInit(char *path, int64_t log_level) {

  if (path == nullptr || log_level > 3) {
    z_panic("path == nullptr || log_level(%lld) > 3", log_level);
  }

  z_log_level = log_level;
  if (*path == '\0') {
    return z_OK;
  }
  z_log_file = fopen(path, "a");
  if (z_log_file == nullptr) {
    z_panic("z_log_file == nullptr %s", path);
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