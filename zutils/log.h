#ifndef z_LOG_H
#define z_LOG_H

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "zerror/error.h"
#include "zutils/time.h"

#define z_LOG_LEN 128
#define z_LOG_TAIL_LEN 64

FILE *z_log_file = nullptr;
const char *z_color_yellow = "\033[;33m";
const char *z_color_pink = "\033[;35m";
const char *z_color_red = "\033[;31m";
const char *z_color_end = "\033[;0m";

z_Error z_LogInit(char *path) {
  z_log_file = fopen(path, "a");
  if (z_log_file == nullptr) {
    return z_ERR_SYS;
  }

  z_color_yellow = "";
  z_color_pink = "";
  z_color_red = "";
  z_color_end = "";

  return z_OK;
}

void z_LogDestory() { fclose(z_log_file); }

FILE *z_LogFile() {
  if (z_log_file == nullptr) {
    z_log_file = stdout;
  }

  return z_log_file;
}

#ifndef NDEBUG
#define z_debug(...)                                                           \
  {                                                                            \
    char time_str[32];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str), "%s [debug] %s:%d:%s\n", time_str,      \
             __FILE__, __LINE__, tail_str);                                    \
    fprintf(z_LogFile(), "%s", log_str);                                       \
  }
#else
#define z_debug(...)
#endif

#define z_info(...)                                                            \
  {                                                                            \
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
  {                                                                            \
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
  {                                                                            \
    char time_str[32];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    if (errno != 0) {                                                          \
      snprintf(log_str, sizeof(log_str), "%s %s [error] %s:%d:%s:%s %s\n",     \
               z_color_pink, time_str, __FILE__, __LINE__, tail_str,           \
               strerror(errno), z_color_end);                                  \
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
               "%s %s [panic] %s:%d:%s:%s exit %s \n", z_color_red, time_str,  \
               __FILE__, __LINE__, tail_str, strerror(errno), z_color_end);    \
    } else {                                                                   \
      snprintf(log_str, sizeof(log_str), "%s %s [panic] %s:%d:%s exit %s \n",  \
               z_color_red, time_str, __FILE__, __LINE__, tail_str,            \
               z_color_end);                                                   \
    }                                                                          \
    fprintf(z_LogFile(), "%s", log_str);                                       \
    exit(EXIT_FAILURE);                                                        \
  }

#endif