#ifndef z_LOG_H
#define z_LOG_H

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "zutils/utils.h"

#define z_LOG_LEN 128
#define z_LOG_TAIL_LEN 64

#define z_color_yellow "\033[;33m"
#define z_color_pink "\033[;35m"
#define z_color_red "\033[;31m"
#define z_color_end "\033[;0m"

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
    fprintf(stdout, "%s", log_str);                                            \
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
    fprintf(stdout, "%s", log_str);                                            \
  }

#define z_warning(...)                                                         \
  {                                                                            \
    char time_str[32];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str),                                         \
             z_color_yellow "%s [warning] %s:%d:%s\n" z_color_end, time_str,   \
             __FILE__, __LINE__, tail_str);                                    \
    fprintf(stdout, "%s", log_str);                                            \
  }

#define z_error(...)                                                           \
  {                                                                            \
    char time_str[32];                                                         \
    z_LocalTime(time_str);                                                     \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    if (errno != 0) {                                                          \
      snprintf(log_str, sizeof(log_str),                                       \
               z_color_pink "%s [error] %s:%d:%s:%s\n" z_color_end, time_str,  \
               __FILE__, __LINE__, tail_str, strerror(errno));                 \
    } else {                                                                   \
      snprintf(log_str, sizeof(log_str),                                       \
               z_color_pink "%s [error] %s:%d:%s\n" z_color_end, time_str,     \
               __FILE__, __LINE__, tail_str);                                  \
    }                                                                          \
    fprintf(stdout, "%s", log_str);                                            \
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
               z_color_red "%s [panic] %s:%d:%s:%s exit\n" z_color_end,        \
               time_str, __FILE__, __LINE__, tail_str, strerror(errno));       \
    } else {                                                                   \
      snprintf(log_str, sizeof(log_str),                                       \
               z_color_red "%s [panic] %s:%d:%s exit\n" z_color_end, time_str, \
               __FILE__, __LINE__, tail_str);                                  \
    }                                                                          \
    fprintf(stdout, "%s", log_str);                                            \
    exit(EXIT_FAILURE);                                                        \
  }

#endif