#ifndef z_LOG_H
#define z_LOG_H

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define z_LOG_LEN 128
#define z_LOG_TAIL_LEN 64

#ifdef z_COLOR_LOG
#define z_color_yellow "\033[;33m"
#define z_color_pink "\033[;35m"
#define z_color_red "\033[;31m"
#define z_color_end "\033[;0m"
#else
#define z_color_yellow
#define z_color_pink
#define z_color_red
#define z_color_end
#endif

#ifndef NDEBUG
#define z_debug(...)                                                          \
  {                                                                            \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str), "[debug] %s:%d:%s\n", __FILE__,         \
             __LINE__, tail_str);                                              \
    fprintf(stdout, "%s", log_str);                                            \
  }
#else
#define z_debug(...)
#endif

#define z_info(...)                                                           \
  {                                                                            \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str), "[info] %s:%d:%s\n", __FILE__,          \
             __LINE__, tail_str);                                              \
    fprintf(stdout, "%s", log_str);                                            \
  }

#define z_warning(...)                                                        \
  {                                                                            \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str),                                         \
             z_color_yellow "[warning] %s:%d:%s\n" z_color_end, __FILE__,    \
             __LINE__, tail_str);                                              \
    fprintf(stdout, "%s", log_str);                                            \
  }

#define z_error(...)                                                          \
  {                                                                            \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    snprintf(log_str, sizeof(log_str),                                         \
             z_color_pink "[error] %s:%d:%s\n" z_color_end, __FILE__,        \
             __LINE__, tail_str);                                              \
    fprintf(stdout, "%s", log_str);                                            \
  }

#define z_panic(...)                                                          \
  {                                                                            \
    char log_str[z_LOG_LEN] = {};                                              \
    char tail_str[z_LOG_TAIL_LEN] = {};                                        \
    snprintf(tail_str, sizeof(tail_str), __VA_ARGS__);                         \
    if (errno != 0) {                                                          \
      snprintf(log_str, sizeof(log_str),                                       \
               z_color_red "[panic] %s:%d:%s:%s\n" z_color_end, __FILE__,    \
               __LINE__, tail_str, strerror(errno));                           \
      fprintf(stdout, "%s", log_str);                                          \
    } else {                                                                   \
      snprintf(log_str, sizeof(log_str),                                       \
               z_color_pink "[panic] %s:%d:%s\n" z_color_end, __FILE__,      \
               __LINE__, tail_str);                                            \
      fprintf(stdout, "%s", log_str);                                          \
    }                                                                          \
    exit(EXIT_FAILURE);                                                        \
  }

#endif