#ifndef z_TEST_H
#define z_TEST_H

#include "zlog/log.h"

#define z_ASSERT(e)                                                            \
  {                                                                            \
    if (!(e)) {                                                                \
      printf("\033[;31m"                                                       \
             "[FAIL] %s:%d:[%s] exit\n"                                             \
             "\033[;0m",                                                       \
             __FILE__, __LINE__, #e);                                          \
      exit(EXIT_FAILURE);                                                      \
    } else {                                                                   \
      printf("\033[;32m"                                                       \
             "[SUCC] %s:%d:[%s]\n"                                             \
             "\033[;0m",                                                       \
             __FILE__, __LINE__, #e);                                          \
    }                                                                          \
  }

#define z_CHECK(e)                                                            \
  {                                                                            \
    if (!(e)) {                                                                \
      printf("\033[;31m"                                                       \
             "[FAIL] %s:%d:[%s]\n"                                             \
             "\033[;0m",                                                       \
             __FILE__, __LINE__, #e);                                          \
    } else {                                                                   \
      printf("\033[;32m"                                                       \
             "[SUCC] %s:%d:[%s]\n"                                             \
             "\033[;0m",                                                       \
             __FILE__, __LINE__, #e);                                          \
    }                                                                          \
  }
#endif