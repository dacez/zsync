#ifndef z_TEST_H
#define z_TEST_H
#include <stdio.h> // IWYU pragma: export
#include <stdlib.h>

#define z_ASSERT_TRUE(e)                                                            \
  {                                                                            \
    if (!(e)) {                                                                \
      printf("\033[;31m"                                                       \
             "[FAIL] %s:%d:[%s] exit\n"                                        \
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

#endif