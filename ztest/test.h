#ifndef z_TEST_H
#define z_TEST_H
#include <stdio.h> // IWYU pragma: export
#include <stdlib.h>

#include "zutils/time.h" // IWYU pragma: export
#include <stdatomic.h>

static atomic_int_fast64_t z_test_succ_count;
static atomic_int_fast64_t z_test_fail_count;
static int64_t z_test_start_time_ms;

#define z_ASSERT_TRUE(e)                                                       \
  {                                                                            \
    if (!(e)) {                                                                \
      atomic_fetch_add(&z_test_fail_count, 1);                                 \
      printf("\033[;31m"                                                       \
             "[FAIL] %s:%d:[%s] exit"                                          \
             "\033[;0m\n",                                                     \
             __FILE__, __LINE__, #e);                                          \
      exit(EXIT_FAILURE);                                                      \
    } else {                                                                   \
      atomic_fetch_add(&z_test_succ_count, 1);                                 \
      printf("\033[;32m"                                                       \
             "[SUCC] %s:%d:[%s]"                                               \
             "\033[;0m\n",                                                     \
             __FILE__, __LINE__, #e);                                          \
    }                                                                          \
  }

#define z_TEST_START()                                                         \
  {                                                                            \
    atomic_store(&z_test_succ_count, 0);                                       \
    atomic_store(&z_test_fail_count, 0);                                       \
    z_test_start_time_ms = z_NowMS();                                          \
  }

#define z_TEST_END()                                                           \
  {                                                                            \
    int64_t succ_count = atomic_load(&z_test_succ_count);                      \
    int64_t fail_count = atomic_load(&z_test_fail_count);                      \
    printf(                                                                    \
        "\033[;32m[All Finished] Succ:%lld Fail:%lld Time:%lldms\n\033[;0m",   \
        succ_count, fail_count, z_NowMS() - z_test_start_time_ms);             \
  }
#endif