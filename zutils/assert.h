#ifndef z_CHECK_H
#define z_CHECK_H

#include <assert.h>

#include "zutils/macro.h"

#define z_force_true1(a0)                                                      \
  if ((a0) != true) {                                                          \
    z_panic(#a0);                                                              \
  }
#define z_force_true2(a0, a1)                                                  \
  z_force_true1(a0);                                                           \
  z_force_true1(a1);
#define z_force_true3(a0, a1, a2)                                              \
  z_force_true2(a0, a1);                                                       \
  z_force_true1(a2);
#define z_force_true4(a0, a1, a2, a3)                                          \
  z_force_true3(a0, a1, a2);                                                   \
  z_force_true1(a3);
#define z_force_true5(a0, a1, a2, a3, a4)                                      \
  z_force_true4(a0, a1, a2, a3);                                               \
  z_force_true1(a4);
#define z_force_true6(a0, a1, a2, a3, a4, a5)                                  \
  z_force_true5(a0, a1, a2, a3, a4);                                           \
  z_force_true1(a5);
#define z_force_true7(a0, a1, a2, a3, a4, a5, a6)                              \
  z_force_true6(a0, a1, a2, a3, a4, a5);                                       \
  z_force_true1(a6);

#ifndef NDEBUG
#define z_assert(...)                                                          \
  static_assert(z_argc(__VA_ARGS__) > 0 && z_argc(__VA_ARGS__) < 8);           \
  z_cat(z_force_true, z_argc(__VA_ARGS__))(__VA_ARGS__);
#else
#define z_assert(...)
#endif
#endif