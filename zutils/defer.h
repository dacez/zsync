#ifndef z_DEFER_H
#define z_DEFER_H

#include <assert.h>

#include "zutils/macro.h"

static inline void z_defer_cleanup(void (^*b)(void)) { (*b)(); }
#define z_defer_var(c)                                                         \
  [[gnu::cleanup(z_defer_cleanup)]] void (^z_cat(z_defer_var_, c))(void)

#define z_defer_func0(c, f)                                                    \
  z_defer_var(c) = ^{                                                          \
    f();                                                                       \
  }

#define z_defer_func1(c, f, a1)                                                \
  auto z_cat(z_defer_fn_var, 1, c) = a1;                                       \
  z_defer_var(c) = ^{                                                          \
    f(z_cat(z_defer_fn_var, 1, c));                                            \
  }

#define z_defer_func2(c, f, a1, a2)                                            \
  auto z_cat(z_defer_fn_var, 1, c) = a1;                                       \
  auto z_cat(z_defer_fn_var, 2, c) = a2;                                       \
  z_defer_var(c) = ^{                                                          \
    f(z_cat(z_defer_fn_var, 1, c), z_cat(z_defer_fn_var, 2, c));               \
  }

#define z_defer0(f) z_defer_func0(__COUNTER__, f)
#define z_defer1(f, a1) z_defer_func1(__COUNTER__, f, a1)
#define z_defer2(f, a1, a2) z_defer_func2(__COUNTER__, f, a1, a2)

#define z_defer(f, ...)                                                        \
  static_assert(z_argc(__VA_ARGS__) < 3);                                      \
  z_cat(z_defer, z_argc(__VA_ARGS__))(f __VA_OPT__(, ) __VA_ARGS__)
#endif

#define z_unique(type) [[gnu::cleanup(z_cat(type, Destroy))]] type