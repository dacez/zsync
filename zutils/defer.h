#ifndef z_DEFER_H
#define z_DEFER_H

static inline void z_defer_cleanup(void (^*b)(void)) { (*b)(); }
#define z_defer_merge(a, b) a##b
#define z_defer_var_name(a) z_defer_merge(z_defer_var_, a)
#define z_defer_var(c)                                                         \
  [[gnu::cleanup(z_defer_cleanup)]] void (^z_defer_var_name(c))(void)

#define z_defer_func0(c, f)                                                    \
  z_defer_var(c) = ^{                                                          \
    f();                                                                       \
  }

#define z_defer_func1(c, f, a1)                                                \
  auto z_defer_merge(z_defer_func1_var1, c) = a1;                              \
  z_defer_var(c) = ^{                                                          \
    f(z_defer_merge(z_defer_func1_var1, c));                                   \
  }

#define z_defer_func2(c, f, a1, a2)                                            \
  auto z_defer_merge(z_defer_func2_var1, c) = a1;                              \
  auto z_defer_merge(z_defer_func2_var2, c) = a2;                              \
  z_defer_var(c) = ^{                                                          \
    f(z_defer_merge(z_defer_func2_var1, c),                                    \
      z_defer_merge(z_defer_func2_var2, c));                                   \
  }

#define z_defer0(f) z_defer_func0(__COUNTER__, f)
#define z_defer1(f, a1) z_defer_func1(__COUNTER__, f, a1)
#define z_defer2(f, a1, a2) z_defer_func2(__COUNTER__, f, a1, a2)

#define z_defer_select(_0, _1, selected, ...) selected
#define z_defer(f, ...)                                                        \
  z_defer_select(__VA_ARGS__, z_defer2, z_defer1, z_defer0)(f, __VA_ARGS__)

#endif
