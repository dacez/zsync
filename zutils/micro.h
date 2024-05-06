#ifndef z_MICRO_H
#define z_MICRO_H

#define z_seq(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14,     \
              _15, _16, _17, _18, _19, _20, _21, _22, _23, _24, _25, _26, _27, \
              _28, _29, _30, N, ...)                                           \
  N
#define z_argc(...)                                                            \
  z_seq(__VA_ARGS__ __VA_OPT__(, ) 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, \
        19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define z_merge0(_1) _1##0
#define z_merge1(_1, _2) _1##_2
#define z_merge2(_1, _2, _3) _1##_2##_3
#define z_merge3(_1, _2, _3, _4) _1##_2##_3##_4
#define z_merge4(_1, _2, _3, _4, _5) _1##_2##_3##_4##_5
#define z_merge5(_1, _2, _3, _4, _5, _6) _1##_2##_3##_4##_5##_6
#define z_merge6(_1, _2, _3, _4, _5, _6, _7) _1##_2##_3##_4##_5##_6##_7

#define z_inner_merge2(x, y) x##y
#define z_inner_merge(x, y) z_inner_merge2(x, y)
#define z_cat(str, ...) z_inner_merge(z_merge, z_argc(__VA_ARGS__))(str __VA_OPT__(, ) __VA_ARGS__)
#endif