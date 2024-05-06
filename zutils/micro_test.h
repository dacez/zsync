#include "zutils/micro.h"
#include "ztest/test.h"

#define z_test0 0
#define z_test1 1
#define z_test12 2
#define z_test123 3
#define z_test1234 4
#define z_test12345 5
#define z_test123456 6

void z_MicroTest() {
  z_ASSERT(z_argc() == 0);
  z_ASSERT(z_argc(1) == 1);
  z_ASSERT(z_argc(2, 22) == 2);
  z_ASSERT(z_argc(3, 33, 333) == 3);
  z_ASSERT(z_argc(4, 44, 444, 4444) == 4);
  z_ASSERT(z_argc(55, 55, 55, 55, 55) == 5);
  z_ASSERT(z_argc(1, 2, 3, 4, 5, 6) == 6);
  z_ASSERT(z_argc(1, 2, 3, 4, 5, 6, 7) == 7);
  z_ASSERT(z_argc(1, 2, 3, 4, 5, 6, 7, 108) == 8);
  z_ASSERT(z_argc(1, 2, 3, 4, 5, 6, 7, 888, 9) == 9);
  z_ASSERT(z_argc() == 0);

  int a = -1;
  a = z_cat(z_test);
  z_ASSERT(a == 0);
  a = z_cat(z_test, 1);
  z_ASSERT(a == 1);
  a = z_cat(z_test, 1, 2);
  z_ASSERT(a == 2);
  a = z_cat(z_test, 1, 2, 3);
  z_ASSERT(a == 3);
  a = z_cat(z_test, 1, 2, 3, 4);
  z_ASSERT(a == 4);
  a = z_cat(z_test, 1, 2, 3, 4, 5);
  z_ASSERT(a == 5);
  a = z_cat(z_test, 1, 2, 3, 4, 5, 6);
  z_ASSERT(a == 6);
}