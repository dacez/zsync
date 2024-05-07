#include "ztest/test.h"
#include "zutils/defer.h"

int arg = -1;

void func0() { arg = 0; }
void func1(int a1) { arg = 1; }
void func2(int a1, int a2) { arg = 2; }
void funcptr(int *a1, int a2) { *a1 = a2; }

void z_DeferTest() {

  int a1 = 1, a2 = 2;
  { z_defer(func0); }
  z_ASSERT_TRUE(arg == 0);

  { z_defer(func1, a1); }
  z_ASSERT_TRUE(arg == 1);

  { z_defer(func2, a1, a2); }
  z_ASSERT_TRUE(arg == 2);

  { z_defer(funcptr, &a1, a2); }
  z_ASSERT_TRUE(a1 == a2)

  {
    z_defer(
        ^(int) {
          arg = 4;
        },
        a1);
  }
  z_ASSERT_TRUE(arg == 4);

  a1 = 1;
  a2 = 2;
  {
    z_defer(
        ^(int a1, int *a2) {
          *a2 = a1;
        },
        a1, &a2);
  }
  z_ASSERT_TRUE(a1 == a2);
}