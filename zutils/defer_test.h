#include "ztest/test.h"
#include "zutils/defer.h"

int arg = -1;

void func0() { arg = 0; }
void func1(int a1) { arg = 1; }
void func2(int a1, int a2) { arg = 2; }

void z_DeferTest() {

  int a1, a2;
  { z_defer(func1, a1); }
  z_ASSERT(arg == 1);
  { z_defer(func2, a1, a2); }
  z_ASSERT(arg == 2);

  {
    z_defer(
        ^(int) {
          arg = 4;
        },
        a1);
  }
  z_ASSERT(arg == 4);

  int a = 5;
  int aa = 6;
  {
    z_defer(^(int *a, int aa) { *a = aa;}, &a, aa);
  }
  z_ASSERT(a == 6);
}