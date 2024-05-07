#include "ztest/test.h"
#include "zutils/time.h"

void z_TimeTest() {
  z_ASSERT_TRUE(z_NowMS() > 0 && z_NowNS() > 0 && z_NowS > 0);
  char lt[32] = {};
  z_LocalTime(lt);
  z_ASSERT_TRUE(strlen(lt) > 0);
}