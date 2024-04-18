#include "ztest/test.h"
#include "zutils/utils.h"

void z_TimeTest() {
  z_CHECK(z_NowMS() > 0 && z_NowNS() > 0 && z_NowS > 0);
  char lt[32] = {};
  z_LocalTime(lt);
  z_CHECK(strlen(lt) > 0);
}