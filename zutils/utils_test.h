#include <string.h>

#include "zutils/utils.h"
#include "zlog/log.h"
#include "ztest/test.h"

void z_UtilsTest() {
  z_CHECK(z_NowMS() > 0 && z_NowNS() > 0 && z_NowS > 0);
  char lt[32] = {};
  z_LocalTime(lt);
  z_CHECK(strlen(lt) > 0);
}