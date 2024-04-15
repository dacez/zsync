#include "ztest/test.h"

#include "zbinlog/record_test.h"
#include "zutils/utils_test.h"

int main() {
  z_LogInit("./bin/log.txt");

  z_ASSERT(1);
  z_CHECK(1);
  z_RecordTest();
  z_UtilsTest();

  z_LogDestory();
}