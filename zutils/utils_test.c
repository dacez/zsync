#include "utils.h"
#include "zlog/log.h"

int main() {
  z_info("TimeStamp Nanos : %llu", z_Now());
  char dt[32] = {};
  z_LocalTime(dt);
  z_info("DateTime %s", dt);
}