#include "zutils/log.h"

int main() {
  z_debug("debug %d", 1);
  z_info("info %d", 2);
  z_warning("warning %d", 3);
  z_error("error %d", 4);
  z_panic("panic %d", 5);
  return 1;
}