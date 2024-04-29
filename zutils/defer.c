#ifndef z_DEFER_H
#define z_DEFER_H

#endif

#include "stdio.h"


int main() {

  void (^myBlock)(void) = ^ void (void) {
    printf("This is a block!\n");
  };

  myBlock();

  return 0;
}