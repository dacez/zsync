#ifndef z_CLIENT_H
#define z_CLIENT_H
#include <stdint.h>

typedef struct {
  char IP[64];
  uint16_t Port;
} z_Cli;
#endif