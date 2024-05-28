#ifndef z_FILE_RECORD
#define z_FILE_RECORD

#include <stdint.h>

#include "zrecord/record.h"

typedef struct {
  int64_t Seq;
  z_Record *Record;
} z_FileRecord;


#endif