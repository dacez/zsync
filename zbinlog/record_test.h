#include "ztest/test.h"

#include "zbinlog/record.h"

void z_FileRecordTest() {
  z_FileRecord *r = z_FileRecordNewByLen(sizeof(z_FileRecord) - 1);
  z_ASSERT(r == nullptr);
  z_FileRecordFree(r);

  r = z_FileRecordNewByLen(sizeof(z_FileRecord));
  z_ASSERT(r != nullptr);
  z_FileRecordFree(r);
}