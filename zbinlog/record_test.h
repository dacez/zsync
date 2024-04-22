#include "ztest/test.h"

#include "zbinlog/record.h"

void z_BinlogRecordTest() {
  z_BinlogRecord *r = z_BinlogRecordNewByLen(sizeof(z_BinlogRecord) - 1);
  z_ASSERT(r == nullptr);
  z_BinlogRecordFree(r);

  r = z_BinlogRecordNewByLen(sizeof(z_BinlogRecord));
  z_ASSERT(r != nullptr);
  z_BinlogRecordFree(r);
}