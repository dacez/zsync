#include "ztest/test.h"

#include "zbinlog/record.h"

void z_RecordTest() {
  z_Record *r = z_RecordNewByLen(sizeof(z_Record) - 1);
  z_ASSERT(r == nullptr);
  z_RecordFree(r);

  r = z_RecordNewByLen(sizeof(z_Record));
  z_ASSERT(r != nullptr);
  z_RecordFree(r);
}