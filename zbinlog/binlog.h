#ifndef z_BINLOG_H
#define z_BINLOG_H

#include <stdint.h>

#include "zbinlog/file.h"
#include "zbinlog/file_record.h"
#include "zerror/error.h"
#include "zutils/lock.h"

typedef z_Error z_BinLogAfterWrite(void *, z_Record *, int64_t);

typedef struct {
  void *Attr;
  z_BinLogAfterWrite *AfterWrite;
  z_Lock Lock;
  z_Writer Writer;
  atomic_int_fast64_t Seq;
} z_BinLog;

void z_BinLogDestroy(z_BinLog *bl) {
  if (bl == nullptr) {
    z_error("bl == nullptr");
    return;
  }

  z_WriterDestroy(&bl->Writer);

  z_LockDestroy(&bl->Lock);
  return;
}

z_Error z_BinLogInit(z_BinLog *bl, char *path, int64_t max_size, void *attr,
                     z_BinLogAfterWrite *after_write) {
  if (bl == nullptr || path == nullptr || max_size == 0 || attr == nullptr ||
      after_write == nullptr) {
    z_error("bl == nullptr || path == nullptr || max_size == 0 || attr == "
            "nullptr || after_write == nullptr");
    return z_ERR_INVALID_DATA;
  }

  z_LockInit(&bl->Lock);

  z_Error ret = z_WriterInit(&bl->Writer, path, max_size);
  if (ret != z_OK) {
    return ret;
  }

  bl->Attr = attr;
  bl->AfterWrite = after_write;
  atomic_store(&bl->Seq, 1);
  return z_OK;
}

z_Error z_BinLogAppendRecord(z_BinLog *bl, z_FileRecord *r) {
  z_LockLock(&bl->Lock);

  z_Error ret = z_OK;
  int64_t offset = 0;

  ret = z_WriterOffset(&bl->Writer, &offset);
  if (ret != z_OK) {
    return ret;
  }
  
  r->Seq = atomic_fetch_add(&bl->Seq, 1);
  z_RecordSum(r->Record);
  ret = z_WriterAppendRecord(&bl->Writer, r);
  if (ret != z_OK) {
    z_LockUnLock(&bl->Lock);
    return ret;
  }

  ret = bl->AfterWrite(bl->Attr, r->Record, offset);
  if (ret != z_OK) {
    z_LockUnLock(&bl->Lock);
    return ret;
  }

  z_LockUnLock(&bl->Lock);
  return ret;
}
#endif