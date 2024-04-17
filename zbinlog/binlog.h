#ifndef z_BINLOG_H
#define z_BINLOG_H

#include <pthread.h>

#include "zerror/error.h"
#include "zutils/utils.h"

#include "zbinlog/binlogfile.h"

typedef z_Error z_BinLogAfterWrite(void *, z_Record *, int64_t);

typedef struct {
  void *Attr;
  z_BinLogAfterWrite *AfterWrite;
  pthread_mutex_t Mtx;
  z_BinLogFileWriter Writer;
} z_BinLog;

void z_BinLogDestory(z_BinLog *bl) {
  if (bl == nullptr) {
    z_error("bl == nullptr");
    return;
  }

  z_BinLogFileWriterDestory(&bl->Writer);

  if (pthread_mutex_destroy(&bl->Mtx) != 0) {
    z_error("pthread_mutex_destroy");
    return;
  }

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

  if (pthread_mutex_init(&bl->Mtx, nullptr) != 0) {
    z_error("pthread_mutex_init");
    return z_ERR_SYS;
  }

  z_Error ret = z_BinLogFileWriterInit(&bl->Writer, path, max_size);
  if (ret != z_OK) {
    return ret;
  }

  bl->Attr = attr;
  bl->AfterWrite = after_write;
  return z_OK;
}

z_Error z_BinLogAppendRecord(z_BinLog *bl, z_Record *r) {
  if (pthread_mutex_lock(&bl->Mtx) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  z_Error ret = z_OK;
  int64_t offset = 0;
  if (z_BinLogFileWriterAppendRecord(&bl->Writer, r, &offset) == z_OK) {
    ret = bl->AfterWrite(bl->Attr, r, offset);
    if (ret != z_OK) {
      z_error("AfterWrite");
    }
  }

  if (pthread_mutex_unlock(&bl->Mtx) != 0) {
    z_error("pthread_mutex_unlock");
    return z_ERR_SYS;
  }
  return ret;
}

#endif