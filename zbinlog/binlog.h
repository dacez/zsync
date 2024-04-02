#ifndef z_BINLOG_H
#define z_BINLOG_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>

#include "zlog/log.h"
#include "ztypes/types.h"

#include "record.h"

typedef struct {
  uint64_t MaxSize;
  uint64_t CurSize;
  pthread_mutex_t Mnt;
  FILE *File;
} z_BinLogFileWriter;

z_BinLogFileWriter *z_BinLogFileWriterNew(char *path, uint64_t max_size) {
  z_BinLogFileWriter *wr = malloc(sizeof(z_BinLogFileWriter));
  wr->MaxSize = max_size;
  wr->CurSize = 0;
  int ret = pthread_mutex_init(&wr->Mnt, nullptr);
  if (ret != 0) {
    z_panic("pthread_mutex_init");
  }

  wr->File = fopen(path, "a");
  if (wr->File == nullptr) {
    z_panic("fopen %s", path);
  }
  return wr;
}

void z_BinLogFileWriterFree(z_BinLogFileWriter *wr) {
  int ret = pthread_mutex_destroy(&wr->Mnt);
  if (ret != 0) {
    z_panic("pthread_mutex_destroy");
  }

  ret = fclose(wr->File);
  if (ret != 0) {
    z_panic("fclose");
  }

  free(wr);
}

z_Error z_BinLogFileWriterWrite(z_BinLogFileWriter *wr, uint8_t *data,
                                uint64_t len) {
  z_Error ret = z_OK;
  if (pthread_mutex_lock(&wr->Mnt) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  if (wr->CurSize + len < wr->MaxSize) {
    if (fwrite(data, sizeof(uint8_t), len, wr->File) != len) {
      z_error("fwrite");
      ret = z_ERR_SYS;
    }
    wr->CurSize += len;
  } else {
    z_error("nospace current:%llu len:%llu max:%llu", wr->CurSize, len, wr->MaxSize);
    ret = z_ERR_NOSPACE;
  }

  if (pthread_mutex_unlock(&wr->Mnt) != 0) {
    z_error("pthread_mutex_lock");
    return z_ERR_SYS;
  }

  return ret;
}

z_Error z_BinLogFileWriterAppendRecord(z_BinLogFileWriter *wr, z_Record *r) {
  return z_BinLogFileWriterWrite(wr, (uint8_t*)r, z_RecordLen(r));
}

typedef struct {
  FILE *File;
} z_BinLogFileReader;


z_BinLogFileReader *z_BinLogFileReaderNew(char *path, uint64_t max_size) {
  z_BinLogFileReader *rd = malloc(sizeof(z_BinLogFileReader));
  rd->File = fopen(path, "r");
  if (rd->File == nullptr) {
    z_panic("fopen %s", path);
  }
  return rd;
}

void z_BinLogFileReaderFree(z_BinLogFileReader *rd) {
  int ret = fclose(rd->File);
  if (ret != 0) {
    z_panic("fclose");
  }

  free(rd);
}

z_Error z_BinLogFileReaderRead(z_BinLogFileReader *rd, uint8_t *data,
                                uint64_t len) {
  if (fread(data, sizeof(uint8_t), len, rd->File) != len) {
    z_error("fread");
    return z_ERR_SYS;
  }

  return z_OK;
}

z_Error z_BinLogFileReaderGetRecord(z_BinLogFileReader *rd, z_Record **r) {
  z_Record record;
  z_Error ret = z_BinLogFileReaderRead(rd, (uint8_t*)&record, sizeof(z_Record));
  if (ret != z_OK) {
    z_error("z_BinLogFileReaderRead");
    return ret;
  }

  uint64_t len = z_RecordLen(&record);
  z_Record * ret_record = (z_Record *)malloc(len);
  *ret_record = record;
  ret = z_BinLogFileReaderRead(rd, (uint8_t*)(ret_record+sizeof(z_Record)), len - sizeof(z_Record));
  if (ret != z_OK) {
    z_error("z_BinLogFileReaderRead");
    z_RecordFree(ret_record);
    return ret;
  }

  ret = z_RecordCheck(ret_record);
  if (ret != z_OK) {
    z_error("z_RecordCheck");
    z_RecordFree(ret_record);
    return ret;
  }

  *r = ret_record;
  return z_OK;
}

z_Error z_BinLogFileReaderReset(z_BinLogFileReader *rd) {
  if (fseek(rd->File, 0, SEEK_SET) != 0) {
    z_error("fseek");
    return z_ERR_SYS;
  }
  return z_OK;
}
#endif