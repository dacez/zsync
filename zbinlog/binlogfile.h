#ifndef z_BINLOGFILE_H
#define z_BINLOGFILE_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "zerror/error.h"
#include "zutils/utils.h"

#include "zbinlog/record.h"

typedef struct {
  int64_t MaxSize;
  int64_t CurSize;
  FILE *File;
} z_BinLogFileWriter;

z_Error z_BinLogFileWriterInit(z_BinLogFileWriter *wr, char *path,
                               int64_t max_size) {
  if (wr == nullptr || path == nullptr || max_size == 0) {
    z_error("wr == nullptr || path == nullptr || max_size == 0");
    return z_ERR_INVALID_DATA;
  }
  wr->MaxSize = max_size;
  wr->CurSize = 0;

  wr->File = fopen(path, "a");
  if (wr->File == nullptr) {
    z_error("fopen %s", path);
    return z_ERR_SYS;
  }

  return z_OK;
}

void z_BinLogFileWriterDestroy(z_BinLogFileWriter *wr) {
  if (wr == nullptr || wr->File == nullptr) {
    z_debug("wr == nullptr || wr->File == nullptr");
    return;
  }

  int ret = fclose(wr->File);
  if (ret != 0) {
    z_error("fclose");
  }

  wr->MaxSize = 0;
  wr->CurSize = 0;
}

z_Error z_BinLogFileWriterWrite(z_BinLogFileWriter *wr, int8_t *data,
                                int64_t len, int64_t *offset) {
  z_Error ret = z_OK;
  if (wr->CurSize + len < wr->MaxSize) {
    *offset = ftell(wr->File);
    if (*offset < 0) {
      z_error("ftell %lld", *offset);
      return z_ERR_SYS;
    }

    if (fwrite(data, sizeof(int8_t), len, wr->File) != len) {
      z_error("fwrite");
      ret = z_ERR_SYS;
    }
    if (fflush(wr->File) != 0) {
      z_error("fflush");
    }
    wr->CurSize += len;

  } else {
    z_error("nospace current:%lld len:%lld max:%lld", wr->CurSize, len,
            wr->MaxSize);
    ret = z_ERR_NOSPACE;
  }
  return ret;
}

z_Error z_BinLogFileWriterAppendRecord(z_BinLogFileWriter *wr, z_Record *r,
                                       int64_t *offset) {
  return z_BinLogFileWriterWrite(wr, (int8_t *)r, z_RecordLen(r), offset);
}

typedef struct {
  FILE *File;
} z_BinLogFileReader;

z_Error z_BinLogFileReaderInit(z_BinLogFileReader *rd, char *path) {
  if (rd == nullptr || path == nullptr) {
    z_debug("rd == nullptr || path == nullptr");
    return z_ERR_INVALID_DATA;
  }

  rd->File = fopen(path, "r");
  if (rd->File == nullptr) {
    z_error("fopen %s", path);
    return z_ERR_SYS;
  }

  return z_OK;
}

void z_BinLogFileReaderDestroy(z_BinLogFileReader *rd) {
  if (rd == nullptr) {
    z_debug("rd == nullptr");
    return;
  }

  int ret = fclose(rd->File);
  if (ret != 0) {
    z_error("fclose");
  }
}

z_Error z_BinLogFileReaderRead(z_BinLogFileReader *rd, int8_t *data,
                               int64_t len) {
  if (rd == nullptr || data == nullptr || len == 0) {
    z_error("rd == nullptr || data == nullptr || len == 0");
    return z_ERR_INVALID_DATA;
  }

  int64_t l = fread(data, sizeof(int8_t), len, rd->File);
  if (l != len) {
    z_error("fread %lld", l);
    return z_ERR_SYS;
  }

  return z_OK;
}

z_Error z_BinLogFileReaderGetRecord(z_BinLogFileReader *rd, z_Record **r) {
  if (rd == nullptr || r == nullptr) {
    z_error("rd == nullptr || r == nullptr");
    return z_ERR_INVALID_DATA;
  }

  *r = nullptr;

  z_Record record;
  z_Error ret = z_BinLogFileReaderRead(rd, (int8_t *)&record, sizeof(z_Record));
  if (ret != z_OK) {
    z_error("z_BinLogFileReaderRead");
    return ret;
  }

  int64_t len = z_RecordLen(&record);
  z_Record *ret_record = z_RecordNewByLen(len);
  *ret_record = record;
  ret = z_BinLogFileReaderRead(rd, (int8_t *)(ret_record + 1),
                               len - sizeof(z_Record));
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

z_Error z_BinLogFileReaderSet(z_BinLogFileReader *rd, int64_t offset) {
  if (rd == nullptr) {
    z_error("rd == nullptr");
    return z_ERR_INVALID_DATA;
  }

  if (fseek(rd->File, offset, SEEK_SET) != 0) {
    z_error("fseek");
    return z_ERR_SYS;
  }
  return z_OK;
}

z_Error z_BinLogFileReaderReset(z_BinLogFileReader *rd) {
  return z_BinLogFileReaderSet(rd, 0);
}
#endif