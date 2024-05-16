#ifndef z_FILE_H
#define z_FILE_H

#include <stdint.h>
#include <stdio.h>

#include "zbinlog/file_record.h"
#include "zerror/error.h"

typedef struct {
  int64_t MaxSize;
  FILE *File;
} z_Writer;

z_Error z_WriterOffset(z_Writer *wr, int64_t *offset) {
  if (wr->File == nullptr) {
    z_error("wr->File == nullptr");
    return z_ERR_INVALID_DATA;
  }

  *offset = ftell(wr->File);
  if (*offset < 0) {
    z_error("ftell %lld", *offset);
    return z_ERR_FS;
  }

  return z_OK;
}

z_Error z_WriterInit(z_Writer *wr, char *path, int64_t max_size) {
  if (wr == nullptr || path == nullptr || max_size == 0) {
    z_error("wr == nullptr || path == nullptr || max_size == 0");
    return z_ERR_INVALID_DATA;
  }
  wr->MaxSize = max_size;

  wr->File = fopen(path, "a");
  if (wr->File == nullptr) {
    z_error("fopen %s", path);
    return z_ERR_FS;
  }
  
  return z_OK;
}

void z_WriterDestroy(z_Writer *wr) {
  if (wr == nullptr || wr->File == nullptr) {
    return;
  }

  int ret = fclose(wr->File);
  if (ret != 0) {
    z_error("fclose");
  }

  wr->MaxSize = 0;
}

z_Error z_WriterWrite(z_Writer *wr, int8_t *data, int64_t len) {
  int64_t offset = {};
  z_Error ret = z_WriterOffset(wr, &offset);
  if (ret != z_OK) {
    z_error("z_WriterOffset %d", ret);
    return z_ERR_FS;
  }

  if (offset + len < wr->MaxSize) {
    if (fwrite(data, sizeof(int8_t), len, wr->File) != len) {
      z_error("fwrite");
      return z_ERR_FS;
    }

    if (fflush(wr->File) != 0) {
      z_error("fflush");
    }
  } else {
    z_error("nospace current:%lld len:%lld max:%lld", offset, len,
            wr->MaxSize);
    return z_ERR_NOSPACE;
  }
  return z_OK;
}

z_Error z_WriterAppendRecord(z_Writer *wr, z_FileRecord *r) {
  z_Error ret = z_WriterWrite(wr, (int8_t*)&r->Seq, sizeof(r->Seq));
  if (ret != z_OK) {
    return ret;
  }

  ret = z_WriterWrite(wr, (int8_t *)r->Record, z_RecordLen(r->Record));
  if (ret != z_OK) {
    return ret;
  }

  return z_OK;
}

typedef struct {
  FILE *File;
} z_Reader;

z_Error z_ReaderInit(z_Reader *rd, char *path) {
  if (rd == nullptr || path == nullptr) {
    return z_ERR_INVALID_DATA;
  }

  rd->File = fopen(path, "r");
  if (rd->File == nullptr) {
    z_error("fopen %s", path);
    return z_ERR_FS;
  }

  return z_OK;
}

void z_ReaderDestroy(z_Reader *rd) {
  if (rd == nullptr) {
    return;
  }

  int ret = fclose(rd->File);
  if (ret != 0) {
    z_error("fclose");
  }
}

z_Error z_ReaderRead(z_Reader *rd, int8_t *data, int64_t len) {
  if (rd == nullptr || data == nullptr || len == 0) {
    z_error("rd == nullptr || data == nullptr || len == 0");
    return z_ERR_INVALID_DATA;
  }

  int64_t l = fread(data, sizeof(int8_t), len, rd->File);
  if (l != len) {
    z_error("fread %lld", l);
    return z_ERR_FS;
  }

  return z_OK;
}

z_Error z_ReaderGetRecord(z_Reader *rd, z_FileRecord *r) {
  if (rd == nullptr || r == nullptr) {
    z_error("rd == nullptr || r == nullptr");
    return z_ERR_INVALID_DATA;
  }
  
  z_Error ret = z_ReaderRead(rd, (int8_t *)&r->Seq, sizeof(r->Seq));
  if (ret != z_OK) {
    z_error("z_ReaderRead");
    return ret;
  }
  
  z_Record record;
  ret = z_ReaderRead(rd, (int8_t *)&record, sizeof(z_Record));
  if (ret != z_OK) {
    z_error("z_ReaderRead");
    return ret;
  }

  int64_t len = z_RecordLen(&record);
  z_Record *ret_record = z_RecordNewByLen(len);
  *ret_record = record;
  ret =
      z_ReaderRead(rd, (int8_t *)(ret_record + 1), len - sizeof(z_Record));
  if (ret != z_OK) {
    z_error("z_ReaderRead");
    z_RecordFree(ret_record);
    return ret;
  }

  ret = z_RecordCheck(ret_record);
  if (ret != z_OK) {
    z_error("z_RecordCheck");
    z_RecordFree(ret_record);
    return ret;
  }

  r->Record = ret_record;
  return z_OK;
}

z_Error z_ReaderSet(z_Reader *rd, int64_t offset) {
  if (rd == nullptr) {
    z_error("rd == nullptr");
    return z_ERR_INVALID_DATA;
  }

  if (fseek(rd->File, offset, SEEK_SET) != 0) {
    z_error("fseek");
    return z_ERR_FS;
  }
  return z_OK;
}

z_Error z_ReaderReset(z_Reader *rd) { return z_ReaderSet(rd, 0); }

z_Error z_ReaderOffset(z_Reader *rd, int64_t *offset) {
  if (rd->File == nullptr) {
    z_error("rd->File == nullptr");
    return z_ERR_INVALID_DATA;
  }

  *offset = ftell(rd->File);
  if (*offset < 0) {
    z_error("ftell %lld", *offset);
    return z_ERR_FS;
  }

  return z_OK;
}

z_Error z_ReaderMaxOffset(z_Reader *rd, int64_t *offset) {
  z_assert(rd != nullptr, offset != nullptr);

  int64_t src_offset = 0;
  z_Error ret = z_ReaderOffset(rd, &src_offset);
  if (ret != z_OK) {
    return ret;
  }

  if (fseek(rd->File, 0, SEEK_END) != 0) {
    z_error("fseek");
    return z_ERR_FS;
  }

  ret = z_ReaderOffset(rd, offset);
  if (ret != z_OK) {
    return ret;
  }

  if (fseek(rd->File, src_offset, SEEK_SET) != 0) {
    z_error("fseek");
    return z_ERR_FS;
  }

  return z_OK;
}


#endif