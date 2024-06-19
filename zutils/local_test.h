
#include "zutils/buffer.h"
#include "zutils/local.h"
#include "ztest/test.h"
#include "zutils/mem.h"
#include <string.h>



void z_LocalTest() {
  z_LocalBuffer b = {};
  void *data = z_malloc(1024);
  memset(data, '1', 1024);
  z_LocalBufferAppend(&b, data, 1024);
  z_ConstBuffer cb = {.Data = data, .Size = 1024};
  z_ASSERT_TRUE(z_BufferIsEqual(&b, &cb));
  z_ASSERT_TRUE(z_thread_local_allocator.Data[0] != nullptr);
  z_ASSERT_TRUE(z_thread_local_allocator.Data[1] == nullptr);
  z_ASSERT_TRUE(z_thread_local_allocator.Pos.Level == 0);
  z_ASSERT_TRUE(z_thread_local_allocator.Pos.Offset == 1024);

  void *data1 = z_malloc(1024);
  memset(data1, '2', 1024);

  void *data2 = z_malloc(2048);
  memcpy(data2, data, 1024);
  memcpy(data2+1024, data1, 1024);

  z_LocalBufferAppend(&b, data1, 1024);
  cb.Data = data2;
  cb.Size = 2048;
  
  z_ASSERT_TRUE(z_BufferIsEqual(&b, &cb));
  z_ASSERT_TRUE(z_thread_local_allocator.Data[0] != nullptr);
  z_ASSERT_TRUE(z_thread_local_allocator.Data[1] != nullptr);
  z_ASSERT_TRUE(z_thread_local_allocator.Data[2] == nullptr);
  z_ASSERT_TRUE(z_thread_local_allocator.Pos.Level == 1);
  z_ASSERT_TRUE(z_thread_local_allocator.Pos.Offset == 2048);

  z_free(data);
  z_free(data1);
  z_free(data2);
}
