#pragma once
#include "storage/projected_columns.h"
#include "storage/projected_row.h"
#include "storage/write_ahead_log/log_io.h"
namespace terrier::storage {
// TODO(Zhaozhe): Should fetch block_size. Use magic number first.
// The block size to output to disk every time
#define CHECKPOINT_BLOCK_SIZE 4096
#define CHECKSUM_BYTES 4

/**
 * A buffered tuple writer collects tuples into blocks, and output a block once a time.
 * The block size is fetched from database setting, and should be the page size.
 * layout:
 * -------------------------------------------------------------------------------
 * | checksum | tuple1 | varlen_entry(if exist) | tuple2 | tuple3 | ... |
 * -------------------------------------------------------------------------------
 */
// TODO(zhaozhes): might better move these io functions to a new file, eg., checkpoint_io.h.
class BufferedTupleWriter {
//  TODO(Zhaozhe): checksum
public:
  BufferedTupleWriter() = default;
  
  explicit BufferedTupleWriter(const char *log_file_path)
    : buffered_writer(log_file_path), block_size_(CHECKPOINT_BLOCK_SIZE) {
    buffer_ = new char[block_size_];
    InitBuffer();
  }
  
  void Open(const char *log_file_path) {
    buffered_writer.Open(log_file_path);
    buffer_ = new char[block_size_];
    InitBuffer();
  }
  
  void Persist() {
    WriteBuffer();
  }
  
  void Close() {
    buffered_writer.Close();
    delete(buffer_);
  }
  
  void SerializeTuple(ProjectedColumns::RowView &row, ProjectedRow *row_buffer,
                      const storage::BlockLayout &layout);

private:
  BufferedLogWriter buffered_writer;
  uint32_t block_size_ = CHECKPOINT_BLOCK_SIZE;
  uint32_t cur_buffer_size_ = 0;
  char *buffer_;
  
  void InitBuffer() {
    memset(buffer_, 0, block_size_);
    cur_buffer_size_ = CHECKSUM_BYTES;
  }
  
  void WriteBuffer() {
    // TODO(zhaozhe): calculate CHECKSUM. Currently using default 0 as checksum
    // TODO(zhaozhe): header size might need to be considered as well
    if (cur_buffer_size_ == CHECKSUM_BYTES) {
      // If the buffer has no contents, just return
      return;
    }
    buffered_writer.BufferWrite(buffer_, block_size_);
    buffered_writer.Persist();
    InitBuffer();
  }
};
}
