#pragma once
#include "storage/projected_columns.h"
#include "storage/projected_row.h"
#include "storage/write_ahead_log/log_io.h"
namespace terrier::storage {
// TODO(Zhaozhe): Should fetch block_size. Use magic number first.
// The block size to output to disk every time
#define CHECKPOINT_BLOCK_SIZE 4096

/**
 * A buffered tuple writer collects tuples into blocks, and output a block once a time.
 * The block size is fetched from database setting, and should be the page size.
 * layout:
 * -------------------------------------------------------------------------------
 * | checksum | tuple1 | varlen_entry(if exist) | tuple2 | tuple3 | ... |
 * -------------------------------------------------------------------------------
 */
class BufferedTupleWriter {
//  TODO(Zhaozhe): checksum
public:
  // TODO(Zhaozhe, Mengyang): More fields can be added to header
  class Header {
  public:
    Header() {
      checksum_ = 0;
    }
    void SetCheckSum(uint32_t checksum) {
      checksum_ = checksum;
    }
    uint32_t GetCheckSum() {
      return checksum_;
    }
  private:
    uint32_t checksum_;
  };
  
  BufferedTupleWriter() = default;
  
  explicit BufferedTupleWriter(const char *log_file_path) {
    Open(log_file_path);
  }
  
  void Open(const char *log_file_path) {
    buffered_writer.Open(log_file_path);
    block_size_ = CHECKPOINT_BLOCK_SIZE;
    buffer_ = new char[block_size_];
    ResetBuffer();
  }
  
  void Persist() {
    PersistBuffer();
  }
  
  void Close() {
    buffered_writer.Close();
    delete(buffer_);
  }
  
  // Serialize the tuple into the internal block buffer, and write to disk when the buffer is full.
  void SerializeTuple(ProjectedColumns::RowView &row, ProjectedRow *row_buffer,
                      const storage::BlockLayout &layout);
  
  void AppendTupleToBuffer(ProjectedRow *row_buffer, int32_t total_varlen,
                           const std::vector<const VarlenEntry*> &varlen_entries);
  
private:
  BufferedLogWriter buffered_writer;
  uint32_t block_size_;
  uint32_t cur_buffer_size_ = 0;
  char *buffer_;
  
  void ResetBuffer() {
    memset(buffer_, 0, block_size_);
    Header *header = new Header();
    memcpy(buffer_, header, sizeof(Header));
    cur_buffer_size_ = sizeof(Header);
  }
  
  void PersistBuffer() {
    // TODO(zhaozhe): calculate CHECKSUM. Currently using default 0 as checksum
    // TODO(zhaozhe): header size might need to be considered as well
    if (cur_buffer_size_ == sizeof(Header)) {
      // If the buffer has no contents, just return
      return;
    }
    buffered_writer.BufferWrite(buffer_, block_size_);
    buffered_writer.Persist();
    ResetBuffer();
  }
};
}
