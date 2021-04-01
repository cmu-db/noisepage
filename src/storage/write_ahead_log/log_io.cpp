#include "storage/write_ahead_log/log_io.h"

#include <algorithm>
namespace noisepage::storage {

bool BufferedLogReader::Read(void *dest, uint32_t size) {
  if (read_head_ + size <= filled_size_) {
    // bytes to read are already buffered.
    ReadFromBuffer(dest, size);
    return true;
  }
  // Not enough left in the buffer.
  uint32_t bytes_read = 0;
  while (bytes_read < size) {
    if (!HasMore()) return false;
    uint32_t read_size = std::min(size - bytes_read, filled_size_ - read_head_);
    if (read_size == 0) RefillBuffer();  // when all contents in the buffer is fully read
    ReadFromBuffer(reinterpret_cast<char *>(dest) + bytes_read, read_size);
    bytes_read += read_size;
  }
  return true;
}

void BufferedLogReader::RefillBuffer() {
  NOISEPAGE_ASSERT(read_head_ == filled_size_, "Refilling a buffer that is not fully read results in loss of data");
  if (in_ == -1) throw std::runtime_error("No more bytes left in the log file");
  read_head_ = 0;
  filled_size_ = PosixIoWrappers::ReadFully(in_, buffer_, common::Constants::LOG_BUFFER_SIZE);
  if (filled_size_ < common::Constants::LOG_BUFFER_SIZE) {
    // TODO(Tianyu): Is it better to make this an explicit close?
    PosixIoWrappers::Close(in_);
    in_ = -1;
  }
}

}  // namespace noisepage::storage
