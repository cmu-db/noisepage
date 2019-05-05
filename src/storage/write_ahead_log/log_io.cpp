#include "storage/write_ahead_log/log_io.h"
#include <algorithm>
namespace terrier::storage {
void PosixIoWrappers::Close(int fd) {
  while (true) {
    int ret = close(fd);
    if (ret == -1) {
      if (errno == EINTR) continue;
      throw std::runtime_error("Failed to close file with errno " + std::to_string(errno));
    }
    return;
  }
}

uint32_t PosixIoWrappers::ReadFully(int fd, void *buf, size_t nbyte) {
  ssize_t bytes_read = 0;
  while (bytes_read < static_cast<ssize_t>(nbyte)) {
    ssize_t ret = read(fd, reinterpret_cast<char *>(buf) + bytes_read, static_cast<ssize_t>(nbyte) - bytes_read);
    if (ret == -1) {
      if (errno == EINTR) continue;
      throw std::runtime_error("Read failed with errno " + std::to_string(errno));
    }
    if (ret == 0) break;  // no more bytes left in the file
    bytes_read += ret;
  }
  return static_cast<uint32_t>(bytes_read);
}

void PosixIoWrappers::WriteFully(int fd, const void *buf, size_t nbyte) {
  ssize_t written = 0;
  while (static_cast<size_t>(written) < nbyte) {
    ssize_t ret = write(fd, reinterpret_cast<const char *>(buf) + written, nbyte - written);
    if (ret == -1) {
      if (errno == EINTR) continue;
      throw std::runtime_error("Write to log file failed with errno " + std::to_string(errno));
    }
    written += ret;
  }
}

bool BufferedLogReader::Read(void *dest, uint32_t size) {
  if (read_head_ + size <= filled_size_) {
    // bytes to read are already buffered.
    ReadFromBuffer(dest, size);
    return true;
  }
  // Not enough left in the buffer.
  uint32_t bytes_read_ = 0;
  while (bytes_read_ < size) {
    if (!HasMore()) return false;
    uint32_t read_size = std::min(size - bytes_read_, filled_size_ - read_head_);
    if (read_size == 0) RefillBuffer();  // when all contents in the buffer is fully read
    ReadFromBuffer(reinterpret_cast<char *>(dest) + bytes_read_, read_size);
    bytes_read_ += read_size;
  }
  return true;
}

void BufferedLogReader::RefillBuffer() {
  TERRIER_ASSERT(read_head_ == filled_size_, "Refilling a buffer that is not fully read results in loss of data");
  if (in_ == -1) throw std::runtime_error("No more bytes left in the log file");
  read_head_ = 0;
  filled_size_ = PosixIoWrappers::ReadFully(in_, buffer_, BUFFER_SIZE);
  if (filled_size_ < BUFFER_SIZE) {
    // TODO(Tianyu): Is it better to make this an explicit close?
    PosixIoWrappers::Close(in_);
    in_ = -1;
  }
}

// TODO(zhaozhes): copied from log_test.cpp because I believe it should be here because checkpoint recovery need it.
LogRecord *ReadNextLogRecord(BufferedLogReader *in) {
  auto size = in->ReadValue<uint32_t>();
  byte *buf = common::AllocationUtil::AllocateAligned(size);
  auto record_type = in->ReadValue<LogRecordType>();
  auto txn_begin = in->ReadValue<transaction::timestamp_t>();
  if (record_type == LogRecordType::COMMIT) {
    auto txn_commit = in->ReadValue<transaction::timestamp_t>();
    // Okay to fill in null since nobody will invoke the callback.
    // is_read_only argument is set to false, because we do not write out a commit record for a transaction if it is
    // not read-only.
    return CommitRecord::Initialize(buf, txn_begin, txn_commit, nullptr, nullptr, false, nullptr);
  }
  // TODO(Tianyu): Without a lookup mechanism this oid is not exactly meaningful. Implement lookup when possible
  auto table_oid UNUSED_ATTRIBUTE = in->ReadValue<catalog::table_oid_t>();
  auto tuple_slot = in->ReadValue<TupleSlot>();
  auto result = RedoRecord::PartialInitialize(buf, size, txn_begin,
                                              // TODO(Tianyu): Hacky as hell
                                              nullptr, tuple_slot);
  // TODO(Tianyu): For now, without inlined attributes, the delta portion is a straight memory copy. This
  // will obviously change in the future. Also, this is hacky as hell
  auto delta_size = in->ReadValue<uint32_t>();
  byte *dest = reinterpret_cast<byte *>(result->GetUnderlyingRecordBodyAs<RedoRecord>()->Delta()) + sizeof(uint32_t);
  in->Read(dest, delta_size - static_cast<uint32_t>(sizeof(uint32_t)));
  return result;
}

}  // namespace terrier::storage
