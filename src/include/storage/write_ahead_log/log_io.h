#pragma once
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include "common/macros.h"
#include "loggers/storage_logger.h"

namespace terrier::storage {
// TODO(Tianyu): Get rid of magic constant
#define BUFFER_SIZE (1 << 12)
// TODO(Tianyu): Apparently, c++ fstream is considered slow and inefficient. Additionally, we
// need control over when and what to flush as the log manager. Thus, we need to write our
// own wrapper around lower level I/O functions. I could be wrong, and in that case we should
// revert to using STL.
/**
 * Handles buffered writes to the write ahead log, and provides control over flushing.
 */
class BufferedLogWriter {
  // TODO(Tianyu): Checksum
 public:
  /**
   * Instantiates a new BufferedLogWriter to write to the specified log file.
   *
   * @param log_file_path path to the the log file to write to. New entries are appended to the end of the file if the
   * file already exists; otherwise, a file is created.
   */
  explicit BufferedLogWriter(const char *log_file_path)
      : out_(open(log_file_path, O_WRONLY | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR)) {
    if (out_ == -1) throw std::runtime_error("Opening of log file failed with errno " + std::to_string(errno));
  }

  /**
   * Destructs a BufferedLogWriter and close the file descriptor. It is up to the caller to ensure that all contents
   * are flushed before the destructor is called.
   */
  ~BufferedLogWriter() {
    if (close(out_) == -1) {
      STORAGE_LOG_ERROR("Closing of log file failed with errno %d", errno);
      TERRIER_ASSERT(false, "Unexpected invocation of unreachable code");
    }
  }

  /**
   * Check if the internal buffer of this BufferedLogWriter has enough space for the specified number of bytes. If the
   * call returns false, the caller is responsible for either flushing the buffer and handle accordingly or use the
   * non-buffered version for writes that are too large.
   *
   * @param size number of bytes to check for
   * @return if the internal buffer can hold the given number of bytes.
   */
  bool CanBuffer(uint32_t size) { return BUFFER_SIZE - buffer_size_ >= size; }

  /**
   * Write to the log file the given amount of bytes from the given location in memory, but buffer the write so the
   * update is only written out when the BufferedLogWriter is flushed. It is the caller's responsibility to check
   * before hand with @see CanBuffer that the write can be buffered into the BufferedLogWriter.
   * @param data memory location of the bytes to write
   * @param size number of bytes to write
   */
  void BufferWrite(const void *data, uint32_t size) {
    TERRIER_ASSERT(CanBuffer(size), "attempting to write to full write buffer");
    TERRIER_MEMCPY(buffer_ + buffer_size_, data, size);
    buffer_size_ += size;
  }

  /**
   * Write directly to the file, without buffer, the given amount of bytes from the given location in memory. This
   * method should only be called on objects that are too large to be buffered, as calling this on every small write
   * would be slow. Additionally, there is no guarantee on the write being persistent on method exist until a call to
   * Flush().
   *
   * @param data memory location of the bytes to write
   * @param size number of bytes to write
   */
  void WriteUnsyncedFully(const void *data, uint32_t size) {
    ssize_t written = 0;
    while (written != size) {
      ssize_t ret = write(out_, reinterpret_cast<const char *>(data) + written, static_cast<size_t>(size - written));
      if (ret == -1) {
        switch (errno) {
          case EINTR:
            continue;
          default:
            throw std::runtime_error("Write to log file failed with errno " + std::to_string(errno));
        }
      }
      written += ret;
    }
  }

  /**
   * Flush any buffered writes and call fsync to make sure that all writes are consistent.
   */
  void Flush() {
    WriteUnsyncedFully(buffer_, buffer_size_);
    if (fsync(out_) == -1) throw std::runtime_error("fsync failed with errno " + std::to_string(errno));
    buffer_size_ = 0;
  }

 private:
  int out_;  // fd of the output files
  char buffer_[BUFFER_SIZE];
  uint32_t buffer_size_ = 0;
};
}  // namespace terrier::storage
