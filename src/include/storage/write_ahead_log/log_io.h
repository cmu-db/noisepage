#pragma once
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include "common/macros.h"
#include "loggers/storage_logger.h"

namespace terrier::storage {
// TODO(Tianyu): Get rid of magic constant
#define BUFFER_SIZE (1 << 12)

/**
 * Modernized wrappers around Posix I/O sys calls to hide away the ugliness and use exceptions for error reporting.
 */
struct PosixIoWrappers {
  PosixIoWrappers() = delete;  // Un-instantiable

  // TODO(Tianyu): Use a better exception than runtime_error.
  /**
   * Wrapper around posix open call
   * @tparam Args type of varlen arguments
   * @param path posix path arg
   * @param oflag posix oflag arg
   * @param args posix mode arg
   * @throws runtime_error if the underlying posix call failed
   * @return a non-negative interger that is the file descriptor if the opened file.
   */
  template <class... Args>
  static int Open(const char *path, int oflag, Args... args) {
    while (true) {
      int ret = open(path, oflag, args...);
      if (ret == -1) {
        if (errno == EINTR) continue;
        throw std::runtime_error("Failed to open file with errno " + std::to_string(errno));
      }
      return ret;
    }
  }
  /**
   * Wrapper around posix close call
   * @param fd posix filedes arg
   * @throws runtime_error if the underlying posix call failed
   */
  static void Close(int fd);

  /**
   * Wrapper around the posix read call, where a single function call will always read the specified amount of bytes
   * unless eof is read. (unlike posix read, which can read arbitrarily many bytes less than the given amount)
   * @param fd posix fildes arg
   * @param buf posix buf arg
   * @param nbyte posix nbyte arg
   * @throws runtime_error if the underlying posix call failed
   * @return nbyte if the read is successful, or the number of bytes actually read if eof is read before nbytes are
   *         read. (i.e. there aren't enough bytes left in the file to read out nbyte many)
   */
  static uint32_t ReadFully(int fd, void *buf, size_t nbyte);

  /**
   * Wrapper around the posix write call, where a single function call will always write the entire buffer out.
   * (unlike posix write, which can write arbitrarily many bytes less than the given amount)
   * @param fd posix fildes arg
   * @param buf posix buf arg
   * @param nbyte posix nbyte arg
   * @throws runtime_error if the underlying posix call failed
   */
  static void WriteFully(int fd, const void *buf, size_t nbyte);
};
// TODO(Tianyu):  we need control over when and what to flush as the log manager. Thus, we need to write our
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
      : out_(PosixIoWrappers::Open(log_file_path, O_WRONLY | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR)) {}

  /**
   * Must call before object is destructed
   */
  void Close() { PosixIoWrappers::Close(out_); }

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
  void WriteUnsynced(const void *data, uint32_t size) { PosixIoWrappers::WriteFully(out_, data, size); }

  /**
   * Flush any buffered writes and call fsync to make sure that all writes are consistent.
   */
  void Flush() {
    WriteUnsynced(buffer_, buffer_size_);
    if (fsync(out_) == -1) throw std::runtime_error("fsync failed with errno " + std::to_string(errno));
    buffer_size_ = 0;
  }

 private:
  int out_;  // fd of the output files
  char buffer_[BUFFER_SIZE];
  uint32_t buffer_size_ = 0;
};

/**
 * Buffered reads from the write ahead log
 */
class BufferedLogReader {
  // TODO(Tianyu): Checksum
 public:
  /**
   * Instantiates a new BufferedLogReader to read from the specified log file.
   * @param log_file_path path to the the log file to read from.
   */
  explicit BufferedLogReader(const char *log_file_path) : in_(PosixIoWrappers::Open(log_file_path, O_RDONLY)) {}

  /**
   * @return if there are contents left in the write ahead log
   */
  bool HasMore() { return filled_size_ > read_head_ || in_ != -1; }

  /**
   * Read the specified number of bytes into the target location from the write ahead log. The method reads as many as
   * possible if there are not enough bytes in the log and returns false. The underlying log file fd is automatically
   * closed when all remaining bytes are buffered.
   *
   * @param dest pointer location to read into
   * @param size number of bytes to read
   * @return whether the log has the given number of bytes left
   */
  bool Read(void *dest, uint32_t size);

  /**
   * Read a value of the specified type from the log. An exception is thrown if the log file does not
   * have enough bytes left for a well formed value
   * @tparam T type of value to read
   * @return the value read
   */
  template <class T>
  T ReadValue() {
    T result;
    bool ret UNUSED_ATTRIBUTE = Read(&result, sizeof(T));
    TERRIER_ASSERT(ret, "Reading of value failed");
    return result;
  }

 private:
  int in_;  // or -1 if closed
  uint32_t read_head_ = 0, filled_size_ = 0;
  char buffer_[BUFFER_SIZE];

  void ReadFromBuffer(void *dest, uint32_t size) {
    TERRIER_ASSERT(read_head_ + size <= filled_size_, "Not enough bytes in buffer for the read");
    TERRIER_MEMCPY(dest, buffer_ + read_head_, size);
    read_head_ += size;
  }

  void RefillBuffer();
};
}  // namespace terrier::storage
