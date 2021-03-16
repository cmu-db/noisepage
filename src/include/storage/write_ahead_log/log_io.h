#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.h"
#include "common/macros.h"
#include "common/posix_io_wrappers.h"
#include "loggers/storage_logger.h"
#include "transaction/transaction_defs.h"

namespace noisepage::replication {
class ReplicationManager;
class PrimaryReplicationManager;
class ReplicaReplicationManager;
}  // namespace noisepage::replication

namespace noisepage::storage {

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
   * Move constructor.
   *
   * This is necessary because of the atomic refcount field, which invalidates the default move ctor.
   * To my knowledge, existing code always pre-allocates buffers in one shot, so these buffers will not actually get
   * moved at runtime -- this exists solely so that std::vector's emplace_back requirement of being both MoveInsertable
   * and EmplaceConstructible will be satisfied.
   */
  BufferedLogWriter(BufferedLogWriter &&other) noexcept {
    out_ = other.out_;
    memcpy(buffer_, other.buffer_, common::Constants::LOG_BUFFER_SIZE);
    buffer_size_ = other.buffer_size_;
    serialize_refcount_.store(other.serialize_refcount_.load());
  }

  /**
   * Must call before object is destructed
   */
  void Close() { PosixIoWrappers::Close(out_); }

  /**
   * Write to the log file the given amount of bytes from the given location in memory, but buffer the write so the
   * update is only written out when the BufferedLogWriter is persisted. Note that this function writes to the buffer
   * only until it is full. If buffer gets full, then call FlushBuffer() and call BufferWrite(..) again with the correct
   * offset of the data, depending on the number of bytes that were already written.
   * @param data memory location of the bytes to write
   * @param size number of bytes to write
   * @return number of bytes written. This function only writes until the buffer gets full, so this can be used as the
   * offset when calling this function again after flushing.
   */
  uint32_t BufferWrite(const void *data, uint32_t size) {
    // If we still do not have buffer space after flush, the write is too large to be buffered. We partially write the
    // buffer and return the number of bytes written
    if (!CanBuffer(size)) {
      size = common::Constants::LOG_BUFFER_SIZE - buffer_size_;
    }
    std::memcpy(buffer_ + buffer_size_, data, size);
    buffer_size_ += size;
    return size;
  }

  /**
   * Call fsync to make sure that all writes are consistent. fdatasync is used as an optimization on Linux since we
   * don't care about all of the file's metadata being persisted, just the contents.
   */
  void Persist() {
#if __APPLE__
    // macOS provides fcntl(out_, F_FULLFSYNC) to guarantee that on-disk buffers are flushed. AFAIK there is no portable
    // way to do this on Linux so we'll just keep fsync for now.
    if (fsync(out_) == -1) throw std::runtime_error("fsync failed with errno " + std::to_string(errno));
#else
    if (fdatasync(out_) == -1) throw std::runtime_error("fdatasync failed with errno " + std::to_string(errno));
#endif
  }

  /**
   * Flush any buffered writes.
   * @return amount of data flushed
   */
  uint64_t FlushBuffer() {
    auto size = buffer_size_;
    WriteUnsynced(buffer_, buffer_size_);
    buffer_size_ = 0;
    return size;
  }

  /**
   * @return if the buffer is full
   */
  bool IsBufferFull() { return buffer_size_ == common::Constants::LOG_BUFFER_SIZE; }

  /**
   * Mark that the BufferedLogWriter is now ready to be persisted and sent to different destinations.
   *
   * For example, the BufferedLogWriter may then be sent to any of the following destinations:
   * - Serialized to disk.
   * - Sent to replicas over the network.
   *
   * This function exists to avoid copying the BufferedLogWriter's buffers needlessly.
   * Instead, a refcount is maintained depending on the retention policy.
   *
   * @param policy The retention policy that describes the destinations for this BufferedLogWriter.
   */
  void PrepareForSerialization(transaction::RetentionPolicy policy) {
    NOISEPAGE_ASSERT(serialize_refcount_.load() == 0, "This buffer is already being serialized.");
    switch (policy) {
      case transaction::RetentionPolicy::DISABLE_RETENTION: {
        serialize_refcount_.store(0);  // Nothing.
        break;
      }
      case transaction::RetentionPolicy::RETENTION_LOCAL_DISK: {
        serialize_refcount_.store(1);  // DiskLogConsumerTask.
        break;
      }
      case transaction::RetentionPolicy::RETENTION_LOCAL_DISK_AND_NETWORK_REPLICAS: {
        serialize_refcount_.store(2);  // DiskLogConsumerTask + ReplicationManager.
        break;
      }
      default:
        throw std::runtime_error("Unknown retention policy in PrepareForSerialization().");
    }
  }

  /**
   * Mark one successful serialization of the buffered log.
   *
   * This should be called exactly once for each serializer of this log. See PrepareForSerialization() for more info.
   * @return True if the current log has been completely serialized, meaning that no serializers are left and that
   *         it is safe to now reuse this BufferedLogWriter.
   */
  bool MarkSerialized() {
    auto count = serialize_refcount_.fetch_sub(1);
    NOISEPAGE_ASSERT(serialize_refcount_.load() >= 0, "This buffer was serialized too many times?");
    return count == 1;
  }

 private:
  friend class replication::ReplicationManager;
  friend class replication::PrimaryReplicationManager;
  friend class replication::ReplicaReplicationManager;

  int out_;  // fd of the output files
  char buffer_[common::Constants::LOG_BUFFER_SIZE];

  uint32_t buffer_size_ = 0;
  std::atomic<int8_t> serialize_refcount_ = 0;  ///< The number of would-be serializers that haven't serialized yet.

  bool CanBuffer(uint32_t size) { return common::Constants::LOG_BUFFER_SIZE - buffer_size_ >= size; }

  void WriteUnsynced(const void *data, uint32_t size) { PosixIoWrappers::WriteFully(out_, data, size); }
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
   * Closes log file if it has not been closed already. While Read will close the file if it reaches the end, this will
   * handle cases where we destroy the reader before reading the whole file.
   */
  ~BufferedLogReader() {
    if (in_ != -1) PosixIoWrappers::Close(in_);
  }

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
    NOISEPAGE_ASSERT(ret, "Reading of value failed");
    return result;
  }

 private:
  int in_;  // or -1 if closed
  uint32_t read_head_ = 0, filled_size_ = 0;
  char buffer_[common::Constants::LOG_BUFFER_SIZE];

  void ReadFromBuffer(void *dest, uint32_t size) {
    NOISEPAGE_ASSERT(read_head_ + size <= filled_size_, "Not enough bytes in buffer for the read");
    std::memcpy(dest, buffer_ + read_head_, size);
    read_head_ += size;
  }

  void RefillBuffer();
};

/**
 * Callback function and arguments to be called when record is persisted
 */
using CommitCallback = std::pair<transaction::callback_fn, void *>;

/**
 * A BufferedLogWriter containing serialized logs, as well as all commit callbacks for transaction's whose commit are
 * serialized in this BufferedLogWriter
 */
using SerializedLogs = std::pair<BufferedLogWriter *, std::vector<CommitCallback>>;

}  // namespace noisepage::storage
