#pragma once

#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "storage/sql_table.h"
#include "storage/write_ahead_log/log_record.h"

namespace noisepage::storage {

/**
 * @@brief Abstract class for log providers
 * A log provider is an object that supplies logs to the recovery manager from an arbitrary source (log file, network,
 * etc). A provider should implement the Read method in such a way that it appears like it is reading contiguous memory
 */
class AbstractLogProvider {
 public:
  /** The type of log provider that this is. */
  enum class LogProviderType : uint8_t { RESERVED = 0, DISK, REPLICATION };

  /** @return The type of this log provider. */
  virtual LogProviderType GetType() const = 0;

  /**
   * Provide next available log record
   * @warning Can be a blocking call if provider is waiting to receive more logs
   * @return next log record along with vector of varlen entry pointers. nullptr log record if no more logs will be
   * provided.
   */
  std::pair<LogRecord *, std::vector<byte *>> GetNextRecord() {
    return HasMoreRecords() ? ReadNextRecord() : std::make_pair(nullptr, std::vector<byte *>());
  }

 protected:
  /**
   * @return true if provider has more records to provide. false otherwise
   */
  virtual bool HasMoreRecords() = 0;

  /**
   * Read the specified number of bytes into the target location from the log provider. The method reads as many as
   * possible. If there are not enough bytes, it returns false.
   *
   * Provider should override this method, and implement it in a way such that reading should appear as if the memory is
   * being read continuously
   *
   * @param dest pointer location to read into
   * @param size number of bytes to read
   * @return whether the log has the given number of bytes left
   */
  virtual bool Read(void *dest, uint32_t size) = 0;

 private:
  // TODO(Gus): Support a more fail-safe way than just throwing an exception
  /**
   * Read a value of the specified type from log provider. An exception is thrown if the reading failed
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

  /**
   * Reads in the next log record from the log provider
   * @warning If the serialization format of logs ever changes, this function will need to be updated.
   * @return next log record, along with vector of varlen entry pointers
   */
  std::pair<LogRecord *, std::vector<byte *>> ReadNextRecord();
};
}  // namespace noisepage::storage
