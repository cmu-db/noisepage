#pragma once
#include <fstream>
#include <map>
#include <string>
#include <utility>
#include "common/macros.h"
#include "common/serializable.h"
#include "loggers/storage_logger.h"
#include "logging/log_common.h"
#include "logging/log_record.h"
#include "storage/storage_defs.h"

namespace terrier::logging {

/**
 * A RecoveryManager reads log records from log files, and replays the records
 * by the order of transactions.
 */
class RecoveryManager {
 public:
  /**
   * @brief A RecoveryManager is only initiated with parameters when the system
   * starts, and should never be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(RecoveryManager);

  /**
   * @brief Constructs a RecoveryManager with the given directory that stores
   * log files.
   *
   * @param dir the directory where the log file is stored
   */
  explicit RecoveryManager(std::string dir) : dir_(std::move(dir)) {
    if (file_.is_open()) {
      STORAGE_LOG_ERROR("Failed to open the stream because it it open already");
    }
    file_name_ = LOG_FILE_NAME;
    std::string path = dir_ + "/" + file_name_;
    file_.open(path, std::ios_base::in);
    if (file_.fail()) {
      STORAGE_LOG_ERROR("Failed to open the stream: %s", strerror(errno));
    }
  }

  /**
   * @brief Destructs a RecoveryManager, closes the log file.
   */
  ~RecoveryManager() { file_.close(); }

  /**
   * @brief Performs complete database recovery.
   */
  void Recover();

  /**
   * @brief Parses log files to note down the offset of each committed
   * transaction in the recovery memory area.
   *
   * @return the total size of log records of all committed transactions
   */
  uint32_t ParseFile();

  /**
   * @brief Reads some bytes from a file stream to memory.
   *
   * @param stream the file stream from which the bytes are read
   * @param pos the memory to which the bytes are read
   * @param size number of bytes to be read
   *
   * @return the number of bytes actually read
   */
  uint32_t ReadBytes(std::fstream &stream, std::byte *pos, uint32_t size) {
    PELOTON_ASSERT(!stream.fail(), "an error occurs during an input or output operation");
    stream.read(reinterpret_cast<char *>(pos), size);
    return static_cast<uint32_t>(stream.gcount());
  }

  /**
   * @brief Pack a log record type and a length into a 64 bit unsigned integer.
   *
   * @param type the log record type to be packed
   * @param length the length to be packed
   *
   * @return The 64 bit packed representation of a log record type and a length
   */
  uint64_t PackTypeLength(LogRecordType type, uint32_t length) {
    uint64_t value = (static_cast<uint64_t>(type) << 32) | length;
    return value;
  }

  /**
   * @brief Extract a log record type from the 64 bit packed representation of a
   * log record type and a length.
   *
   * @param value the 64 bit packed representation of a log record type and a length
   *
   * @return The extracted log record type
   */
  LogRecordType ExtractType(uint64_t value) {
    auto type = static_cast<LogRecordType>((value >> 32) & 0xFFFFFFFF);
    return type;
  }

  /**
   * @brief Extract a length from the 64 bit packed representation of a log
   * record type and a length.
   *
   * @param value the 64 bit packed representation of a log record type and a length
   *
   * @return The extracted length
   */
  uint32_t ExtractLength(uint64_t value) {
    auto length = static_cast<uint32_t>(value & 0xFFFFFFFF);
    return length;
  }

 private:
  // TODO(Aaron): fstream is not thread safe, might need to change it if more
  // than one logger thread is used
  std::string dir_;
  std::string file_name_;
  std::fstream file_;
  std::map<timestamp_t, uint32_t> offsets_;
  std::map<timestamp_t, uint64_t> txns_;
};
}  // namespace terrier::logging
