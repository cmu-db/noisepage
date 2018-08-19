#pragma once
#include <fstream>
#include <string>
#include "common/macros.h"
#include "loggers/storage_logger.h"
#include "logging/log_common.h"

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
   * @brief Replays all the log records by the order of transactions.
   */
  void Replay();

 private:
  // TODO(Aaron): fstream is not thread safe, might need to change it if more
  // than one logger thread is used
  std::string dir_;
  std::string file_name_;
  std::fstream file_;
};
}  // namespace terrier::logging
