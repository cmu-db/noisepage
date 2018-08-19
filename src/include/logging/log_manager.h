#pragma once
#include <boost/filesystem.hpp>
#include <fstream>
#include <string>
#include <utility>
#include "common/macros.h"
#include "logging/log_common.h"

namespace terrier::logging {

/**
 * A LogManager allocates free buffers to work threads, and writes a buffer to
 * a log file after the buffer is returned by a work thread, where either the
 * buffer gets full or a transaction commits.
 */
class LogManager {
 public:
  /**
   * @brief A LogManager is only initiated with parameters when the system
   * starts, and should never be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(LogManager);

  /**
   * @brief Constructs a LogManager with the given logging switch, using the
   * given directory to store log files if logging is enabled.
   *
   * @param enabled whether the LogManager is enabled
   * @param dir the directory where the log file is stored
   */
  LogManager(bool enabled, std::string dir) : enabled_(false), dir_(std::move(dir)) {
    // TODO(Aaron): Eventually we want to look into replacing this with C++17's
    // native filesystem library, but Clang support isn't there yet.
    boost::filesystem::create_directories(dir_);
    file_name_ = LOG_FILE_NAME;
    std::string path = dir_ + "/" + file_name_;
    file_.open(path, std::ios::out | std::ios::app | std::ios::binary);
  }

  /**
   * @brief Destructs a LogManager, closes the log file.
   */
  ~LogManager() { file_.close(); }

  /**
   * @brief Gets the log file path.
   *
   * @return the log file path
   */
  bool IsEnabled() { return enabled_; }

  /**
   * @brief Gets the log file path.
   *
   * @return the log file path
   */
  std::string GetPath() { return dir_ + "/" + file_name_; }

  /**
   * @brief Gets the log file stream.
   *
   * @return the log file stream
   */
  std::ofstream &GetFile() { return file_; }

  /**
   * @brief Writes changes in the output file stream to the underlying output
   * sequence and resets the stream.
   */
  void Flush() {
    file_.flush();
    // Clear fail and eof bits
    file_.clear();
    // Reset the insert position
    file_.seekp(0, std::ios_base::beg);
  }

 private:
  // TODO(Aaron): ofstream is not thread safe, might need to change it if more
  // than one logger thread is used
  bool enabled_;
  std::string dir_;
  std::string file_name_;
  std::ofstream file_;
};
}  // namespace terrier::logging
