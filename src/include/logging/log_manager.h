#pragma once

namespace terrier::logging {

/**
 * A LogManager allocates free buffers to work threads, and writes a buffer to
 * a log file after the buffer is returned by a work thread, where the buffer
 * either gets full or the transaction commits.
 */
class LogManager{
 public:
  /**
   * A LogManager is only initiated with parameters when the system starts,
   * and should never be copied or moved.
   */
  LogManager() = delete;
  DISALLOW_COPY_AND_MOVE(LogManager);

  /**
   * Constructs a LogManager with the given logging switch, using the given
   * directory to store log files if logging is enabled.
   *
   * @param enabled whether the LogManager is enabled
   * @param dir the directory where the log file is stored
   */
  LogManager(bool enabled, std::string dir) : enabled_(false), dir_(dir) {
    std::filesystem::create_directories(dir_);
    file_name_ = "terrier.log";
    std::string path = dir_ + "/" + file_name_;
    file_.open(path, std::ios::out | std::ios::app | std::ios::binary);
  }

  /**
   * Destructs a LogManager, closes the log file.
   */
  ~LogManager() {
    file_.close();
  }

  /**
   * Get the log file path.
   *
   * @return the log file path
   */
  bool IsEnabled() {
    return enabled_;
  }

  /**
   * Get the log file path.
   *
   * @return the log file path
   */
  std::string GetPath() {
    return file_ + "/" + file_name_;
  }

  /**
   * Get the log file stream.
   *
   * @return the log file stream
   */
  std::ofstream &GetFile() {
    return file_;
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