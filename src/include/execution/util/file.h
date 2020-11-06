#pragma once

#include <cstddef>
#include <string>

#include "common/macros.h"

namespace noisepage::execution::util {

/**
 * Handle to a file.
 */
class File {
  // An invalid file descriptor
  static constexpr int32_t INVALID_DESCRIPTOR = -1;

 public:
  /**
   * Flags to control opening or creating files.
   *
   * The first five flags (i.e., FLAG_(OPEN|CREATE)_* are mutually exclusive - only one should be
   * used during construction or opening a file.
   *
   * Additionally, all calls to open must include at least one of FLAG_READ or FLAG_WRITE to
   * indicate the intention with the file.
   */
  enum Flags {
    FLAG_OPEN = 1 << 0,             // Opens a file, only if it exists.
    FLAG_CREATE = 1 << 1,           // Creates a new file, only if it doesn't already exist.
    FLAG_OPEN_ALWAYS = 1 << 2,      // May create a new file.
    FLAG_CREATE_ALWAYS = 1 << 3,    // May overwrite an old file.
    FLAG_OPEN_TRUNCATED = 1 << 4,   // Opens a file and truncates it, only if it exists.
    FLAG_READ = 1 << 5,             // Opens a file with read permissions.
    FLAG_WRITE = 1 << 6,            // Opens a file with write permissions.
    FLAG_APPEND = 1 << 7,           // Opens a file with write permissions, positioned at the end.
    FLAG_DELETE_ON_CLOSE = 1 << 8,  // Will delete the file upon closing.
  };

  /**
   * Possible errors.
   */
  enum class Error {
    ACCESS_DENIED,
    EXISTS,
    FAILED,
    IO,
    IN_USE,
    NO_MEMORY,
    NO_SPACE,
    NOT_FOUND,
    OK,
    TOO_MANY_OPENED,
    // Put new entries above this comment
    MAX
  };

  /**
   * Used as origin during file seeks. The enumeration values matter here.
   */
  enum class Whence { FROM_BEGIN = 0, FROM_CURRENT = 1, FROM_END = 2 };

  /**
   * Create a file handle to no particular file.
   */
  File() = default;

  /**
   * Open a handle to a file at the given path and flags.
   * @param path Path to file.
   * @param flags Flags to use.
   */
  File(const std::string_view &path, uint32_t flags);

  /**
   * File handles cannot be copied.
   */
  DISALLOW_COPY_AND_MOVE(File);

  /**
   * Destructor.
   */
  ~File() { Close(); }

  /**
   * Open the file at the given path and with the provided access mode.
   * @param path The path to the file.
   * @param flags The access mode.
   */
  void Open(const std::string_view &path, uint32_t flags);

  /**
   * Create a file at the given path. If a file exists already, an exception is thrown.
   * @param path Path.
   */
  void Create(const std::string_view &path);

  /**
   * Create a temporary file.
   */
  void CreateTemp(bool delete_on_close);

  /**
   * Make a best-effort attempt to read @em len bytes of data from the current file position into
   * the provided output byte array @em data.
   * @param[out] data Buffer where file contents are written to. Must be large enough to store at
   *                  least @em len bytes of data.
   * @param len The maximum number of bytes to read.
   * @return The number of bytes read if >= 0, or the error code if < 0.
   */
  int32_t ReadFull(std::byte *data, std::size_t len) const;

  /**
   * Make a best-effort attempt to read @em len bytes of data from a specific position in the file
   * into the provided output byte array @em data.
   * @param offset The offset in the file to read from.
   * @param[out] data Buffer where file contents are written to. Must be large enough to store at
   *                  least @em len bytes of data.
   * @param len The maximum number of bytes to read.
   * @return The number of bytes read if >= 0, or the error code if < 0.
   */
  int32_t ReadFullFromPosition(std::size_t offset, std::byte *data, std::size_t len) const;

  /**
   * Attempt to read @em len bytes of data from the current file position into the provided output
   * buffer @em data. No effort is made to guarantee that all requested bytes are read, and no retry
   * logic is used to catch spurious failures.
   * @param[out] data Buffer where file contents are written to. Must be large enough to store at
   *                  least @em len bytes of data.
   * @param len The maximum number of bytes to read.
   * @return The number of bytes written if >= 0, or the error code if < 0.
   */
  int32_t Read(std::byte *data, std::size_t len) const;

  /**
   * Make a best-effort attempt to write @em len bytes from @em data into the file.
   * @param data The data to write.
   * @param len The number of bytes to write.
   * @return The number of bytes written if >= 0, or the error code if < 0.
   */
  int32_t WriteFull(const std::byte *data, std::size_t len) const;

  /**
   * Make a best-effort attempt to write @em len bytes from @em data into the file at the specified
   * byte offset.
   * @param offset Offset into the file to write.
   * @param data The data to write.
   * @param len The number of bytes to write.
   * @return The number of bytes written if >= 0, or the error code if < 0.
   */
  int32_t WriteFullAtPosition(std::size_t offset, const std::byte *data, std::size_t len) const;

  /**
   * Attempt to write @em len bytes from @em data into the file. No effort is made to write all
   * requested bytes, and no retry logic is used to catch spurious failures.
   * @param data The data to write.
   * @param len The number of bytes to write.
   * @return The number of bytes written if >= 0, or the error code if < 0.
   */
  int32_t Write(const std::byte *data, std::size_t len) const;

  /**
   * Repositions the file offset to @em offset bytes relative to the origin defined by @em whence.
   * Cases:
   * FROM_BEGIN -> repositions file to @em offset bytes from the start of the file.
   * FROM_CUR -> repositions file to @em offset bytes from the current position.
   * FROM_END -> repositions file to @em offset bytes from the end of the file
   *
   * @param whence Relative position to seek by.
   * @param offset Number of bytes to shift position.
   * @return The result position in the file relative tot he start; -1 in case of error.
   */
  int64_t Seek(Whence whence, int64_t offset) const;

  /**
   * Flush any in-memory buffered contents into the file.
   * @return True if the flush was successful; false otherwise.
   */
  bool Flush() const;

  /**
   * @return The size of the file, if valid and open, in bytes. Return -1 on error.
   */
  int64_t Length();

  /**
   * @return The error indicator.
   */
  Error GetErrorIndicator() const noexcept { return error_; }

  /**
   * @return True if there is an error; false otherwise.
   */
  bool HasError() const noexcept { return error_ != Error::OK; }

  /**
   * @return True if the file is open; false otherwise.
   */
  bool IsOpen() const noexcept { return fd_ != INVALID_DESCRIPTOR; }

  /**
   * @return True if this file was created new; false otherwise.
   */
  bool IsCreated() const noexcept { return created_; }

  /**
   * Close the file. Does nothing if already closed, or not open.
   */
  void Close();

  /**
   * Convert an error code into a string. Used for logging.
   * @param error The error code.
   * @return String text describing the error.
   */
  static std::string ErrorToString(Error error);

 private:
  // Initialization logic.
  void Initialize(const std::string_view &path, uint32_t flags);

  // Convert an error number into a high level error
  static Error OsErrorToFileError(int saved_errno);

 private:
  // The file descriptor.
  int32_t fd_{INVALID_DESCRIPTOR};
  // Was the file created?
  bool created_{false};
  // Error indicator.
  Error error_{Error::FAILED};
};

}  // namespace noisepage::execution::util
