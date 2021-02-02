#pragma once

#include "common/macros.h"

namespace noisepage::storage {

/**
 * Modernized wrappers around Posix I/O sys calls to hide away the ugliness and use exceptions for error reporting.
 */
class PosixIoWrappers {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(PosixIoWrappers);

  // TODO(Tianyu): Use a better exception than runtime_error.
  /**
   * Wrapper around posix open call
   * @tparam Args type of varlen arguments
   * @param path posix path arg
   * @param oflag posix oflag arg
   * @param args posix mode arg
   * @throws runtime_error if the underlying posix call failed
   * @return a non-negative integer that is the file descriptor if the opened file.
   */
  template <class... Args>
  static int Open(const char *path, int oflag, Args... args);

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

extern template int PosixIoWrappers::Open<>(const char *path, int oflag);
extern template int PosixIoWrappers::Open<int>(const char *path, int oflag, int mode);

}  // namespace noisepage::storage
