#include "storage/posix_io_wrappers.h"

namespace noisepage::storage {

void PosixIoWrappers::Close(int fd) {
  while (true) {
    int ret = close(fd);
    if (ret == -1) {
      if (errno == EINTR) continue;
      throw std::runtime_error("Failed to close file with errno " + std::to_string(errno));
    }
    return;
  }
}

uint32_t PosixIoWrappers::ReadFully(int fd, void *buf, size_t nbyte) {
  ssize_t bytes_read = 0;
  while (bytes_read < static_cast<ssize_t>(nbyte)) {
    ssize_t ret = read(fd, reinterpret_cast<char *>(buf) + bytes_read, static_cast<ssize_t>(nbyte) - bytes_read);
    if (ret == -1) {
      if (errno == EINTR) continue;
      throw std::runtime_error("Read failed with errno " + std::to_string(errno));
    }
    if (ret == 0) break;  // no more bytes left in the file
    bytes_read += ret;
  }
  return static_cast<uint32_t>(bytes_read);
}

void PosixIoWrappers::WriteFully(int fd, const void *buf, size_t nbyte) {
  ssize_t written = 0;
  while (static_cast<size_t>(written) < nbyte) {
    ssize_t ret = write(fd, reinterpret_cast<const char *>(buf) + written, nbyte - written);
    if (ret == -1) {
      if (errno == EINTR) continue;
      throw std::runtime_error("Write to log file failed with errno " + std::to_string(errno));
    }
    written += ret;
  }
}

}  // namespace noisepage::storage
