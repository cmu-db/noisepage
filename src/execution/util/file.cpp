#include "execution/util/file.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>

#include "execution/util/execution_common.h"

namespace noisepage::execution::util {

// Interrupt handling logic is inspired by the Chromium project's multi-purpose file.

#if defined(NDEBUG)

#define HANDLE_EINTR(x)                                     \
  ({                                                        \
    decltype(x) eintr_wrapper_result;                       \
    do {                                                    \
      eintr_wrapper_result = (x);                           \
    } while (eintr_wrapper_result == -1 && errno == EINTR); \
    eintr_wrapper_result;                                   \
  })

#else

#define HANDLE_EINTR(x)                                                                      \
  ({                                                                                         \
    uint32_t eintr_wrapper_counter = 0;                                                      \
    decltype(x) eintr_wrapper_result;                                                        \
    do {                                                                                     \
      eintr_wrapper_result = (x);                                                            \
    } while (eintr_wrapper_result == -1 && errno == EINTR && eintr_wrapper_counter++ < 100); \
    eintr_wrapper_result;                                                                    \
  })

#endif

#define IGNORE_EINTR(x)                                   \
  ({                                                      \
    decltype(x) eintr_wrapper_result;                     \
    do {                                                  \
      eintr_wrapper_result = (x);                         \
      if (eintr_wrapper_result == -1 && errno == EINTR) { \
        eintr_wrapper_result = 0;                         \
      }                                                   \
    } while (0);                                          \
    eintr_wrapper_result;                                 \
  })

File::File(const std::string_view &path, uint32_t flags) : error_(Error::OK) { Initialize(path, flags); }

void File::Initialize(const std::string_view &path, uint32_t flags) {
  // Close the existing file if it's open
  Close();

  // Remember that the FLAG_(OPEN|CREATE)* flags are all mutually exclusive.
  // Only one of them should be specified. What follows is a series of checks to
  // ensure this constraint.

  int32_t open_flags = 0;

  if (flags & FLAG_CREATE) {  // NOLINT
    open_flags = O_CREAT | O_EXCL;
  }

  if (flags & FLAG_CREATE_ALWAYS) {  // NOLINT
    NOISEPAGE_ASSERT(!open_flags, "CreateAlways is mutually exclusive with other Open/Create flags");
    NOISEPAGE_ASSERT(flags & FLAG_WRITE, "CreateAlways must include Write flag");
    open_flags = O_CREAT | O_TRUNC;
  }

  if (flags & FLAG_OPEN_TRUNCATED) {  // NOLINT
    NOISEPAGE_ASSERT(!open_flags, "OpenTruncated is mutually exclusive with other Open/Create flags");
    NOISEPAGE_ASSERT(flags & FLAG_WRITE, "OpenTruncated must include Write flag");
    open_flags = O_TRUNC;
  }

  if (!open_flags && !(flags & FLAG_OPEN) && !(flags & FLAG_OPEN_ALWAYS)) {  // NOLINT
    NOISEPAGE_ASSERT(false, "Must provide one of Open/Create/OpenAlways/CreateAlways/OpenTruncated");
    errno = EOPNOTSUPP;
    error_ = Error::FAILED;
    return;
  }

  if (flags & FLAG_WRITE && flags & FLAG_READ) {  // NOLINT
    open_flags |= O_RDWR;
  } else if (flags & FLAG_WRITE) {  // NOLINT
    open_flags |= O_WRONLY;
  } else if (flags & FLAG_READ) {  // NOLINT
    open_flags |= O_RDONLY;
  } else if (!(flags & FLAG_READ) && !(flags & FLAG_APPEND) && !(flags & FLAG_OPEN_ALWAYS)) {  // NOLINT
    NOISEPAGE_ASSERT(false, "Flags must include one of Read, Append, or OpenAlways");
  }

  if (flags & FLAG_APPEND && flags & FLAG_READ) {  // NOLINT
    open_flags |= O_APPEND | O_RDWR;
  } else if (flags & FLAG_APPEND) {  // NOLINT
    open_flags |= O_APPEND | O_WRONLY;
  }

  int32_t mode = S_IRUSR | S_IWUSR;

  int32_t fd = open(path.data(), open_flags, mode);

  if (flags & FLAG_OPEN_ALWAYS) {  // NOLINT
    // Check if the file is open, create it otherwise.
    if (fd == INVALID_DESCRIPTOR) {
      open_flags |= O_CREAT;

      fd = HANDLE_EINTR(open(path.data(), open_flags, mode));

      if (fd >= 0) {
        created_ = true;
      }
    }
  }

  // Check error
  if (fd == INVALID_DESCRIPTOR) {
    error_ = OsErrorToFileError(errno);
    return;
  }

  // If the file was successfully created, set flag
  if (flags & (FLAG_CREATE_ALWAYS | FLAG_CREATE)) {  // NOLINT
    created_ = true;
  }

  // If requested to delete on close, unlink now. Unlinking removes the file
  // from the file system, but keeps the physical space so open handles can
  // read from it. When all open handles are closed, the file will be deleted
  // and its space will be reclaimed.
  if (flags & FLAG_DELETE_ON_CLOSE) {  // NOLINT
    unlink(path.data());
  }

  // Done
  fd_ = fd;
  error_ = Error::OK;
}

void File::Open(const std::string_view &path, uint32_t flags) { Initialize(path, flags); }

void File::Create(const std::string_view &path) { Open(path, FLAG_CREATE_ALWAYS | FLAG_WRITE); }

void File::CreateTemp(bool delete_on_close) {
  // Close the existing file if it's open
  Close();

  // Attempt to create a temporary file
  char tmp[] = "/tmp/noisepage-tpl.XXXXXX";
  int32_t fd = HANDLE_EINTR(mkstemp(tmp));

  // Fail?
  if (fd == INVALID_DESCRIPTOR) {
    error_ = OsErrorToFileError(errno);
    return;
  }

  // If we need to delete on close, unlink it now
  if (delete_on_close) {
    unlink(tmp);
  }

  // Done
  fd_ = fd;
  created_ = true;
  error_ = Error::OK;
}

int32_t File::ReadFull(std::byte *data, size_t len) const {
  NOISEPAGE_ASSERT(IsOpen(), "File must be open before reading");

  std::size_t bytes_read = 0;
  int32_t ret;
  do {
    ret = HANDLE_EINTR(read(fd_, data + bytes_read, len - bytes_read));
    if (ret <= 0) {
      break;
    }
    bytes_read += ret;
  } while (bytes_read < len);

  return bytes_read > 0 ? bytes_read : ret;
}

int32_t File::ReadFullFromPosition(std::size_t offset, std::byte *data, std::size_t len) const {
  NOISEPAGE_ASSERT(IsOpen(), "File must be open before reading");

  std::size_t bytes_read = 0;
  int32_t ret;
  do {
    ret = HANDLE_EINTR(pread(fd_, data + bytes_read, len - bytes_read, offset + bytes_read));
    if (ret <= 0) {
      break;
    }
    bytes_read += ret;
  } while (bytes_read < len);

  return bytes_read > 0 ? bytes_read : ret;
}

int32_t File::Read(std::byte *data, std::size_t len) const {
  NOISEPAGE_ASSERT(IsOpen(), "File must be open before reading");
  return HANDLE_EINTR(read(fd_, data, len));
}

int32_t File::WriteFull(const std::byte *data, size_t len) const {
  NOISEPAGE_ASSERT(IsOpen(), "File must be open before reading");

  std::size_t bytes_written = 0;
  int32_t ret;
  do {
    ret = HANDLE_EINTR(write(fd_, data + bytes_written, len - bytes_written));
    if (ret <= 0) {
      break;
    }
    bytes_written += ret;
  } while (bytes_written < len);

  return bytes_written > 0 ? bytes_written : ret;
}

int32_t File::WriteFullAtPosition(std::size_t offset, const std::byte *data, std::size_t len) const {
  NOISEPAGE_ASSERT(IsOpen(), "File must be open before reading");

  std::size_t bytes_written = 0;
  int32_t ret;
  do {
    ret = HANDLE_EINTR(pwrite(fd_, data + bytes_written, len - bytes_written, offset + bytes_written));
    if (ret <= 0) {
      break;
    }
    bytes_written += ret;
  } while (bytes_written < len);

  return bytes_written > 0 ? bytes_written : ret;
}

int32_t File::Write(const std::byte *data, std::size_t len) const {
  NOISEPAGE_ASSERT(IsOpen(), "File must be open before reading");
  return HANDLE_EINTR(write(fd_, data, len));
}

int64_t File::Seek(File::Whence whence, int64_t offset) const {
  static_assert(sizeof(int64_t) == sizeof(off_t), "off_t must be 64 bits");
  return lseek(fd_, static_cast<off_t>(offset), static_cast<int32_t>(whence));
}

bool File::Flush() const {
#if defined(OS_LINUX)
  return HANDLE_EINTR(fdatasync(fd_)) == 0;
#else
  return HANDLE_EINTR(fsync(fd_)) == 0;
#endif
}

int64_t File::Length() {
  NOISEPAGE_ASSERT(IsOpen(), "File must be open before reading");

  // Save the current position
  off_t curr_off = lseek(fd_, 0, SEEK_CUR);
  if (curr_off == -1) {
    error_ = OsErrorToFileError(errno);
    return -1;
  }

  // Seek to the end of the file, returning the new file position i.e., the size
  // of the file in bytes.
  off_t off = lseek(fd_, 0, SEEK_END);
  if (off == -1) {
    error_ = OsErrorToFileError(errno);
    return -1;
  }

  off_t restore = lseek(fd_, curr_off, SEEK_SET);
  if (restore == -1) {
    error_ = OsErrorToFileError(errno);
    return -1;
  }

  // Restore position
  return static_cast<std::size_t>(off);
}

void File::Close() {
  if (IsOpen()) {
    UNUSED_ATTRIBUTE auto ret = IGNORE_EINTR(close(fd_));
    NOISEPAGE_ASSERT(ret == 0, "Invalid return code from close()");
    fd_ = INVALID_DESCRIPTOR;
  }
}

std::string File::ErrorToString(File::Error error) {
  switch (error) {
    case Error::ACCESS_DENIED:
      return "ACCESS DENIED";
    case Error::EXISTS:
      return "FILE EXISTS";
    case Error::FAILED:
      return "FAILED";
    case Error::IO:
      return "IO ERROR";
    case Error::IN_USE:
      return "IN USE";
    case Error::NO_MEMORY:
      return "NO MEMORY";
    case Error::NO_SPACE:
      return "NO SPACE";
    case Error::NOT_FOUND:
      return "NOT FOUND";
    case Error::OK:
      return "OK";
    case Error::TOO_MANY_OPENED:
      return "TOO MANY OPENED";
    case Error::MAX:
      break;
  }
  UNREACHABLE("Impossible");
}

File::Error File::OsErrorToFileError(int saved_errno) {
  switch (saved_errno) {
    case EACCES:
    case EISDIR:
    case EROFS:
    case EPERM:
      return Error::ACCESS_DENIED;
    case EBUSY:
    case ETXTBSY:
      return Error::IN_USE;
    case EEXIST:
      return Error::EXISTS;
    case EIO:
      return Error::IO;
    case ENOENT:
      return Error::NOT_FOUND;
    case ENFILE:
    case EMFILE:
      return Error::TOO_MANY_OPENED;
    case ENOMEM:
      return Error::NO_MEMORY;
    case ENOSPC:
      return Error::NO_SPACE;
    default:
      return Error::FAILED;
  }
}

}  // namespace noisepage::execution::util
