// Copyright (c) 2012-2015, Susumu Yata
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
//    this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.

#include "file.h"

#ifdef _WIN32
 #include <sys/types.h>
 #include <sys/stat.h>
 #include <windows.h>
 #ifdef max
  #undef max
 #endif  // max
#else  // _WIN32
 #include <fcntl.h>
 #include <sys/mman.h>
 #include <sys/types.h>
 #include <sys/stat.h>
 #include <unistd.h>
 #ifndef MAP_ANONYMOUS
  #define MAP_ANONYMOUS MAP_ANON
 #endif  // MAP_ANONYMOUS
#endif  // _WIN32

#include <cstring>
#include <limits>
#include <new>

namespace madoka {

class FileImpl {
 public:
  FileImpl() noexcept;
  ~FileImpl() noexcept;

  void create(const char *path, std::size_t size, int flags);
  void open(const char *path, int flags);
  void close() noexcept;

  void load(const char *path, int flags);
  void save(const char *path, int flags);

  void *addr() const noexcept {
    return addr_;
  }
  std::size_t size() const noexcept {
    return size_;
  }
  int flags() const noexcept {
    return flags_;
  }

  void swap(FileImpl *file) noexcept;

 private:
  void *addr_;
  std::size_t size_;
  int flags_;
#ifdef _WIN32
  HANDLE file_handle_;
  HANDLE map_handle_;
  LPVOID view_addr_;
#else  // _WIN32
  int fd_;
  void *map_addr_;
#endif  // _WIN32

  void create_(const char *path, std::size_t size, int flags);
  void open_(const char *path, int flags);

  void load_(const char *path, int flags);

  // Disallows copy and assignment.
  FileImpl(const FileImpl &);
  FileImpl &operator=(const FileImpl &);
};

#ifdef _WIN32

FileImpl::FileImpl() noexcept
  : addr_(NULL), size_(0), flags_(0), file_handle_(INVALID_HANDLE_VALUE),
    map_handle_(INVALID_HANDLE_VALUE), view_addr_(NULL) {}

FileImpl::~FileImpl() noexcept {
  if (view_addr_ != NULL) {
    ::UnmapViewOfFile(view_addr_);
  }
  if (map_handle_ != INVALID_HANDLE_VALUE) {
    ::CloseHandle(map_handle_);
  }
  if (file_handle_ != INVALID_HANDLE_VALUE) {
    ::CloseHandle(file_handle_);
  }
}

#else  // _WIN32

FileImpl::FileImpl() noexcept
  : addr_(NULL), size_(0), flags_(0), fd_(-1), map_addr_(MAP_FAILED) {}

FileImpl::~FileImpl() noexcept {
  if (map_addr_ != MAP_FAILED) {
    ::munmap(map_addr_, size_);
  }
  if (fd_ != -1) {
    ::close(fd_);
  }
}

#endif  // _WIN32

void FileImpl::create(const char *path, std::size_t size, int flags) {
  FileImpl new_file;
  new_file.create_(path, size, flags);
  new_file.swap(this);
}

void FileImpl::open(const char *path, int flags) {
  FileImpl new_file;
  new_file.open_(path, flags);
  new_file.swap(this);
}

void FileImpl::close() noexcept {
  FileImpl().swap(this);
}

void FileImpl::load(const char *path, int flags) {
  FileImpl new_file;
  new_file.load_(path, flags);
  new_file.swap(this);
}

void FileImpl::save(const char *path, int flags) {
  FileImpl file;
  file.create(path, size(), flags);
  std::memcpy(file.addr(), addr(), size());
}

void FileImpl::swap(FileImpl *file) noexcept {
  util::swap(addr_, file->addr_);
  util::swap(size_, file->size_);
  util::swap(flags_, file->flags_);
#ifdef _WIN32
  util::swap(file_handle_, file->file_handle_);
  util::swap(map_handle_, file->map_handle_);
  util::swap(view_addr_, file->view_addr_);
#else  // _WIN32
  util::swap(fd_, file->fd_);
  util::swap(map_addr_, file->map_addr_);
#endif  // _WIN32
}

void FileImpl::load_(const char *path, int flags) {
  MADOKA_THROW_IF(path == NULL);

  const int VALID_FLAGS = FILE_HUGETLB;
  MADOKA_THROW_IF(flags & ~VALID_FLAGS);

  File file;
  file.open(path, FILE_READONLY);

  create(NULL, file.size(), flags);
  std::memcpy(addr(), file.addr(), size());
}

#ifdef _WIN32

DWORD get_access_flags(int flags) {
  DWORD access_flags = 0;
  if (flags & FILE_READONLY) {
    access_flags |= GENERIC_READ;
  }
  if (flags & FILE_WRITABLE) {
    access_flags |= GENERIC_READ | GENERIC_WRITE;
  }
  return access_flags;
}

DWORD get_disposition_type(int flags) {
  DWORD disposition_type = 0;
  if (flags & FILE_CREATE) {
    if (flags & FILE_TRUNCATE) {
      disposition_type = CREATE_ALWAYS;
    } else {
      disposition_type = CREATE_NEW;
    }
  } else {
    disposition_type = OPEN_EXISTING;
  }
  return disposition_type;
}

DWORD get_map_type(int flags) {
  DWORD map_type = 0;
  if (flags & FILE_READONLY) {
    map_type = PAGE_READONLY;
  }
  if (flags & FILE_WRITABLE) {
    if (flags & FILE_PRIVATE) {
      map_type = PAGE_WRITECOPY;
    } else {
      map_type = PAGE_READWRITE;
    }
  }
  return map_type;
}

DWORD get_view_type(int flags) {
  DWORD view_type = 0;
  if (flags & FILE_READONLY) {
    view_type = FILE_MAP_READ;
  }
  if (flags & FILE_WRITABLE) {
    if (flags & FILE_PRIVATE) {
      view_type = FILE_MAP_COPY;
    } else {
      view_type = FILE_MAP_WRITE;
    }
  }
  return view_type;
}

void FileImpl::create_(const char *path, std::size_t size, int flags) {
  const int VALID_FLAGS = FILE_TRUNCATE | FILE_HUGETLB;
  MADOKA_THROW_IF(flags & ~VALID_FLAGS);

  flags |= FILE_WRITABLE;
  flags &= ~FILE_HUGETLB;

  if (path == NULL) {
    MADOKA_THROW_IF(flags & FILE_TRUNCATE);
    flags |= FILE_PRIVATE | FILE_ANONYMOUS;
  } else {
    flags |= FILE_CREATE | FILE_SHARED;
    if (~flags & FILE_TRUNCATE) {
      struct __stat64 stat;
      MADOKA_THROW_IF(::_stat64(path, &stat) == 0);
    }

    file_handle_ = ::CreateFileA(path, get_access_flags(flags),
                                 FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
                                 get_disposition_type(flags),
                                 FILE_ATTRIBUTE_NORMAL, NULL);
    MADOKA_THROW_IF(file_handle_ == INVALID_HANDLE_VALUE);

    if (size != 0) {
      const LONG size_low = static_cast<LONG>(size & 0xFFFFFFFFU);
      LONG size_high = static_cast<LONG>(
          static_cast<UInt64>(size) >> 32);
      const DWORD file_pos = ::SetFilePointer(file_handle_, size_low,
                                              &size_high, FILE_BEGIN);
      MADOKA_THROW_IF((file_pos == INVALID_SET_FILE_POINTER) &&
                      (::GetLastError() != 0));
      MADOKA_THROW_IF(::SetEndOfFile(file_handle_) == 0);
    }
  }

  if (size == 0) {
    static char DUMMY_BUF[1];
    addr_ = DUMMY_BUF;
  } else {
    const DWORD size_low = static_cast<DWORD>(size & 0xFFFFFFFFU);
    const DWORD size_high = static_cast<DWORD>(
        static_cast<UInt64>(size) >> 32);
    map_handle_ = ::CreateFileMapping(file_handle_, NULL, get_map_type(flags),
                                      size_high, size_low, NULL);
    MADOKA_THROW_IF(map_handle_ == INVALID_HANDLE_VALUE);

    view_addr_ = ::MapViewOfFile(map_handle_, get_view_type(flags), 0, 0, 0);
    MADOKA_THROW_IF(view_addr_ == NULL);

    addr_ = view_addr_;
  }
  size_ = size;
  flags_ = flags;
}

void FileImpl::open_(const char *path, int flags) {
  MADOKA_THROW_IF(path == NULL);

  const int VALID_FLAGS = FILE_READONLY | FILE_PRIVATE |
                          FILE_HUGETLB | FILE_PRELOAD;
  MADOKA_THROW_IF(flags & ~VALID_FLAGS);

  if (~flags & FILE_READONLY) {
    flags |= FILE_WRITABLE;
  }
  if (~flags & FILE_PRIVATE) {
    flags |= FILE_SHARED;
  }
  flags &= ~FILE_HUGETLB;

  struct __stat64 stat;
  MADOKA_THROW_IF(::_stat64(path, &stat) == -1);
  MADOKA_THROW_IF(stat.st_size < 0);
  MADOKA_THROW_IF(static_cast<UInt64>(stat.st_size) >
                  std::numeric_limits<std::size_t>::max());
  const std::size_t size = static_cast<std::size_t>(stat.st_size);

  file_handle_ = ::CreateFileA(path, get_access_flags(flags),
                               FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
                               get_disposition_type(flags),
                               FILE_ATTRIBUTE_NORMAL, NULL);
  MADOKA_THROW_IF(file_handle_ == NULL);

  if (size == 0) {
    static char DUMMY_BUF[1];
    addr_ = DUMMY_BUF;
  } else {
    map_handle_ = ::CreateFileMapping(file_handle_, NULL, get_map_type(flags),
                                      0, 0, NULL);
    MADOKA_THROW_IF(map_handle_ == NULL);

    view_addr_ = ::MapViewOfFile(map_handle_, get_view_type(flags), 0, 0, 0);
    MADOKA_THROW_IF(view_addr_ == NULL);

    addr_ = view_addr_;
  }
  size_ = size;
  flags_ = flags;

  if (flags & FILE_PRELOAD) {
    volatile UInt64 count = 0;
    for (std::size_t offset = 0; offset < size_; offset += 1024) {
      count += *(static_cast<UInt8 *>(addr_) + offset);
    }
  }
}

#else  // _WIN32

namespace {

int get_open_flags(int flags) noexcept {
  int open_flags = 0;
  if (flags & FILE_CREATE) {
    open_flags |= O_CREAT;
  }
  if (flags & FILE_TRUNCATE) {
    open_flags |= O_TRUNC;
  }
  if (flags & FILE_READONLY) {
    open_flags |= O_RDONLY;
  }
  if (flags & FILE_WRITABLE) {
    open_flags |= O_RDWR;
  }
  return open_flags;
}

int get_prot_flags(int flags) noexcept {
  int prot_flags = PROT_READ;
  if (flags & FILE_WRITABLE) {
    prot_flags |= PROT_WRITE;
  }
  return prot_flags;
}

int get_map_flags(int flags) noexcept {
  int map_flags = 0;
  if (flags & FILE_SHARED) {
    map_flags |= MAP_SHARED;
  }
  if (flags & FILE_PRIVATE) {
    map_flags |= MAP_PRIVATE;
  }
  if (flags & FILE_ANONYMOUS) {
    map_flags |= MAP_ANONYMOUS;
  }
#ifdef MAP_HUGETLB
  if (flags & FILE_HUGETLB) {
    map_flags |= MAP_HUGETLB;
  }
#endif  // MAP_HUGETLB
  return map_flags;
}

}  // namespace

void FileImpl::create_(const char *path, std::size_t size, int flags) {
  const int VALID_FLAGS = FILE_TRUNCATE | FILE_HUGETLB;
  MADOKA_THROW_IF(flags & ~VALID_FLAGS);

  flags |= FILE_WRITABLE;

  if (path == NULL) {
    MADOKA_THROW_IF(flags & FILE_TRUNCATE);
    flags |= FILE_PRIVATE | FILE_ANONYMOUS;
  } else {
    flags |= FILE_CREATE | FILE_SHARED;
    if (~flags & FILE_TRUNCATE) {
      struct stat stat;
      MADOKA_THROW_IF(::stat(path, &stat) == 0);
    }

    fd_ = ::open(path, get_open_flags(flags), 0666);
    MADOKA_THROW_IF(fd_ == -1);

    if (size != 0) {
      MADOKA_THROW_IF(::ftruncate(fd_, size) == -1);
    }
  }

  if (size == 0) {
    static char DUMMY_BUF[1];
    addr_ = DUMMY_BUF;
  } else {
     map_addr_ = ::mmap(NULL, size, get_prot_flags(flags),
                        get_map_flags(flags), fd_, 0);
#ifdef MAP_HUGETLB
    if ((map_addr_ == MAP_FAILED) && (flags & FILE_HUGETLB)) {
      flags &= ~FILE_HUGETLB;
      map_addr_ = ::mmap(NULL, size, get_prot_flags(flags),
                         get_map_flags(flags), fd_, 0);
    }
#endif  // MAP_HUGETLB
    MADOKA_THROW_IF(map_addr_ == MAP_FAILED);
    addr_ = map_addr_;
  }
  size_ = size;
  flags_ = flags;
}

void FileImpl::open_(const char *path, int flags) {
  MADOKA_THROW_IF(path == NULL);

  const int VALID_FLAGS = FILE_READONLY | FILE_PRIVATE |
                          FILE_HUGETLB | FILE_PRELOAD;
  MADOKA_THROW_IF(flags & ~VALID_FLAGS);

  if (~flags & FILE_READONLY) {
    flags |= FILE_WRITABLE;
  }
  if (~flags & FILE_PRIVATE) {
    flags |= FILE_SHARED;
  }

  struct stat stat;
  MADOKA_THROW_IF(::stat(path, &stat) == -1);
  MADOKA_THROW_IF(stat.st_size < 0);
  MADOKA_THROW_IF(static_cast<UInt64>(stat.st_size) >
                  std::numeric_limits<std::size_t>::max());
  const std::size_t size = static_cast<std::size_t>(stat.st_size);

  fd_ = ::open(path, get_open_flags(flags));
  MADOKA_THROW_IF(fd_ == -1);

  if (size == 0) {
    static char DUMMY_BUF[1];
    addr_ = DUMMY_BUF;
  } else {
    map_addr_ = ::mmap(NULL, size, get_prot_flags(flags),
                       get_map_flags(flags), fd_, 0);
#ifdef MAP_HUGETLB
    if ((map_addr_ == MAP_FAILED) && (flags & FILE_HUGETLB)) {
      flags &= ~FILE_HUGETLB;
      map_addr_ = ::mmap(NULL, size, get_prot_flags(flags),
                         get_map_flags(flags), fd_, 0);
    }
#endif  // MAP_HUGETLB
    MADOKA_THROW_IF(map_addr_ == MAP_FAILED);
    addr_ = map_addr_;
  }
  size_ = size;
  flags_ = flags;

  if (flags & FILE_PRELOAD) {
    volatile UInt64 count = 0;
    for (std::size_t offset = 0; offset < size_; offset += 1024) {
      count += *static_cast<UInt8 *>(addr_) + offset;
    }
  }
}

#endif  // _WIN32

File::File() noexcept : impl_(NULL) {}

File::~File() noexcept {
  delete impl_;
}

void File::create(const char *path, std::size_t size, int flags) {
  if (impl_ == NULL) {
    impl_ = new (std::nothrow) FileImpl;
    MADOKA_THROW_IF(impl_ == NULL);
  }
  impl_->create(path, size, flags);
}

void File::open(const char *path, int flags) {
  if (impl_ == NULL) {
    impl_ = new (std::nothrow) FileImpl;
    MADOKA_THROW_IF(impl_ == NULL);
  }
  impl_->open(path, flags);
}

void File::close() noexcept {
  File().swap(this);
}

void File::load(const char *path, int flags) {
  if (impl_ == NULL) {
    impl_ = new (std::nothrow) FileImpl;
    MADOKA_THROW_IF(impl_ == NULL);
  }
  impl_->load(path, flags);
}

void File::save(const char *path, int flags) const {
  if (impl_ != NULL) {
    impl_->save(path, flags);
  }
}

void *File::addr() const noexcept {
  return (impl_ != NULL) ? impl_->addr() : NULL;
}

std::size_t File::size() const noexcept {
  return (impl_ != NULL) ? impl_->size() : 0;
}

int File::flags() const noexcept {
  return (impl_ != NULL) ? impl_->flags() : 0;
}

void File::swap(File *file) noexcept {
  util::swap(impl_, file->impl_);
}

}  // namespace madoka
