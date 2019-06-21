// Copyright (c) 2012, Susumu Yata
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

#ifndef MADOKA_FILE_H
#define MADOKA_FILE_H

#include "util.h"

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

typedef enum {
  MADOKA_FILE_CREATE    = 1 << 0,
  MADOKA_FILE_TRUNCATE  = 1 << 1,
  MADOKA_FILE_READONLY  = 1 << 2,
  MADOKA_FILE_WRITABLE  = 1 << 3,
  MADOKA_FILE_SHARED    = 1 << 4,
  MADOKA_FILE_PRIVATE   = 1 << 5,
  MADOKA_FILE_ANONYMOUS = 1 << 6,
  MADOKA_FILE_HUGETLB   = 1 << 7,
  MADOKA_FILE_PRELOAD   = 1 << 8
} madoka_file_flag;

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#ifdef __cplusplus
#include "exception.h"

namespace madoka {

enum FileFlag {
  FILE_CREATE    = MADOKA_FILE_CREATE,
  FILE_TRUNCATE  = MADOKA_FILE_TRUNCATE,
  FILE_READONLY  = MADOKA_FILE_READONLY,
  FILE_WRITABLE  = MADOKA_FILE_WRITABLE,
  FILE_SHARED    = MADOKA_FILE_SHARED,
  FILE_PRIVATE   = MADOKA_FILE_PRIVATE,
  FILE_ANONYMOUS = MADOKA_FILE_ANONYMOUS,
  FILE_HUGETLB   = MADOKA_FILE_HUGETLB,
  FILE_PRELOAD   = MADOKA_FILE_PRELOAD
};

class FileImpl;

class File {
 public:
  File() noexcept;
  ~File() noexcept;

  void create(const char *path, std::size_t size, int flags = 0);
  void open(const char *path, int flags = 0);
  void close() noexcept;

  void load(const char *path, int flags = 0);
  void save(const char *path, int flags = 0) const;

  void *addr() const noexcept;
  std::size_t size() const noexcept;
  int flags() const noexcept;

  void swap(File *file) noexcept;

 private:
  FileImpl *impl_;

  // Disallows copy and assignment.
  File(const File &);
  File &operator=(const File &);
};

}  // namespace madoka
#endif  // __cplusplus

#endif  // MADOKA_FILE_H
