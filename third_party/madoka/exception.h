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

#ifndef MADOKA_EXCEPTION_H
#define MADOKA_EXCEPTION_H

#include "util.h"

#ifdef __cplusplus
namespace madoka {

// These macros transform a line number to a string constant.
#define MADOKA_INT_TO_STR(value) #value
#define MADOKA_LINE_TO_STR(line) MADOKA_INT_TO_STR(line)
#define MADOKA_LINE_STR MADOKA_LINE_TO_STR(__LINE__)

// MADOKA_THROW() throws an exception with `message'.
#define MADOKA_THROW(message) \
  (throw ::madoka::Exception(__FILE__ ":" MADOKA_LINE_STR ": " message))

// MADOKA_THROW_IF() throws an exception if `condition' is true. Also,
// `condition' is used as the error message of that exception.
#define MADOKA_THROW_IF(condition) \
  static_cast<void>((!(condition)) || (MADOKA_THROW(#condition), 0))

class Exception {
 public:
  Exception() noexcept : what_("") {}
  ~Exception() noexcept {}

  explicit Exception(const char *what) noexcept : what_(what) {}

  Exception(const Exception &exception) noexcept : what_(exception.what_) {}

  Exception &operator=(const Exception &exception) noexcept {
    what_ = exception.what_;
    return *this;
  }

  const char *what() const noexcept {
    return what_;
  }

 private:
  const char *what_;
};

}  // namespace madoka
#endif  // __cplusplus

#endif  // MADOKA_EXCEPTION_H
