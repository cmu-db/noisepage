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

#ifndef MADOKA_HEADER_H
#define MADOKA_HEADER_H

#include "util.h"

#ifdef __cplusplus
namespace madoka {

class Header {
 public:
  Header() noexcept
    : width_(0), width_mask_(0), depth_(0), max_value_(0), value_size_(0),
      seed_(0), table_size_(0), file_size_(0) {}
  ~Header() noexcept {}

  UInt64 width() const noexcept {
    return width_;
  }
  UInt64 width_mask() const noexcept {
    return width_mask_;
  }
  UInt64 depth() const noexcept {
    return depth_;
  }
  UInt64 max_value() const noexcept {
    return max_value_;
  }
  UInt64 value_size() const noexcept {
    return value_size_;
  }
  UInt64 seed() const noexcept {
    return seed_;
  }
  UInt64 table_size() const noexcept {
    return table_size_;
  }
  UInt64 file_size() const noexcept {
    return file_size_;
  }

  void set_width(UInt64 width) noexcept {
    width_ = width;
    width_mask_ = ((width & (width - 1)) == 0) ? (width - 1) : 0;
  }
  void set_depth(UInt64 depth) noexcept {
    depth_ = depth;
  }
  void set_max_value(UInt64 max_value) noexcept {
    max_value_ = max_value;
  }
  void set_value_size(UInt64 value_size) noexcept {
    value_size_ = value_size;
  }
  void set_seed(UInt64 seed) noexcept {
    seed_ = seed;
  }
  void set_table_size(UInt64 table_size) noexcept {
    table_size_ = table_size;
  }
  void set_file_size(UInt64 file_size) noexcept {
    file_size_ = file_size;
  }

 private:
  UInt64 width_;
  UInt64 width_mask_;
  UInt64 depth_;
  UInt64 max_value_;
  UInt64 value_size_;
  UInt64 seed_;
  UInt64 table_size_;
  UInt64 file_size_;

  // Disallows copy and assignment.
  Header(const Header &);
  Header &operator=(const Header &);
};

}  // namespace madoka
#endif  // __cplusplus

#endif  // MADOKA_HEADER_H
