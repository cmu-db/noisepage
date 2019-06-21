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

#include "sketch.h"

#include <cstring>
#include <limits>
#include <new>

extern "C" {

struct madoka_sketch_ {
  madoka_sketch_() : impl() {}
  ~madoka_sketch_() {}

  madoka::Sketch impl;
};

}  // extern "C"

extern "C" {

madoka_sketch *madoka_create(madoka_uint64 width, madoka_uint64 max_value,
                             const char *path, int flags, madoka_uint64 seed,
                             const char **what) try {
  madoka::Sketch impl;
  impl.create(width, max_value, path, flags, seed);
  madoka_sketch * const sketch = new (std::nothrow) madoka_sketch;
  MADOKA_THROW_IF(sketch == NULL);
  sketch->impl.swap(&impl);
  return sketch;
} catch (const madoka::Exception &ex) {
  if (what != NULL) {
    *what = ex.what();
  }
  return NULL;
}

madoka_sketch *madoka_open(const char *path, int flags,
                           const char **what) try {
  madoka::Sketch impl;
  impl.open(path, flags);
  madoka_sketch * const sketch = new (std::nothrow) madoka_sketch;
  MADOKA_THROW_IF(sketch == NULL);
  sketch->impl.swap(&impl);
  return sketch;
} catch (const madoka::Exception &ex) {
  if (what != NULL) {
    *what = ex.what();
  }
  return NULL;
}

void madoka_close(madoka_sketch *sketch) {
  if (sketch != NULL) {
    delete sketch;
  }
}

madoka_sketch *madoka_load(const char *path, int flags,
                           const char **what) try {
  madoka::Sketch impl;
  impl.load(path, flags);
  madoka_sketch * const sketch = new (std::nothrow) madoka_sketch;
  MADOKA_THROW_IF(sketch == NULL);
  sketch->impl.swap(&impl);
  return sketch;
} catch (const madoka::Exception &ex) {
  if (what != NULL) {
    *what = ex.what();
  }
  return NULL;
}

int madoka_save(const madoka_sketch *sketch, const char *path, int flags,
                const char **what) try {
  sketch->impl.save(path, flags);
  return 0;
} catch (const madoka::Exception &ex) {
  if (what != NULL) {
    *what = ex.what();
  }
  return -1;
}

madoka_uint64 madoka_get_width(const madoka_sketch *sketch) {
  return sketch->impl.width();
}

madoka_uint64 madoka_get_width_mask(const madoka_sketch *sketch) {
  return sketch->impl.width_mask();
}

madoka_uint64 madoka_get_depth(const madoka_sketch *sketch) {
  return sketch->impl.depth();
}

madoka_uint64 madoka_get_max_value(const madoka_sketch *sketch) {
  return sketch->impl.max_value();
}

madoka_uint64 madoka_get_value_mask(const madoka_sketch *sketch) {
  return sketch->impl.value_mask();
}

madoka_uint64 madoka_get_value_size(const madoka_sketch *sketch) {
  return sketch->impl.value_size();
}

madoka_uint64 madoka_get_seed(const madoka_sketch *sketch) {
  return sketch->impl.seed();
}

madoka_uint64 madoka_get_table_size(const madoka_sketch *sketch) {
  return sketch->impl.table_size();
}

madoka_uint64 madoka_get_file_size(const madoka_sketch *sketch) {
  return sketch->impl.file_size();
}

int madoka_get_flags(const madoka_sketch *sketch) {
  return sketch->impl.flags();
}

madoka_sketch_mode madoka_get_mode(const madoka_sketch *sketch) {
  switch (sketch->impl.mode()) {
    case madoka::SKETCH_EXACT_MODE: {
      return MADOKA_SKETCH_EXACT_MODE;
    }
    case madoka::SKETCH_APPROX_MODE: {
      return MADOKA_SKETCH_APPROX_MODE;
    }
  }
  return MADOKA_SKETCH_APPROX_MODE;
}

madoka_uint64 madoka_get(const madoka_sketch *sketch, const void *key_addr,
                         size_t key_size) {
  return sketch->impl.get(key_addr, key_size);
}

void madoka_set(madoka_sketch *sketch, const void *key_addr,
                size_t key_size, madoka_uint64 value) {
  sketch->impl.set(key_addr, key_size, value);
}

madoka_uint64 madoka_inc(madoka_sketch *sketch, const void *key_addr,
                         size_t key_size) {
  return sketch->impl.inc(key_addr, key_size);
}

madoka_uint64 madoka_add(madoka_sketch *sketch, const void *key_addr,
                         size_t key_size, madoka_uint64 value) {
  return sketch->impl.add(key_addr, key_size, value);
}

void madoka_clear(madoka_sketch *sketch) {
  sketch->impl.clear();
}

madoka_sketch *madoka_copy(const madoka_sketch *src, const char *path,
                           int flags, const char **what) try {
  madoka::Sketch impl;
  impl.copy(src->impl, path, flags);
  madoka_sketch * const sketch = new (std::nothrow) madoka_sketch;
  MADOKA_THROW_IF(sketch == NULL);
  sketch->impl.swap(&impl);
  return sketch;
} catch (const madoka::Exception &ex) {
  if (what != NULL) {
    *what = ex.what();
  }
  return NULL;
}

void madoka_filter(madoka_sketch *sketch, madoka_sketch_filter filter) {
  sketch->impl.filter(filter);
}

madoka_sketch *madoka_shrink(const madoka_sketch *src,
                             madoka_uint64 width, madoka_uint64 max_value,
                             madoka_sketch_filter filter, const char *path,
                             int flags, const char **what) try {
  madoka::Sketch impl;
  impl.shrink(src->impl, width, max_value, filter, path, flags);
  madoka_sketch * const sketch = new (std::nothrow) madoka_sketch;
  MADOKA_THROW_IF(sketch == NULL);
  sketch->impl.swap(&impl);
  return sketch;
} catch (const madoka::Exception &ex) {
  if (what != NULL) {
    *what = ex.what();
  }
  return NULL;
}

int madoka_merge(madoka_sketch *lhs, const madoka_sketch *rhs,
                 madoka_sketch_filter lhs_filter,
                 madoka_sketch_filter rhs_filter, const char **what) try {
  lhs->impl.merge(rhs->impl, lhs_filter, rhs_filter);
  return 0;
} catch (const madoka::Exception &ex) {
  if (what != NULL) {
    *what = ex.what();
  }
  return -1;
}

void madoka_swap(madoka_sketch *lhs, madoka_sketch *rhs) {
  lhs->impl.swap(&rhs->impl);
}

int madoka_inner_product(const madoka_sketch *lhs, const madoka_sketch *rhs,
                         double *inner_product, double *lhs_square_length,
                         double *rhs_square_length, const char **what) try {
  const double result = lhs->impl.inner_product(rhs->impl, lhs_square_length,
                                                rhs_square_length);
  if (inner_product != NULL) {
    *inner_product = result;
  }
  return 0;
} catch (const madoka::Exception &ex) {
  if (what != NULL) {
    *what = ex.what();
  }
  return -1;
}

}  // extern "C"

namespace madoka {

Sketch::Sketch() noexcept
  : file_(), header_(NULL), random_(NULL), table_(NULL) {}

Sketch::~Sketch() noexcept {}

void Sketch::create(UInt64 width, UInt64 max_value, const char *path,
                    int flags, UInt64 seed) {
  Sketch new_sketch;
  new_sketch.create_(width, max_value, path, flags, seed);
  new_sketch.clear();
  new_sketch.swap(this);
}

void Sketch::open(const char *path, int flags) {
  Sketch new_sketch;
  new_sketch.open_(path, flags);
  new_sketch.swap(this);
}

void Sketch::close() noexcept {
  Sketch().swap(this);
}

void Sketch::load(const char *path, int flags) {
  Sketch new_sketch;
  new_sketch.load_(path, flags);
  new_sketch.swap(this);
}

void Sketch::save(const char *path, int flags) const {
  file_.save(path, flags);
}

UInt64 Sketch::get(const void *key_addr, std::size_t key_size) const noexcept {
  UInt64 cell_ids[3];
  hash(key_addr, key_size, cell_ids);
  if (mode() == SKETCH_EXACT_MODE) {
    cell_ids[1] += width();
    cell_ids[2] += width() * 2;
    return exact_get(cell_ids);
  } else {
    return approx_get(cell_ids);
  }
}

void Sketch::set(const void *key_addr, std::size_t key_size,
                 UInt64 value) noexcept {
  UInt64 cell_ids[3];
  hash(key_addr, key_size, cell_ids);
  if (mode() == SKETCH_EXACT_MODE) {
    cell_ids[1] += width();
    cell_ids[2] += width() * 2;
    exact_set(cell_ids, value);
  } else {
    approx_set(cell_ids, value);
  }
}

UInt64 Sketch::inc(const void *key_addr, std::size_t key_size) noexcept {
  UInt64 cell_ids[3];
  hash(key_addr, key_size, cell_ids);
  if (mode() == SKETCH_EXACT_MODE) {
    cell_ids[1] += width();
    cell_ids[2] += width() * 2;
    return exact_inc(cell_ids);
  } else {
    return approx_inc(cell_ids);
  }
}

UInt64 Sketch::add(const void *key_addr, std::size_t key_size,
                   UInt64 value) noexcept {
  UInt64 cell_ids[3];
  hash(key_addr, key_size, cell_ids);
  if (mode() == SKETCH_EXACT_MODE) {
    cell_ids[1] += width();
    cell_ids[2] += width() * 2;
    return exact_add(cell_ids, value);
  } else {
    return approx_add(cell_ids, value);
  }
}

void Sketch::clear() noexcept {
  std::memset(table_, 0, static_cast<std::size_t>(table_size()));
}

void Sketch::copy(const Sketch &src, const char *path, int flags) {
  Sketch new_sketch;
  new_sketch.copy_(src, path, flags);
  new_sketch.swap(this);
}

void Sketch::filter(Filter filter) noexcept {
  if (filter != NULL) {
    for (UInt64 table_id = 0; table_id < SKETCH_DEPTH; ++table_id) {
      for (UInt64 cell_id = 0; cell_id < width(); ++cell_id) {
        const UInt64 value = filter(get_(table_id, cell_id));
        set_(table_id, cell_id, (value <= max_value()) ? value : max_value());
      }
    }
  }
}

void Sketch::shrink(const Sketch &src, UInt64 width,
                    UInt64 max_value, Filter filter,
                    const char *path, int flags) {
  Sketch new_sketch;
  new_sketch.shrink_(src, width, max_value, filter, path, flags);
  new_sketch.swap(this);
}

void Sketch::merge(const Sketch &rhs, Filter lhs_filter, Filter rhs_filter) {
  MADOKA_THROW_IF(width() != rhs.width());
  MADOKA_THROW_IF(seed() != rhs.seed());

  if ((lhs_filter != NULL) || (rhs_filter != NULL) ||
      (mode() == SKETCH_EXACT_MODE) || (rhs.mode() == SKETCH_EXACT_MODE)) {
    if (mode() == SKETCH_EXACT_MODE) {
      exact_merge_(rhs, lhs_filter, rhs_filter);
    } else {
      approx_merge_(rhs, lhs_filter, rhs_filter);
    }
  } else {
    approx_merge_(rhs);
  }
}

double Sketch::inner_product(const Sketch &rhs, double *lhs_square_length,
                             double *rhs_square_length) const {
  MADOKA_THROW_IF(width() != rhs.width());
  MADOKA_THROW_IF(seed() != rhs.seed());

  double inner_product = std::numeric_limits<double>::max();
  for (UInt64 table_id = 0; table_id < SKETCH_DEPTH; ++table_id) {
    double current_inner_product = 0.0;
    double current_lhs_square_length = 0.0;
    double current_rhs_square_length = 0.0;
    for (UInt64 cell_id = 0; cell_id < width(); ++cell_id) {
      const double lhs_value =
          static_cast<double>(get_(table_id, cell_id));
      const double rhs_value =
          static_cast<double>(rhs.get_(table_id, cell_id));
      current_inner_product += lhs_value * rhs_value;
      if (lhs_square_length != NULL) {
        current_lhs_square_length += lhs_value * lhs_value;
      }
      if (rhs_square_length != NULL) {
        current_rhs_square_length += rhs_value * rhs_value;
      }
    }
    if (current_inner_product < inner_product) {
      inner_product = current_inner_product;
      if (lhs_square_length != NULL) {
        *lhs_square_length = current_lhs_square_length;
      }
      if (rhs_square_length != NULL) {
        *rhs_square_length = current_rhs_square_length;
      }
    }
  }
  return inner_product;
}

void Sketch::swap(Sketch *sketch) noexcept {
  file_.swap(&sketch->file_);
  util::swap(header_, sketch->header_);
  util::swap(random_, sketch->random_);
  util::swap(table_, sketch->table_);
}

void Sketch::create_(UInt64 width, UInt64 max_value, const char *path,
                     int flags, UInt64 seed) {
  if (width == 0) {
    width = SKETCH_DEFAULT_WIDTH;
  }

  if (max_value == 0) {
    max_value = SKETCH_DEFAULT_MAX_VALUE;
  } else if (max_value < (1ULL << 1)) {
    max_value = (1ULL << 1) - 1;
  } else if (max_value < (1ULL << 2)) {
    max_value = (1ULL << 2) - 1;
  } else if (max_value < (1ULL << 4)) {
    max_value = (1ULL << 4) - 1;
  } else if (max_value < (1ULL << 8)) {
    max_value = (1ULL << 8) - 1;
  } else if (max_value < (1ULL << 16)) {
    max_value = (1ULL << 16) - 1;
  } else {
    max_value = SKETCH_MAX_MAX_VALUE;
  }

  MADOKA_THROW_IF(width < SKETCH_MIN_WIDTH);
  MADOKA_THROW_IF(width > SKETCH_MAX_WIDTH);
  MADOKA_THROW_IF(max_value > SKETCH_MAX_MAX_VALUE);

  const UInt64 value_size = util::bit_scan_reverse(max_value) + 1;
  UInt64 table_size = sizeof(UInt64) * width;
  if (value_size != SKETCH_APPROX_VALUE_SIZE) {
    table_size = (((value_size * width * SKETCH_DEPTH) + 63) / 64) * 8;
  }

  const UInt64 file_size = sizeof(Header) + sizeof(Random) + table_size;
  MADOKA_THROW_IF(file_size > std::numeric_limits<std::size_t>::max());

  file_.create(path, static_cast<std::size_t>(file_size), flags);
  header_ = static_cast<Header *>(file_.addr());
  random_ = reinterpret_cast<Random *>(header_ + 1);
  table_ = reinterpret_cast<UInt64 *>(random_ + 1);

  header().set_width(width);
  header().set_depth(SKETCH_DEPTH);
  header().set_max_value(max_value);
  header().set_value_size(value_size);
  header().set_seed(seed);
  header().set_table_size(table_size);
  header().set_file_size(file_size);
  check_header();

  random_->reset(seed);
}

void Sketch::open_(const char *path, int flags) {
  file_.open(path, flags);
  header_ = static_cast<Header *>(file_.addr());
  random_ = reinterpret_cast<Random *>(header_ + 1);
  table_ = reinterpret_cast<UInt64 *>(random_ + 1);
  check_header();
}

void Sketch::load_(const char *path, int flags) {
  file_.load(path, flags);
  header_ = static_cast<Header *>(file_.addr());
  random_ = reinterpret_cast<Random *>(header_ + 1);
  table_ = reinterpret_cast<UInt64 *>(random_ + 1);
  check_header();
}

void Sketch::check_header() const {
  MADOKA_THROW_IF(width() < SKETCH_MIN_WIDTH);
  MADOKA_THROW_IF(width() > SKETCH_MAX_WIDTH);
  MADOKA_THROW_IF((width_mask() != 0) && (width_mask() != (width() - 1)));
  MADOKA_THROW_IF(depth() != SKETCH_DEPTH);
  MADOKA_THROW_IF(max_value() == 0);
  MADOKA_THROW_IF(value_size() != (util::bit_scan_reverse(max_value()) + 1));
  if (mode() == SKETCH_APPROX_MODE) {
    MADOKA_THROW_IF(table_size() != (sizeof(UInt64)) * width());
  } else {
    const UInt64 expected_table_size =
        (((value_size() * width() * SKETCH_DEPTH) + 63) / 64) * 8;
    MADOKA_THROW_IF(table_size() != expected_table_size);
  }
  MADOKA_THROW_IF(file_size() != file_.size());
}

UInt64 Sketch::get_(UInt64 table_id, UInt64 cell_id) const noexcept {
  if (mode() == SKETCH_EXACT_MODE) {
    return exact_get_((width() * table_id) + cell_id);
  } else {
    return Approx::decode(approx_get_(table_id, cell_id), random_);
  }
}

void Sketch::set_(UInt64 table_id, UInt64 cell_id, UInt64 value) noexcept {
  if (mode() == SKETCH_EXACT_MODE) {
    exact_set_((width() * table_id) + cell_id, value);
  } else {
    approx_set_(table_id, cell_id, Approx::encode(value));
  }
}

UInt64 Sketch::exact_get(const UInt64 cell_ids[3]) const noexcept {
  UInt64 min_value = exact_get_(cell_ids[0]);
  if (min_value == 0) {
    return 0;
  }

  UInt64 value = exact_get_(cell_ids[1]);
  if (value == 0) {
    return 0;
  } else if (value < min_value) {
    min_value = value;
  }

  value = exact_get_(cell_ids[2]);
  return (value < min_value) ? value : min_value;
}

void Sketch::exact_set(const UInt64 cell_ids[3], UInt64 value) noexcept {
  if (value > max_value()) {
    value = max_value();
  }

  exact_set_floor_(cell_ids[0], value);
  exact_set_floor_(cell_ids[1], value);
  exact_set_floor_(cell_ids[2], value);
}

UInt64 Sketch::exact_inc(const UInt64 cell_ids[3]) noexcept {
  UInt64 values[3];
  values[0] = exact_get_(cell_ids[0]);
  values[1] = exact_get_(cell_ids[1]);
  values[2] = exact_get_(cell_ids[2]);
  if (values[0] < values[1]) {
    if (values[0] > values[2]) {
      exact_set_(cell_ids[2], ++values[2]);
      return values[2];
    } else {
      if (values[0]++ == values[2]) {
        exact_set_(cell_ids[2], values[0]);
      }
      exact_set_(cell_ids[0], values[0]);
      return values[0];
    }
  } else if (values[0] > values[1]) {
    if (values[1] > values[2]) {
      exact_set_(cell_ids[2], ++values[2]);
      return values[2];
    } else {
      if (values[1]++ == values[2]) {
        exact_set_(cell_ids[2], values[1]);
      }
      exact_set_(cell_ids[1], values[1]);
      return values[1];
    }
  } else if (values[0] > values[2]) {
    exact_set_(cell_ids[2], ++values[2]);
    return values[2];
  } else if (values[0] < max_value()) {
    if (values[0]++ == values[2]) {
      exact_set_(cell_ids[2], values[0]);
    }
    exact_set_(cell_ids[0], values[0]);
    exact_set_(cell_ids[1], values[0]);
  }
  return values[0];
}

UInt64 Sketch::exact_add(const UInt64 cell_ids[3], UInt64 value) noexcept {
  UInt64 new_value = max_value();
  UInt64 values[3];

  values[0] = exact_get_(cell_ids[0]);
  if ((new_value - values[0]) > value) {
    new_value = values[0] + value;
  }

  values[1] = exact_get_(cell_ids[1]);
  if (values[1] < new_value) {
    if ((new_value - values[1]) > value) {
      new_value = values[1] + value;
    }
  }

  values[2] = exact_get_(cell_ids[2]);
  if (values[2] < new_value) {
    if ((new_value - values[2]) > value) {
      new_value = values[2] + value;
    }
    exact_set_(cell_ids[2], new_value);
  }

  if (values[1] < new_value) {
    exact_set_(cell_ids[1], new_value);
  }

  if (values[0] < new_value) {
    exact_set_(cell_ids[0], new_value);
  }

  return new_value;
}

UInt64 Sketch::exact_get_(UInt64 cell_id) const noexcept {
  switch (value_size()) {
    case 1: {
      return (table_[cell_id / 64] >> (cell_id % 64)) & value_mask();
    }
    case 2: {
      return (table_[cell_id / 32] >> ((cell_id % 32) * 2)) & value_mask();
    }
    case 4: {
      return (table_[cell_id / 16] >> ((cell_id % 16) * 4)) & value_mask();
    }
    case 8: {
      return reinterpret_cast<const UInt8 *>(table_)[cell_id];
    }
    case 16: {
      return reinterpret_cast<const UInt16 *>(table_)[cell_id];
    }
    default: {
      return 0;
    }
  }
}

void Sketch::exact_set_(UInt64 cell_id, UInt64 value) noexcept {
  switch (value_size()) {
    case 1: {
      table_[cell_id / 64] &= ~(value_mask() << (cell_id % 64));
      table_[cell_id / 64] |= value << (cell_id % 64);
      break;
    }
    case 2: {
      table_[cell_id / 32] &= ~(value_mask() << ((cell_id % 32) * 2));
      table_[cell_id / 32] |= value << ((cell_id % 32) * 2);
      break;
    }
    case 4: {
      table_[cell_id / 16] &= ~(value_mask() << ((cell_id % 16) * 4));
      table_[cell_id / 16] |= value << ((cell_id % 16) * 4);
      break;
    }
    case 8: {
      reinterpret_cast<UInt8 *>(table_)[cell_id] =
          static_cast<UInt8>(value);
      break;
    }
    case 16: {
      reinterpret_cast<UInt16 *>(table_)[cell_id] =
          static_cast<UInt16>(value);
      break;
    }
  }
}

void Sketch::exact_set_floor_(UInt64 cell_id, UInt64 value) noexcept {
  switch (value_size()) {
    case 1: {
      table_[cell_id / 64] |= value << (cell_id % 64);
      break;
    }
    case 2: {
      const std::size_t unit_id = static_cast<std::size_t>(cell_id / 32);
      const std::size_t unit_offset = static_cast<std::size_t>((cell_id % 32) * 2);
      if (((table_[unit_id] >> unit_offset) & value_mask()) < value) {
        table_[unit_id] &= ~(value_mask() << unit_offset);
        table_[unit_id] |= value << unit_offset;
      }
      break;
    }
    case 4: {
      const std::size_t unit_id = static_cast<std::size_t>(cell_id / 16);
      const std::size_t unit_offset = static_cast<std::size_t>((cell_id % 16) * 4);
      if (((table_[unit_id] >> unit_offset) & value_mask()) < value) {
        table_[unit_id] &= ~(value_mask() << unit_offset);
        table_[unit_id] |= value << unit_offset;
      }
      break;
    }
    case 8: {
      UInt8 &cell = reinterpret_cast<UInt8 *>(table_)[cell_id];
      if (cell < value) {
        cell = static_cast<UInt8>(value);
      }
      break;
    }
    case 16: {
      UInt16 &cell = reinterpret_cast<UInt16 *>(table_)[cell_id];
      if (cell < value) {
        cell = static_cast<UInt16>(value);
      }
      break;
    }
  }
}

UInt64 Sketch::approx_get(const UInt64 cell_ids[3]) const noexcept {
  UInt64 min_approx = approx_get_(0, cell_ids[0]);
  if (min_approx == 0) {
    return 0;
  }

  UInt64 approx = approx_get_(1, cell_ids[1]);
  if (approx == 0) {
    return 0;
  } else if (approx < min_approx) {
    min_approx = approx;
  }
  
  approx = approx_get_(2, cell_ids[2]);
  if (approx < min_approx) {
    min_approx = approx;
  }
  return Approx::decode(min_approx, random_);
}

void Sketch::approx_set(const UInt64 cell_ids[3], UInt64 value) noexcept {
  const UInt64 new_approx = (value < APPROX_MAX_VALUE) ?
      Approx::encode(value) : APPROX_MASK;

  UInt64 approx = approx_get_(0, cell_ids[0]);
  if (approx < new_approx) {
    approx_set_(0, cell_ids[0], new_approx);
  }

  approx = approx_get_(1, cell_ids[1]);
  if (approx < new_approx) {
    approx_set_(1, cell_ids[1], new_approx);
  }

  approx = approx_get_(2, cell_ids[2]);
  if (approx < new_approx) {
    approx_set_(2, cell_ids[2], new_approx);
  }
}

UInt64 Sketch::approx_inc(const UInt64 cell_ids[3]) noexcept {
  const UInt64 flag = 1ULL << ((cell_ids[0] ^ cell_ids[1] ^ cell_ids[2]) & 1);

  UInt64 approxes[3];
  approxes[0] = approx_get_(0, cell_ids[0]);
  approxes[1] = approx_get_(1, cell_ids[1]);
  approxes[2] = approx_get_(2, cell_ids[2]);

  const UInt64 min_approx = (approxes[0] < approxes[1]) ?
      ((approxes[0] < approxes[2]) ? approxes[0] : approxes[2]) :
      ((approxes[1] < approxes[2]) ? approxes[1] : approxes[2]);

  UInt64 new_approx = min_approx;
  if ((new_approx != APPROX_MASK) &&
      ((approxes[0] != min_approx) ||
       !((table_[cell_ids[0]] >> SKETCH_OWNER_OFFSET) & flag)) &&
      ((approxes[1] != min_approx) ||
       !((table_[cell_ids[1]] >> (SKETCH_OWNER_OFFSET + 2)) & flag)) &&
      ((approxes[2] != min_approx) ||
       !((table_[cell_ids[2]] >> (SKETCH_OWNER_OFFSET + 4)) & flag))) {
    new_approx = Approx::inc(new_approx, random_);
  }

  if (approxes[0] < new_approx) {
    approx_set_(0, cell_ids[0], new_approx, 3 ^ flag);
  } else if (approxes[0] == new_approx) {
    table_[cell_ids[0]] &= ~(flag << SKETCH_OWNER_OFFSET);
  }

  if (approxes[1] < new_approx) {
    approx_set_(1, cell_ids[1], new_approx, 3 ^ flag);
  } else if (approxes[1] == new_approx) {
    table_[cell_ids[1]] &= ~(flag << (SKETCH_OWNER_OFFSET + 2));
  }

  if (approxes[2] < new_approx) {
    approx_set_(2, cell_ids[2], new_approx, 3 ^ flag);
  } else if (approxes[2] == new_approx) {
    table_[cell_ids[2]] &= ~(flag << (SKETCH_OWNER_OFFSET + 4));
  }
  return Approx::decode(new_approx, random_);
}

UInt64 Sketch::approx_add(const UInt64 cell_ids[3], UInt64 value) noexcept {
  if (value >= APPROX_MAX_VALUE) {
    table_[cell_ids[0]] |= APPROX_MASK;
    table_[cell_ids[1]] |= APPROX_MASK << APPROX_SIZE;
    table_[cell_ids[2]] |= APPROX_MASK << (APPROX_SIZE * 2);
    return APPROX_MAX_VALUE;
  }

  UInt64 approxes[3];
  approxes[0] = approx_get_(0, cell_ids[0]);
  approxes[1] = approx_get_(1, cell_ids[1]);
  approxes[2] = approx_get_(2, cell_ids[2]);

  const UInt64 min_approx = (approxes[0] < approxes[1]) ?
      ((approxes[0] < approxes[2]) ? approxes[0] : approxes[2]) :
      ((approxes[1] < approxes[2]) ? approxes[1] : approxes[2]);
  const UInt64 min_value = Approx::decode(min_approx, random_);
  if (min_value >= (APPROX_MAX_VALUE - value)) {
    table_[cell_ids[0]] |= APPROX_MASK;
    table_[cell_ids[1]] |= APPROX_MASK << APPROX_SIZE;
    table_[cell_ids[2]] |= APPROX_MASK << (APPROX_SIZE * 2);
    return APPROX_MAX_VALUE;
  }

  const UInt64 new_value = min_value + value;
  const UInt64 new_approx = Approx::encode(new_value);

  if (approxes[0] < new_approx) {
    approx_set_(0, cell_ids[0], new_approx);
  }
  if (approxes[1] < new_approx) {
    approx_set_(1, cell_ids[1], new_approx);
  }
  if (approxes[2] < new_approx) {
    approx_set_(2, cell_ids[2], new_approx);
  }
  return new_value;
}

UInt64 Sketch::approx_get_(UInt64 table_id, UInt64 cell_id) const noexcept {
  return (table_[cell_id] >> (APPROX_SIZE * table_id)) & APPROX_MASK;
}

void Sketch::approx_set_(UInt64 table_id, UInt64 cell_id,
                         UInt64 approx) noexcept {
  table_[cell_id] &= ~(APPROX_MASK << (APPROX_SIZE * table_id));
  table_[cell_id] |= approx << (APPROX_SIZE * table_id);
}

void Sketch::approx_set_(UInt64 table_id, UInt64 cell_id,
                         UInt64 approx, UInt64 mask) noexcept {
  table_[cell_id] &= ~((APPROX_MASK << (APPROX_SIZE * table_id)) |
      (3ULL << (SKETCH_OWNER_OFFSET + (2 * table_id))));
  table_[cell_id] |= (approx << (APPROX_SIZE * table_id)) |
      (mask << (SKETCH_OWNER_OFFSET + (2 * table_id)));
}

void Sketch::hash(const void *key_addr, std::size_t key_size,
                  UInt64 cell_ids[3]) const noexcept {
  UInt64 hash_values[2];
  Hash()(key_addr, key_size, seed(), hash_values);

  cell_ids[0] = hash_values[0] & SKETCH_ID_MASK;
  cell_ids[1] = ((hash_values[0] >> SKETCH_ID_SIZE) |
      (hash_values[1] << (64 - SKETCH_ID_SIZE))) & SKETCH_ID_MASK;
  cell_ids[2] = hash_values[1] >> (64 - SKETCH_ID_SIZE);

  if (width_mask() != 0) {
    cell_ids[0] &= width_mask();
    cell_ids[1] &= width_mask();
    cell_ids[2] &= width_mask();
  } else {
    cell_ids[0] %= width();
    cell_ids[1] %= width();
    cell_ids[2] %= width();
  }
}

void Sketch::copy_(const Sketch &src, const char *path, int flags) {
  create_(src.width(), src.max_value(), path, flags, src.seed());
  std::memcpy(random_, src.random_, sizeof(Random));
  std::memcpy(table_, src.table_, static_cast<std::size_t>(table_size()));
}

void Sketch::exact_merge_(const Sketch &rhs, Filter lhs_filter,
                          Filter rhs_filter) noexcept {
  for (UInt64 table_id = 0; table_id < SKETCH_DEPTH; ++table_id) {
    for (UInt64 cell_id = 0; cell_id < width(); ++cell_id) {
      UInt64 lhs_value = get_(table_id, cell_id);
      UInt64 rhs_value = rhs.get_(table_id, cell_id);
      if (lhs_filter != NULL) {
        lhs_value = lhs_filter(lhs_value);
      }
      if (rhs_filter != NULL) {
        rhs_value = rhs_filter(rhs_value);
      }

      if ((lhs_value >= max_value()) ||
          (rhs_value >= (max_value() - lhs_value))) {
        lhs_value = max_value();
      } else {
        lhs_value += rhs_value;
      }
      set_(table_id, cell_id, lhs_value);
    }
  }
}

void Sketch::approx_merge_(const Sketch &rhs, Filter lhs_filter,
                           Filter rhs_filter) noexcept {
  for (UInt64 cell_id = 0; cell_id < width(); ++cell_id) {
    for (UInt64 table_id = 0; table_id < SKETCH_DEPTH; ++table_id) {
      UInt64 lhs_value = get_(table_id, cell_id);
      UInt64 rhs_value = rhs.get_(table_id, cell_id);
      if (lhs_filter != NULL) {
        lhs_value = lhs_filter(lhs_value);
      }
      if (rhs_filter != NULL) {
        rhs_value = rhs_filter(rhs_value);
      }

      if ((lhs_value >= APPROX_MAX_VALUE) ||
          (rhs_value >= (APPROX_MAX_VALUE - lhs_value))) {
        lhs_value = max_value();
      } else {
        lhs_value += rhs_value;
      }
      approx_set_(table_id, cell_id, Approx::encode(lhs_value), 0);
    }
  }
}

void Sketch::approx_merge_(const Sketch &rhs) noexcept {
  static const UInt64 MASK_TABLE[4] = { 0, 1, 2, 0 };

  for (UInt64 cell_id = 0; cell_id < width(); ++cell_id) {
    table_[cell_id] |= rhs.table_[cell_id] & SKETCH_OWNER_MASK;
    for (UInt64 table_id = 0; table_id < SKETCH_DEPTH; ++table_id) {
      const UInt64 mask = ((table_[cell_id] | rhs.table_[cell_id]) >>
          (SKETCH_OWNER_OFFSET + (2 * table_id))) & 3;

      UInt64 lhs_value = get_(table_id, cell_id);
      const UInt64 rhs_value = rhs.get_(table_id, cell_id);
      if ((rhs_value > (APPROX_MAX_VALUE - lhs_value))) {
        lhs_value = APPROX_MAX_VALUE;
      } else {
        lhs_value += rhs_value;
        if ((mask == 3) && (lhs_value != 0)) {
          --lhs_value;
        }
      }
      approx_set_(table_id, cell_id, Approx::encode(lhs_value),
                  MASK_TABLE[mask]);
    }
  }
}

void Sketch::shrink_(const Sketch &src, UInt64 width,
                     UInt64 max_value, Filter filter,
                     const char *path, int flags) {
  if (width == 0) {
    width = src.width();
  }

  if (max_value == 0) {
    max_value = src.max_value();
  }

  MADOKA_THROW_IF(src.width() == 0);
  MADOKA_THROW_IF(width > src.width());
  MADOKA_THROW_IF((src.width() % width) != 0);

  create_(width, max_value, path, flags, src.seed());
  std::memcpy(random_, src.random_, sizeof(Random));

  width = this->width();
  max_value = this->max_value();

  if (mode() == SKETCH_APPROX_MODE) {
    clear();
  }

  for (UInt64 table_id = 0; table_id < SKETCH_DEPTH; ++table_id) {
    for (UInt64 cell_id = 0; cell_id < width; ++cell_id) {
      UInt64 value = src.get_(table_id, cell_id);
      if (filter != NULL) {
        value = filter(value);
      }
      if (value > max_value) {
        value = max_value;
      }
      set_(table_id, cell_id, value);
    }

    for (UInt64 offset = width; offset < src.width(); offset += width) {
      for (UInt64 cell_id = 0; cell_id < width; ++cell_id) {
        UInt64 value = src.get_(table_id, offset + cell_id);
        if (filter != NULL) {
          value = filter(value);
        }
        if (value > max_value) {
          value = max_value;
        }
        if (value > get_(table_id, cell_id)) {
          set_(table_id, cell_id, value);
        }
      }
    }
  }
}

}  // namespace madoka
