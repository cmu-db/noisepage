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

#ifndef MADOKA_SKETCH_H
#define MADOKA_SKETCH_H

#include "approx.h"
#include "file.h"
#include "hash.h"
#include "header.h"
#include "random.h"

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

typedef madoka_uint64 (*madoka_sketch_filter)(madoka_uint64);

typedef enum {
  MADOKA_SKETCH_EXACT_MODE,
  MADOKA_SKETCH_APPROX_MODE
} madoka_sketch_mode;

typedef struct madoka_sketch_ madoka_sketch;

madoka_sketch *madoka_create(madoka_uint64 width, madoka_uint64 max_value,
                             const char *path, int flags, madoka_uint64 seed,
                             const char **what);

madoka_sketch *madoka_open(const char *path, int flags, const char **what);

void madoka_close(madoka_sketch *sketch);

madoka_sketch *madoka_load(const char *path, int flags, const char **what);

int madoka_save(const madoka_sketch *sketch, const char *path, int flags,
                const char **what);

madoka_uint64 madoka_get_width(const madoka_sketch *sketch);
madoka_uint64 madoka_get_width_mask(const madoka_sketch *sketch);
madoka_uint64 madoka_get_depth(const madoka_sketch *sketch);
madoka_uint64 madoka_get_max_value(const madoka_sketch *sketch);
madoka_uint64 madoka_get_value_mask(const madoka_sketch *sketch);
madoka_uint64 madoka_get_value_size(const madoka_sketch *sketch);
madoka_uint64 madoka_get_seed(const madoka_sketch *sketch);
madoka_uint64 madoka_get_table_size(const madoka_sketch *sketch);
madoka_uint64 madoka_get_file_size(const madoka_sketch *sketch);
int madoka_get_flags(const madoka_sketch *sketch);
madoka_sketch_mode madoka_get_mode(const madoka_sketch *sketch);

madoka_uint64 madoka_get(const madoka_sketch *sketch, const void *key_addr,
                         size_t key_size);
void madoka_set(madoka_sketch *sketch, const void *key_addr,
                size_t key_size, madoka_uint64 value);
madoka_uint64 madoka_inc(madoka_sketch *sketch, const void *key_addr,
                         size_t key_size);
madoka_uint64 madoka_add(madoka_sketch *sketch, const void *key_addr,
                         size_t key_size, madoka_uint64 value);

void madoka_clear(madoka_sketch *sketch);

madoka_sketch *madoka_copy(const madoka_sketch *src, const char *path,
                           int flags, const char **what);

void madoka_filter(madoka_sketch *sketch, madoka_sketch_filter filter);

madoka_sketch *madoka_shrink(const madoka_sketch *src,
                             madoka_uint64 width, madoka_uint64 max_value,
                             madoka_sketch_filter filter, const char *path,
                             int flags, const char **what);

int madoka_merge(madoka_sketch *lhs, const madoka_sketch *rhs,
                 madoka_sketch_filter lhs_filter,
                 madoka_sketch_filter rhs_filter, const char **what);

void madoka_swap(madoka_sketch *lhs, madoka_sketch *rhs);

int madoka_inner_product(const madoka_sketch *lhs, const madoka_sketch *rhs,
                         double *inner_product, double *lhs_square_length,
                         double *rhs_square_length, const char **what);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#ifdef __cplusplus
namespace madoka {

typedef madoka_sketch_filter SketchFilter;

enum SketchMode {
  SKETCH_EXACT_MODE  = MADOKA_SKETCH_EXACT_MODE,
  SKETCH_APPROX_MODE = MADOKA_SKETCH_APPROX_MODE
};

const UInt64 SKETCH_ID_SIZE           = 128 / 3;
const UInt64 SKETCH_MAX_ID            = (1ULL << SKETCH_ID_SIZE) - 1;
const UInt64 SKETCH_ID_MASK           = SKETCH_MAX_ID;

const UInt64 SKETCH_MIN_WIDTH         = 1;
const UInt64 SKETCH_MAX_WIDTH         = SKETCH_MAX_ID + 1;
const UInt64 SKETCH_DEFAULT_WIDTH     = 1ULL << 20;

const UInt64 SKETCH_MAX_MAX_VALUE     = APPROX_MAX_VALUE;
const UInt64 SKETCH_DEFAULT_MAX_VALUE = SKETCH_MAX_MAX_VALUE;

const UInt64 SKETCH_DEPTH             = 3;

const UInt64 SKETCH_APPROX_VALUE_SIZE = APPROX_VALUE_SIZE;

const UInt64 SKETCH_OWNER_OFFSET      = APPROX_SIZE * 3;
const UInt64 SKETCH_OWNER_MASK        = 0x3FULL << SKETCH_OWNER_OFFSET;

class Sketch {
 public:
  typedef SketchFilter Filter;
  typedef SketchMode Mode;

  Sketch() noexcept;
  ~Sketch() noexcept;

  void create(UInt64 width = 0, UInt64 max_value = 0,
              const char *path = NULL, int flags = 0, UInt64 seed = 0);
  void open(const char *path, int flags = 0);
  void close() noexcept;

  void load(const char *path, int flags = 0);
  void save(const char *path, int flags = 0) const;

  UInt64 width() const noexcept {
    return header().width();
  }
  UInt64 width_mask() const noexcept {
    return header().width_mask();
  }
  UInt64 depth() const noexcept {
    return SKETCH_DEPTH;
  }
  UInt64 max_value() const noexcept {
    return header().max_value();
  }
  UInt64 value_mask() const noexcept {
    return header().max_value();
  }
  UInt64 value_size() const noexcept {
    return header().value_size();
  }
  UInt64 seed() const noexcept {
    return header().seed();
  }
  UInt64 table_size() const noexcept {
    return header().table_size();
  }
  UInt64 file_size() const noexcept {
    return header().file_size();
  }
  int flags() const noexcept {
    return file_.flags();
  }
  Mode mode() const noexcept {
    return (value_size() == SKETCH_APPROX_VALUE_SIZE) ?
        SKETCH_APPROX_MODE : SKETCH_EXACT_MODE;
  }

  UInt64 get(const void *key_addr, std::size_t key_size) const noexcept;
  void set(const void *key_addr, std::size_t key_size, UInt64 value) noexcept;
  UInt64 inc(const void *key_addr, std::size_t key_size) noexcept;
  UInt64 add(const void *key_addr, std::size_t key_size, UInt64 value) noexcept;

  void clear() noexcept;

  void copy(const Sketch &src, const char *path = NULL, int flags = 0);

  void filter(Filter filter) noexcept;

  void shrink(const Sketch &src, UInt64 width = 0,
              UInt64 max_value = 0, Filter filter = NULL,
              const char *path = NULL, int flags = 0);

  void merge(const Sketch &rhs, Filter lhs_filter = NULL,
             Filter rhs_filter = NULL);

  void swap(Sketch *sketch) noexcept;

  double inner_product(const Sketch &rhs, double *lhs_square_length = NULL,
                       double *rhs_square_length = NULL) const;

 private:
  File file_;
  Header *header_;
  Random *random_;
  UInt64 *table_;

  const Header &header() const noexcept {
    return *header_;
  }
  Header &header() noexcept {
    return *header_;
  }

  void create_(UInt64 width, UInt64 max_value, const char *path,
               int flags, UInt64 seed);
  void open_(const char *path, int flags);

  void load_(const char *path, int flags);

  void check_header() const;

  inline UInt64 get_(UInt64 table_id, UInt64 cell_id) const noexcept;
  inline void set_(UInt64 table_id, UInt64 cell_id, UInt64 value) noexcept;

  UInt64 exact_get(const UInt64 cell_ids[3]) const noexcept;
  void exact_set(const UInt64 cell_ids[3], UInt64 value) noexcept;
  UInt64 exact_inc(const UInt64 cell_ids[3]) noexcept;
  UInt64 exact_add(const UInt64 cell_ids[3], UInt64 value) noexcept;

  inline UInt64 exact_get_(UInt64 cell_id) const noexcept;
  inline void exact_set_(UInt64 cell_id, UInt64 value) noexcept;
  inline void exact_set_floor_(UInt64 cell_id, UInt64 value) noexcept;

  UInt64 approx_get(const UInt64 cell_ids[3]) const noexcept;
  void approx_set(const UInt64 cell_ids[3], UInt64 value) noexcept;
  UInt64 approx_inc(const UInt64 cell_ids[3]) noexcept;
  UInt64 approx_add(const UInt64 cell_ids[3], UInt64 value) noexcept;

  inline UInt64 approx_get_(UInt64 table_id, UInt64 cell_id) const noexcept;
  inline void approx_set_(UInt64 table_id, UInt64 cell_id,
                          UInt64 approx) noexcept;
  inline void approx_set_(UInt64 table_id, UInt64 cell_id,
                          UInt64 approx, UInt64 mask) noexcept;

  inline void hash(const void *key_addr, std::size_t key_size,
                   UInt64 cell_ids[3]) const noexcept;

  void copy_(const Sketch &src, const char *path, int flags);

  void exact_merge_(const Sketch &rhs, Filter lhs_filter,
                    Filter rhs_filter) noexcept;
  void approx_merge_(const Sketch &rhs, Filter lhs_filter,
                     Filter rhs_filter) noexcept;
  void approx_merge_(const Sketch &rhs) noexcept;

  void shrink_(const Sketch &src, UInt64 width, UInt64 max_value,
               Filter filter, const char *path, int flags);

  // Disallows copy and assignment.
  Sketch(const Sketch &);
  Sketch &operator=(const Sketch &);
};

}  // namespace madoka
#endif  // __cplusplus

#endif  // MADOKA_SKETCH_H
