#pragma once
#include <random>

#include "gtest/gtest.h"
#include "storage/block_store.h"
#include "storage/tuple_access_strategy.h"
#include "common/common_defs.h"
#include "common/test_util.h"

namespace terrier {
namespace testutil {
// Returns a random layout that is guaranteed to be valid.
template<typename Random>
storage::BlockLayout RandomLayout(Random &generator) {
  // We probably won't allow tables with 0 columns
  uint16_t num_attrs =
      std::uniform_int_distribution<uint16_t>(1, UINT16_MAX)(generator);
  std::vector<uint8_t> possible_attr_sizes{1, 2, 4, 8}, attr_sizes(num_attrs);
  for (uint16_t i = 0; i < num_attrs; i++)
    attr_sizes[i] =
        *testutil::UniformRandomElement(possible_attr_sizes, generator);
  return {num_attrs, attr_sizes};
}
// Read specified number of bytes from position and interpret the bytes as
// an integer of given size. (Thus only 1, 2, 4, 8 are allowed)
uint64_t ReadByteValue(uint8_t attr_size, byte *pos) {
  switch (attr_size) {
    case 1: return *reinterpret_cast<uint8_t *>(pos);
    case 2: return *reinterpret_cast<uint16_t *>(pos);
    case 4: return *reinterpret_cast<uint32_t *>(pos);
    case 8: return *reinterpret_cast<uint64_t *>( pos);
    default:
      // Invalid attr size
      PELOTON_ASSERT(false);
      return 0;
  }
}

// Write specified number of bytes to position and interpret the bytes as
// an integer of given size. (Thus only 1, 2, 4, 8 are allowed)
// Truncated if neccessary
void WriteByteValue(uint8_t attr_size, uint64_t val, byte *pos) {
  switch (attr_size) {
    case 1:
      *reinterpret_cast<uint8_t *>(pos) = static_cast<uint8_t>(val);
      return;
    case 2:
      *reinterpret_cast<uint16_t *>(pos) = static_cast<uint16_t>(val);
      return;
    case 4:
      *reinterpret_cast<uint32_t *>(pos) = static_cast<uint32_t>(val);
      return;
    case 8:
      *reinterpret_cast<uint64_t *>(pos) = static_cast<uint64_t>(val);
      return;
    default:
      // Invalid attr size
      PELOTON_ASSERT(false);
  }
}

struct FakeRawTuple {
  FakeRawTuple() = delete;
  ~FakeRawTuple() = delete;
  // Since all fields we store in pages are equal to or shorter than 8 bytes,
  // we can do equality checks on uint64_t always.
  // 0 return for non-primary key indexes should be treated as null.
  uint64_t Attribute(const storage::BlockLayout &layout, uint16_t col) {
    uint32_t pos = 0;
    for (uint16_t i = 0; i < col; i++)
      pos += layout.attr_sizes_[i];
    return ReadByteValue(layout.attr_sizes_[col], contents_ + pos);
  }

  byte contents_[0];
};

// Fill the given location with the specified amount of random bytes, using the
// given generator as a source of randomness.
template<typename Random>
void FillWithRandomBytes(uint32_t num_bytes, byte *out, Random &generator) {
  std::uniform_int_distribution<uint8_t> dist(0, UINT8_MAX);
  for (uint32_t i = 0; i < num_bytes; i++)
    out[i] = static_cast<byte>(dist(generator));
}

// This does NOT return a sensible tuple in general. This is just some filler
// to write into the storage layer and is devoid of meaning outside of this class.
template<typename Random>
FakeRawTuple *RandomTuple(const storage::BlockLayout &layout,
                          Random &generator) {
  auto *result = reinterpret_cast<FakeRawTuple *>(new byte[layout.tuple_size_]);
  FillWithRandomBytes(layout.tuple_size_,
                      reinterpret_cast<byte *>(result),
                      generator);
  return result;
}

// Write the given fake tuple into a block using the given access strategy,
// at the specified offset
void InsertTuple(FakeRawTuple *tuple,
                 storage::TupleAccessStrategy &tested,
                 const storage::BlockLayout &layout,
                 RawBlock *block,
                 uint32_t offset) {
  for (uint16_t col = 0; col < layout.num_cols_; col++) {
    uint64_t col_val = tuple->Attribute(layout, col);
    if (col_val != 0 || col == PRIMARY_KEY_OFFSET)
      WriteByteValue(layout.attr_sizes_[col],
                     tuple->Attribute(layout, col),
                     tested.AccessForceNotNull(block, col, offset));
    // Otherwise leave the field as null.
  }
}

void CheckTupleEqual(FakeRawTuple *expected,
                     storage::TupleAccessStrategy &tested,
                     const storage::BlockLayout &layout,
                     RawBlock *block,
                     uint32_t offset) {
  for (uint16_t col = 0; col < layout.num_cols_; col++) {
    uint64_t expected_col = expected->Attribute(layout, col);
    // 0 return for non-primary key indexes should be treated as null.
    bool null = (expected_col == 0) && (col != PRIMARY_KEY_OFFSET);
    byte *col_slot = tested.AccessWithNullCheck(block, col, offset);
    if (!null) {
      EXPECT_TRUE(col_slot != nullptr);
      EXPECT_EQ(expected->Attribute(layout, col),
                ReadByteValue(layout.attr_sizes_[col], col_slot));
    } else {
      EXPECT_TRUE(col_slot == nullptr);
    }
  }
}

#define TO_INT(p) reinterpret_cast<uintptr_t>(p)
// val address in [lower, upper) ?
template<typename A, typename B, typename C>
void CheckInBounds(A *val, B *lower, C *upper) {
  EXPECT_GE(TO_INT(val), TO_INT(lower));
  EXPECT_LT(TO_INT(val), TO_INT(upper));
};

template<typename A>
A *IncrementByBytes(A *ptr, uint32_t bytes) {
  return reinterpret_cast<A *>(reinterpret_cast<byte *>(ptr) + bytes);
}
}
}
