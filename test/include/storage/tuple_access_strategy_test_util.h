#pragma once
#include <random>
#include <unordered_map>

#include "gtest/gtest.h"
#include "storage/tuple_access_strategy.h"
#include "common/common_defs.h"
#include "common/test_util.h"

namespace terrier {
namespace testutil {
// Returns a random layout that is guaranteed to be valid.
template<typename Random>
storage::BlockLayout RandomLayout(Random &generator,
                                  uint16_t max_cols = UINT16_MAX) {
  PELOTON_ASSERT(max_cols > 1);
  // We probably won't allow tables with 0 columns
  uint16_t num_attrs =
      std::uniform_int_distribution<uint16_t>(1, max_cols)(generator);
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
    case 1:*reinterpret_cast<uint8_t *>(pos) = static_cast<uint8_t>(val);
      return;
    case 2:*reinterpret_cast<uint16_t *>(pos) = static_cast<uint16_t>(val);
      return;
    case 4:*reinterpret_cast<uint32_t *>(pos) = static_cast<uint32_t>(val);
      return;
    case 8:*reinterpret_cast<uint64_t *>(pos) = static_cast<uint64_t>(val);
      return;
    default:
      // Invalid attr size
      PELOTON_ASSERT(false);
  }
}

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
struct FakeRawTuple {
  template <typename Random>
  FakeRawTuple(const storage::BlockLayout &layout, Random &generator)
      : layout_(layout), attr_offsets_(), contents_(new byte[layout.tuple_size_]) {
    uint32_t pos = 0;
    for (uint16_t col = 0; col < layout.num_cols_; col++) {
      attr_offsets_.push_back(pos);
      pos += layout.attr_sizes_[col];
    }
    FillWithRandomBytes(layout.tuple_size_, contents_, generator);
  };

  ~FakeRawTuple() {
    delete[] contents_;
  }

  // Since all fields we store in pages are equal to or shorter than 8 bytes,
  // we can do equality checks on uint64_t always.
  // 0 return for non-primary key indexes should be treated as null.
  uint64_t Attribute(const storage::BlockLayout &layout, uint16_t col) {
    return ReadByteValue(layout.attr_sizes_[col],
                         contents_ + attr_offsets_[col]);
  }

  const storage::BlockLayout &layout_;
  std::vector<uint32_t> attr_offsets_;
  byte *contents_;
};

// Write the given fake tuple into a block using the given access strategy,
// at the specified offset
void InsertTuple(FakeRawTuple &tuple,
                 storage::TupleAccessStrategy &tested,
                 const storage::BlockLayout &layout,
                 storage::TupleSlot slot) {
  for (uint16_t col = 0; col < layout.num_cols_; col++) {
    uint64_t col_val = tuple.Attribute(layout, col);
    if (col_val != 0 || col == PRIMARY_KEY_OFFSET)
      WriteByteValue(layout.attr_sizes_[col],
                     tuple.Attribute(layout, col),
                     tested.AccessForceNotNull(slot, col));
    else
      tested.SetNull(slot, col);
    // Otherwise leave the field as null.
  }
}

// Check that the written tuple is the same as the expected one
void CheckTupleEqual(FakeRawTuple &expected,
                     storage::TupleAccessStrategy &tested,
                     const storage::BlockLayout &layout,
                     storage::TupleSlot slot) {
  for (uint16_t col = 0; col < layout.num_cols_; col++) {
    uint64_t expected_col = expected.Attribute(layout, col);
    // 0 return for non-primary key indexes should be treated as null.
    bool null = (expected_col == 0) && (col != PRIMARY_KEY_OFFSET);
    byte *col_slot = tested.AccessWithNullCheck(slot, col);
    if (!null) {
      EXPECT_TRUE(col_slot != nullptr);
      EXPECT_EQ(expected.Attribute(layout, col),
                ReadByteValue(layout.attr_sizes_[col], col_slot));
    } else {
      EXPECT_TRUE(col_slot == nullptr);
    }
  }
}

// Using the given random generator, attempts to allocate a slot and write a
// random tuple into it. The slot and the tuple are logged in the given map.
// Checks are performed to make sure the insertion is sensible.
template<typename Random>
std::pair<const storage::TupleSlot, testutil::FakeRawTuple> &TryInsertFakeTuple(
    const storage::BlockLayout &layout,
    storage::TupleAccessStrategy &tested,
    storage::RawBlock *block,
    std::unordered_map<storage::TupleSlot, testutil::FakeRawTuple> &tuples,
    Random &generator) {
  storage::TupleSlot slot;
  // There should always be enough slots.
  EXPECT_TRUE(tested.Allocate(block, slot));
  EXPECT_TRUE(tested.ColumnNullBitmap(block,
                                      PRIMARY_KEY_OFFSET)->Test(slot.GetOffset()));

  // Construct a random tuple and associate it with the tuple slot
  auto result = tuples.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(slot),
      std::forward_as_tuple(layout, generator));
  // The tuple slot is not something that is already in use.
  EXPECT_TRUE(result.second);
  testutil::InsertTuple(result.first->second, tested, layout, slot);
  return *(result.first);
}

}
}
