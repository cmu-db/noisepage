#pragma once

#include <random>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/typedefs.h"
#include "gtest/gtest.h"
#include "util/multi_threaded_test_util.h"
#include "storage/storage_util.h"
#include "storage/tuple_access_strategy.h"

namespace terrier {

// TODO(Tianyu): Eventually these should be rewritten into StorageTestUtil
// This does NOT return a sensible tuple in general. This is just some filler
// to write into the storage layer and is devoid of meaning outside of this class.
struct FakeRawTuple {
  template <typename Random>
  FakeRawTuple(const storage::BlockLayout &layout, Random &generator);
  ~FakeRawTuple() { delete[] contents_; }

  // Since all fields we store in pages are equal to or shorter than 8 bytes,
  // we can do equality checks on uint64_t always.
  // 0 return for non-primary key indexes should be treated as null.
  uint64_t Attribute(const storage::BlockLayout &layout, uint16_t col) {
    return storage::StorageUtil::ReadBytes(layout.attr_sizes_[col], contents_ + attr_offsets_[col]);
  }

  const storage::BlockLayout &layout_;
  std::vector<uint32_t> attr_offsets_;
  byte *contents_;
};

struct TupleAccessStrategyTestUtil {
  TupleAccessStrategyTestUtil() = delete;
  // Returns a random layout that is guaranteed to be valid.
  template <typename Random>
  static storage::BlockLayout RandomLayout(Random &generator, uint16_t max_cols = UINT16_MAX) {
    PELOTON_ASSERT(max_cols > 1);
    // We probably won't allow tables with 0 columns
    uint16_t num_attrs = std::uniform_int_distribution<uint16_t>(1, max_cols)(generator);
    std::vector<uint8_t> possible_attr_sizes{1, 2, 4, 8}, attr_sizes(num_attrs);
    for (uint16_t i = 0; i < num_attrs; i++)
      attr_sizes[i] = *MultiThreadedTestUtil::UniformRandomElement(possible_attr_sizes, generator);
    return {num_attrs, attr_sizes};
  }

  // Fill the given location with the specified amount of random bytes, using the
  // given generator as a source of randomness.
  template <typename Random>
  static void FillWithRandomBytes(uint32_t num_bytes, byte *out, Random &generator) {
    std::uniform_int_distribution<uint8_t> dist(0, UINT8_MAX);
    for (uint32_t i = 0; i < num_bytes; i++) out[i] = static_cast<byte>(dist(generator));
  }

  // Write the given fake tuple into a block using the given access strategy,
  // at the specified offset
  static void InsertTuple(FakeRawTuple &tuple, const storage::TupleAccessStrategy &tested,
                          const storage::BlockLayout &layout, storage::TupleSlot slot) {
    for (uint16_t col = 0; col < layout.num_cols_; col++) {
      uint64_t col_val = tuple.Attribute(layout, col);
      if (col_val != 0 || col == PRESENCE_COLUMN_ID)
        storage::StorageUtil::WriteBytes(layout.attr_sizes_[col], tuple.Attribute(layout, col),
                                         tested.AccessForceNotNull(slot, col));
      else
        tested.SetNull(slot, col);
      // Otherwise leave the field as null.
    }
  }

  // Check that the written tuple is the same as the expected one
  static void CheckTupleEqual(FakeRawTuple &expected, storage::TupleAccessStrategy &tested,
                              const storage::BlockLayout &layout, storage::TupleSlot slot) {
    for (uint16_t col = 0; col < layout.num_cols_; col++) {
      uint64_t expected_col = expected.Attribute(layout, col);
      // 0 return for non-primary key indexes should be treated as null.
      bool null = (expected_col == 0) && (col != PRESENCE_COLUMN_ID);
      byte *col_slot = tested.AccessWithNullCheck(slot, col);
      if (!null) {
        EXPECT_TRUE(col_slot != nullptr);
        EXPECT_EQ(expected.Attribute(layout, col), storage::StorageUtil::ReadBytes(layout.attr_sizes_[col], col_slot));
      } else {
        EXPECT_TRUE(col_slot == nullptr);
      }
    }
  }

  // Using the given random generator, attempts to allocate a slot and write a
  // random tuple into it. The slot and the tuple are logged in the given map.
  // Checks are performed to make sure the insertion is sensible.
  template <typename Random>
  static std::pair<const storage::TupleSlot, FakeRawTuple> &TryInsertFakeTuple(
      const storage::BlockLayout &layout, const storage::TupleAccessStrategy &tested, storage::RawBlock *block,
      std::unordered_map<storage::TupleSlot, FakeRawTuple> &tuples, Random &generator) {
    storage::TupleSlot slot;
    // There should always be enough slots.
    EXPECT_TRUE(tested.Allocate(block, &slot));
    EXPECT_TRUE(tested.ColumnNullBitmap(block, PRESENCE_COLUMN_ID)->Test(slot.GetOffset()));

    // Construct a random tuple and associate it with the tuple slot
    auto result =
        tuples.emplace(std::piecewise_construct, std::forward_as_tuple(slot), std::forward_as_tuple(layout, generator));
    // The tuple slot is not something that is already in use.
    EXPECT_TRUE(result.second);
    InsertTuple(result.first->second, tested, layout, slot);
    return *(result.first);
  }
};

template <class Random>
FakeRawTuple::FakeRawTuple(const terrier::storage::BlockLayout &layout, Random &generator)
    : layout_(layout), attr_offsets_(), contents_(new byte[layout.tuple_size_]) {
  uint32_t pos = 0;
  for (uint16_t col = 0; col < layout.num_cols_; col++) {
    attr_offsets_.push_back(pos);
    pos += layout.attr_sizes_[col];
  }
  TupleAccessStrategyTestUtil::FillWithRandomBytes(layout.tuple_size_, contents_, generator);
}

}  // namespace terrier
