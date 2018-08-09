#pragma once

#include <random>
#include <utility>
#include <vector>

#include "common/typedefs.h"
#include "storage/storage_util.h"
#include "storage/tuple_access_strategy.h"
#include "util/storage_test_util.h"

namespace terrier {

struct TupleAccessStrategyBenchmarkUtil {
  TupleAccessStrategyBenchmarkUtil() = delete;

  // Fill the given location with the specified amount of random bytes, using the
  // given generator as a source of randomness.
  template <typename Random>
  static void FillWithRandomBytes(uint32_t num_bytes, byte *out, Random *generator) {
    std::uniform_int_distribution<uint8_t> dist(0, UINT8_MAX);
    for (uint32_t i = 0; i < num_bytes; i++) out[i] = static_cast<byte>(dist(*generator));
  }

  // Write the given fake tuple into a block using the given access strategy,
  // at the specified offset
  static void InsertTuple(const FakeRawTuple &tuple, const storage::TupleAccessStrategy *tested,
                          const storage::BlockLayout &layout, const storage::TupleSlot slot) {
    for (uint16_t col = 0; col < layout.num_cols_; col++) {
      uint64_t col_val = tuple.Attribute(layout, col);
      if (col_val != 0 || col == PRESENCE_COLUMN_ID)
        storage::StorageUtil::WriteBytes(layout.attr_sizes_[col], tuple.Attribute(layout, col),
                                         tested->AccessForceNotNull(slot, col));
      else
        tested->SetNull(slot, col);
      // Otherwise leave the field as null.
    }
  }

  // Using the given random generator, attempts to allocate a slot and write a
  // random tuple into it.
  template <typename Random>
  static void TryInsertFakeTuple(const storage::BlockLayout &layout, const storage::TupleAccessStrategy &tested,
                                 storage::RawBlock *block, Random *generator) {
    storage::TupleSlot slot;
    // There should always be enough slots.
    tested.Allocate(block, &slot);
    InsertTuple(FakeRawTuple(layout, generator), &tested, layout, slot);
  }
};
}  // namespace terrier
