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

  // Write the given tuple (projected row) into a block using the given access strategy,
  // at the specified offset
  static void InsertTuple(const storage::ProjectedRow &tuple, const storage::TupleAccessStrategy *tested,
                          const storage::BlockLayout &layout, const storage::TupleSlot slot) {
    // Skip the version vector for tuples
    for (uint16_t col = 1; col < layout.num_cols_; col++) {
      const byte *val_ptr = tuple.AccessWithNullCheck(static_cast<uint16_t>(col - 1));
      if (val_ptr == nullptr) {
        tested->SetNull(slot, col);
      } else {
        // Read the value
        uint64_t val = storage::StorageUtil::ReadBytes(layout.attr_sizes_[col], val_ptr);
        storage::StorageUtil::WriteBytes(layout.attr_sizes_[col], val, tested->AccessForceNotNull(slot, col));
      }
    }
  }
};
}  // namespace terrier
