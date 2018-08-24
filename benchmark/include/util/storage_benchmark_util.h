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

  // Write the given tuple (projected row) into a block using the given access strategy,
  // at the specified offset
  static void InsertTuple(const storage::ProjectedRow &tuple, const storage::TupleAccessStrategy *tested,
                          const storage::BlockLayout &layout, const storage::TupleSlot slot) {
    // Skip the version vector for tuples
    for (uint16_t col = 1; col < layout.NumCols(); col++) {
      const byte *val_ptr = tuple.AccessWithNullCheck(static_cast<uint16_t>(col - 1));
      if (val_ptr == nullptr) {
        tested->SetNull(slot, col);
      } else {
        // Read the value
        uint64_t val = storage::StorageUtil::ReadBytes(layout.AttrSize(col), val_ptr);
        storage::StorageUtil::WriteBytes(layout.AttrSize(col), val, tested->AccessForceNotNull(slot, col));
      }
    }
  }
};
}  // namespace terrier
