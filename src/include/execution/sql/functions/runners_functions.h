#pragma once

#include "execution/sql/value.h"

namespace noisepage::execution::sql {

/**
 * Utility class to handle mini runners functions
 */
class EXPORT MiniRunnersFunctions {
 public:
  // Delete to force only static functions
  MiniRunnersFunctions() = delete;

  /**
   * Emit tuples
   * @param ctx ExecutionContext
   * @param num_tuples Number tuples
   * @param num_cols Number columns
   * @param num_int_cols Number ints
   * @param num_real_cols Number reals
   */
  static void EmitTuples(noisepage::execution::exec::ExecutionContext *ctx, const Integer &num_tuples,
                         const Integer &num_cols, const Integer &num_int_cols, const Integer &num_real_cols) {
    if (num_tuples.val_ < 2) return;

    // We are already in an output slot
    auto *output_buffer = ctx->OutputBufferNew();

    static_assert(sizeof(Integer) == sizeof(Real));
    for (auto row = 0; row < num_tuples.val_ - 1; row++) {
      auto output_alloc = output_buffer->AllocOutputSlot();

      auto j = 0;
      for (auto icol = 0; icol < num_int_cols.val_; icol++) {
        reinterpret_cast<Integer *>(output_alloc)[j] = execution::sql::Integer(row);
        j++;
      }

      for (auto rcol = 0; rcol < num_real_cols.val_; rcol++) {
        reinterpret_cast<Real *>(output_alloc)[j] = execution::sql::Real(row * 1.0);
        j++;
      }
    }

    // Destroy the OutputBuffer
    output_buffer->Finalize();
    auto *pool = output_buffer->GetMemoryPool();
    output_buffer->~OutputBuffer();
    pool->Deallocate(output_buffer, sizeof(exec::OutputBuffer));
  }
};

}  // namespace noisepage::execution::sql
