#pragma once

#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/exec/output.h"
#include "execution/sql/value.h"
#include "planner/plannodes/output_schema.h"
#include "test_util/test_harness.h"
#include "type/type_id.h"

// TODO(Amadou): Currently all checker only work on single integer columns. Ideally, we want them to work on arbitrary
// expressions, but this is no simple task. We would basically need an expression evaluator on output rows.

namespace noisepage::execution::compiler::test {
/**
 * Helper class to check if the output of a query is corrected.
 */
class OutputChecker {
 public:
  virtual void CheckCorrectness() = 0;
  virtual void ProcessBatch(const std::vector<std::vector<sql::Val *>> &output) = 0;
};

/**
 * Runs multiples output checkers at once
 */
class MultiChecker : public OutputChecker {
 public:
  /**
   * Constructor
   * @param checkers list of output checkers
   */
  explicit MultiChecker(std::vector<OutputChecker *> &&checkers) : checkers_{std::move(checkers)} {}

  /**
   * Call checkCorrectness on all output checkers
   */
  void CheckCorrectness() override {
    for (const auto checker : checkers_) {
      checker->CheckCorrectness();
    }
  }

  /**
   * Calls all output checkers
   * @param output current output batch
   */
  void ProcessBatch(const std::vector<std::vector<sql::Val *>> &output) override {
    for (const auto checker : checkers_) {
      checker->ProcessBatch(output);
    }
  }

 private:
  // list of checkers
  std::vector<OutputChecker *> checkers_;
};

using RowChecker = std::function<void(const std::vector<sql::Val *> &)>;
using CorrectnessFn = std::function<void()>;
class GenericChecker : public OutputChecker {
 public:
  GenericChecker(RowChecker row_checker, CorrectnessFn correctness_fn)
      : row_checker_{std::move(row_checker)}, correctness_fn_(std::move(correctness_fn)) {}

  void CheckCorrectness() override {
    if (correctness_fn_) correctness_fn_();
  }

  void ProcessBatch(const std::vector<std::vector<sql::Val *>> &output) override {
    if (!row_checker_) return;
    for (const auto &vals : output) {
      row_checker_(vals);
    }
  }

 private:
  RowChecker row_checker_;
  CorrectnessFn correctness_fn_;
};

/**
 * Checks if the number of output tuples is correct
 */
class NumChecker : public OutputChecker {
 public:
  /**
   * Constructor
   * @param expected_count the expected number of output tuples
   */
  explicit NumChecker(int64_t expected_count) : expected_count_{expected_count} {}

  /**
   * Checks if the expected number and the received number are the same
   */
  void CheckCorrectness() override { EXPECT_EQ(curr_count_, expected_count_); }

  /**
   * Increment the current count
   * @param output current output batch
   */
  void ProcessBatch(const std::vector<std::vector<sql::Val *>> &output) override { curr_count_ += output.size(); }

 private:
  // Current number of tuples
  int64_t curr_count_{0};
  // Expected number of tuples
  int64_t expected_count_;
};

/**
 * Checks that the values in a column satisfy a certain comparison.
 * @tparam CompFn comparison function to use
 */
class SingleIntComparisonChecker : public OutputChecker {
 public:
  SingleIntComparisonChecker(std::function<bool(int64_t, int64_t)> fn, uint32_t col_idx, int64_t rhs)
      : comp_fn_(std::move(fn)), col_idx_{col_idx}, rhs_{rhs} {}

  void CheckCorrectness() override {}

  void ProcessBatch(const std::vector<std::vector<sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto int_val = static_cast<const sql::Integer *>(vals[col_idx_]);
      EXPECT_TRUE(comp_fn_(int_val->val_, rhs_));
    }
  }

 private:
  std::function<bool(int64_t, int64_t)> comp_fn_;
  uint32_t col_idx_;
  int64_t rhs_;
};

/**
 * Checks if two joined columns are the same.
 */
class SingleIntJoinChecker : public OutputChecker {
 public:
  /**
   * Constructor
   * @param col1 first column of the join
   * @param col2 second column of the join
   */
  SingleIntJoinChecker(uint32_t col1, uint32_t col2) : col1_(col1), col2_(col2) {}

  /**
   * Does nothing. All the checks are done in ProcessBatch
   */
  void CheckCorrectness() override {}

  /**
   * Checks that the two joined columns are the same
   * @param output current output
   */
  void ProcessBatch(const std::vector<std::vector<sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto val1 = static_cast<const sql::Integer *>(vals[col1_]);
      auto val2 = static_cast<const sql::Integer *>(vals[col2_]);
      EXPECT_EQ(val1->val_, val2->val_);
    }
  }

 private:
  uint32_t col1_;
  uint32_t col2_;
};

/**
 * Checks that a columns sums up to an expected value
 */
class SingleIntSumChecker : public OutputChecker {
 public:
  /**
   * Constructor
   * @param col_idx index of column to sum
   * @param expected expected sum
   */
  SingleIntSumChecker(uint32_t col_idx, int64_t expected) : col_idx_{col_idx}, expected_{expected} {}

  /**
   * Checks of the expected sum and the received sum are the same
   */
  void CheckCorrectness() override { EXPECT_EQ(curr_sum_, expected_); }

  /**
   * Update the current sum
   * @param output current output batch
   */
  void ProcessBatch(const std::vector<std::vector<sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto int_val = static_cast<sql::Integer *>(vals[col_idx_]);
      if (!int_val->is_null_) curr_sum_ += int_val->val_;
    }
  }

 private:
  uint32_t col_idx_;
  int64_t curr_sum_{0};
  int64_t expected_;
};

/**
 * Checks that a given column is sorted
 */
class SingleIntSortChecker : public OutputChecker {
 public:
  /**
   * Constructor
   * @param col_idx column to check
   */
  explicit SingleIntSortChecker(uint32_t col_idx) : col_idx_{0} {}

  /**
   * Does nothing. All the checking is done in ProcessBatch.
   */
  void CheckCorrectness() override {}

  /**
   * Compares each value with the previous one to make sure they are sorted
   * @param output current output batch.
   */
  void ProcessBatch(const std::vector<std::vector<sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto int_val = static_cast<sql::Integer *>(vals[col_idx_]);
      if (int_val->is_null_) {
        EXPECT_TRUE(prev_val_.is_null_);
      } else {
        EXPECT_TRUE(prev_val_.is_null_ || int_val->val_ >= prev_val_.val_);
      }
      // Copy the value since the pointer does not belong to this class.
      prev_val_ = *int_val;
    }
  }

 private:
  sql::Integer prev_val_{sql::Integer::Null()};
  uint32_t col_idx_;
};

/**
 * Runs multiple OutputCallbacks at once
 */
class MultiOutputCallback {
 public:
  /**
   * Constructor
   * @param callbacks list of output callbacks
   */
  explicit MultiOutputCallback(std::vector<exec::OutputCallback> callbacks) : callbacks_{std::move(callbacks)} {}

  /**
   * OutputCallback function
   */
  void operator()(byte *tuples, uint32_t num_tuples, uint32_t tuple_size) {
    common::SpinLatch::ScopedSpinLatch guard(&latch_);
    for (auto &callback : callbacks_) {
      callback(tuples, num_tuples, tuple_size);
    }
  }

  /**
   * Constructs an OutputCallback such that invoking any copy of the OutputCallback
   * will invoke operator() on the same MultiOutputCallback instance.
   * @return OutputCallback that calls this MultiOutputCallback's operator()
   */
  exec::OutputCallback ConstructOutputCallback() {
    return
        [this](byte *tuples, uint32_t num_tuples, uint32_t tuple_size) { operator()(tuples, num_tuples, tuple_size); };
  }

 private:
  common::SpinLatch latch_;
  std::vector<exec::OutputCallback> callbacks_;
};

/**
 * An output callback that stores the rows in a vector and runs a checker on them.
 */
class OutputStore {
 public:
  /**
   * Constructor
   * @param checker checker to run
   * @param schema output schema of the query.
   */
  OutputStore(OutputChecker *checker, const noisepage::planner::OutputSchema *schema)
      : schema_(schema), checker_(checker) {}

  /**
   * OutputCallback function. This will gather the output in a vector.
   */
  void operator()(byte *tuples, uint32_t num_tuples, uint32_t tuple_size) {
    for (uint32_t row = 0; row < num_tuples; row++) {
      uint32_t curr_offset = 0;
      std::vector<sql::Val *> vals;
      for (const auto &col : schema_->GetColumns()) {
        auto alignment = execution::sql::ValUtil::GetSqlAlignment(col.GetType());
        if (!common::MathUtil::IsAligned(curr_offset, alignment)) {
          curr_offset = static_cast<uint32_t>(common::MathUtil::AlignTo(curr_offset, alignment));
        }
        // TODO(Amadou): Figure out to print other types.
        switch (col.GetType()) {
          case noisepage::type::TypeId::TINYINT:
          case noisepage::type::TypeId::SMALLINT:
          case noisepage::type::TypeId::BIGINT:
          case noisepage::type::TypeId::INTEGER: {
            auto *val = reinterpret_cast<sql::Integer *>(tuples + row * tuple_size + curr_offset);
            vals.emplace_back(val);
            break;
          }
          case noisepage::type::TypeId::BOOLEAN: {
            auto *val = reinterpret_cast<sql::BoolVal *>(tuples + row * tuple_size + curr_offset);
            vals.emplace_back(val);
            break;
          }
          case noisepage::type::TypeId::DECIMAL: {
            auto *val = reinterpret_cast<sql::Real *>(tuples + row * tuple_size + curr_offset);
            vals.emplace_back(val);
            break;
          }
          case noisepage::type::TypeId::DATE: {
            auto *val = reinterpret_cast<sql::DateVal *>(tuples + row * tuple_size + curr_offset);
            vals.emplace_back(val);
            break;
          }
          case noisepage::type::TypeId::TIMESTAMP: {
            auto *val = reinterpret_cast<sql::TimestampVal *>(tuples + row * tuple_size + curr_offset);
            vals.emplace_back(val);
            break;
          }
          case noisepage::type::TypeId::VARCHAR: {
            auto *val = reinterpret_cast<sql::StringVal *>(tuples + row * tuple_size + curr_offset);
            vals.emplace_back(val);
            break;
          }
          default:
            UNREACHABLE("Cannot output unsupported type!!!");
        }
        curr_offset += sql::ValUtil::GetSqlSize(col.GetType());
      }
      output_.emplace_back(vals);
    }
    checker_->ProcessBatch(output_);
    output_.clear();
  }

 private:
  // Current output batch
  std::vector<std::vector<sql::Val *>> output_;
  // output schema
  const noisepage::planner::OutputSchema *schema_;
  // checker to run
  OutputChecker *checker_;
};
}  // namespace noisepage::execution::compiler::test
