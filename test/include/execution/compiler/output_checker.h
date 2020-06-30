#pragma once

#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "execution/sql/value.h"
#include "execution/tpl_test.h"
#include "planner/plannodes/output_schema.h"

// TODO(Amadou): Currently all checker only work on single integer columns. Ideally, we want them to work on arbitrary
// expressions, but this is no simple task. We would basically need an expression evaluator on output rows.

namespace terrier::execution::compiler {
/**
 * Helper class to check if the output of a query is corrected.
 */
class OutputChecker {
 public:
  virtual void CheckCorrectness() = 0;
  virtual void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) = 0;
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
  explicit MultiChecker(std::vector<OutputChecker *> &&checkers) : checkers_(std::move(checkers)) {}

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
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto checker : checkers_) {
      checker->ProcessBatch(output);
    }
  }

 private:
  // list of checkers
  std::vector<OutputChecker *> checkers_;
};

using RowChecker = std::function<void(const std::vector<const sql::Val *> &)>;
using CorrectnessFn = std::function<void()>;
class GenericChecker : public OutputChecker {
 public:
  GenericChecker(RowChecker row_checker, CorrectnessFn correctness_fn)
      : row_checker_(std::move(row_checker)), correctness_fn_(std::move(correctness_fn)) {}

  void CheckCorrectness() override {
    if (correctness_fn_) correctness_fn_();
  }

  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
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
class TupleCounterChecker : public OutputChecker {
 public:
  /**
   * Constructor.
   * @param expected_count the expected number of output tuples
   */
  explicit TupleCounterChecker(int64_t expected_count) : curr_count_(0), expected_count_(expected_count) {}

  /**
   * Checks if the expected number and the received number are the same.
   */
  void CheckCorrectness() override { EXPECT_EQ(curr_count_, expected_count_); }

  /**
   * Increment the current count.
   * @param output current output batch.
   */
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override { curr_count_ += output.size(); }

 private:
  // Current number of tuples
  int64_t curr_count_;
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
      : comp_fn_(std::move(fn)), col_idx_(col_idx), rhs_(rhs) {}

  void CheckCorrectness() override {}

  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto int_val = static_cast<const sql::Integer *>(vals[col_idx_]);
      EXPECT_TRUE(comp_fn_(int_val->val, rhs_)) << "lhs=" << int_val->val << ",rhs=" << rhs_;
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
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto val1 = static_cast<const sql::Integer *>(vals[col1_]);
      auto val2 = static_cast<const sql::Integer *>(vals[col2_]);
      ASSERT_EQ(val1->val, val2->val);
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
  SingleIntSumChecker(uint32_t col_idx, int64_t expected) : col_idx_(col_idx), curr_sum_(0), expected_(expected) {}

  /**
   * Checks of the expected sum and the received sum are the same
   */
  void CheckCorrectness() override { EXPECT_EQ(curr_sum_, expected_); }

  /**
   * Update the current sum
   * @param output current output batch
   */
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto int_val = static_cast<const sql::Integer *>(vals[col_idx_]);
      if (!int_val->is_null) curr_sum_ += int_val->val;
    }
  }

 private:
  uint32_t col_idx_;
  int64_t curr_sum_;
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
  explicit SingleIntSortChecker(uint32_t col_idx) : col_idx_(col_idx), prev_val_(sql::Integer::Null()) {}

  /**
   * Does nothing. All the checking is done in ProcessBatch.
   */
  void CheckCorrectness() override {}

  /**
   * Compares each value with the previous one to make sure they are sorted
   * @param output current output batch.
   */
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto int_val = static_cast<const sql::Integer *>(vals[col_idx_]);
      if (int_val->is_null) {
        EXPECT_TRUE(prev_val_.is_null);
      } else {
        EXPECT_TRUE(prev_val_.is_null || int_val->val >= prev_val_.val) << prev_val_.val << " NOT < " << int_val->val;
      }
      prev_val_ = *int_val;
    }
  }

 private:
  uint32_t col_idx_;
  sql::Integer prev_val_;
};

/**
 * Runs multiple OutputCallbacks at once
 */
class MultiOutputCallback : public sql::ResultConsumer {
 public:
  /**
   * Constructor
   * @param callbacks list of output callbacks
   */
  explicit MultiOutputCallback(std::vector<sql::ResultConsumer *> callbacks) : callbacks_(std::move(callbacks)) {}

  /**
   * OutputCallback function
   */
  void Consume(const sql::OutputBuffer &tuples) override {
    for (auto &callback : callbacks_) {
      callback->Consume(tuples);
    }
  }

 private:
  std::vector<sql::ResultConsumer *> callbacks_;
};

/**
 * An output callback that stores the rows in a vector and runs a checker on them.
 */
class OutputCollectorAndChecker : public sql::ResultConsumer {
 public:
  /**
   * Constructor
   * @param checker checker to run
   * @param schema output schema of the query.
   */
  OutputCollectorAndChecker(OutputChecker *checker, const planner::OutputSchema *schema)
      : schema_(schema), checker_(checker) {}

  /**
   * OutputCallback function. This will gather the output in a vector.
   */
  void Consume(const sql::OutputBuffer &tuples) override {
    for (uint32_t row = 0; row < tuples.size(); row++) {
      uint32_t curr_offset = 0;
      std::vector<const sql::Val *> vals;
      vals.reserve(schema_->NumColumns());
      for (uint16_t col = 0; col < schema_->GetColumns().size(); col++) {
        switch (schema_->GetColumn(col).GetType()) {
          case sql::TypeId::TinyInt:
          case sql::TypeId::SmallInt:
          case sql::TypeId::BigInt:
          case sql::TypeId::Integer: {
            auto *val = reinterpret_cast<const sql::Integer *>(tuples[row] + curr_offset);
            vals.emplace_back(val);
            curr_offset += sizeof(sql::Integer);
            break;
          }
          case sql::TypeId::Boolean: {
            auto *val = reinterpret_cast<const sql::BoolVal *>(tuples[row] + curr_offset);
            vals.emplace_back(val);
            curr_offset += sizeof(sql::BoolVal);
            break;
          }
          case sql::TypeId::Float:
          case sql::TypeId::Double: {
            auto *val = reinterpret_cast<const sql::Real *>(tuples[row] + curr_offset);
            vals.emplace_back(val);
            curr_offset += sizeof(sql::Real);
            break;
          }
          case sql::TypeId::Date: {
            auto *val = reinterpret_cast<const sql::DateVal *>(tuples[row] + curr_offset);
            vals.emplace_back(val);
            curr_offset += sizeof(sql::DateVal);
            break;
          }
          case sql::TypeId::Varchar: {
            auto *val = reinterpret_cast<const sql::StringVal *>(tuples[row] + curr_offset);
            vals.emplace_back(val);
            curr_offset += sizeof(sql::StringVal);
            break;
          }
          default:
            UNREACHABLE("Cannot output unsupported type!!!");
        }
      }
      output.emplace_back(vals);
    }
    checker_->ProcessBatch(output);
    output.clear();
  }

 private:
  // Current output batch
  std::vector<std::vector<const sql::Val *>> output;
  // output schema
  const planner::OutputSchema *schema_;
  // checker to run
  OutputChecker *checker_;
};

}  // namespace terrier::execution::compiler
