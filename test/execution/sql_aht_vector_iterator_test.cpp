#include <memory>
#include <random>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "execution/sql/aggregation_hash_table.h"
#include "execution/sql/vector_filter_executor.h"
#include "execution/sql/vector_projection.h"
#include "execution/sql/vector_projection_iterator.h"
#include "execution/sql_test.h"

namespace terrier::execution::sql::test {

/**
 * An input tuple, this is what we use to probe and update aggregates
 */
struct InputTuple {
  int64_t key_, col_a_;

  InputTuple(uint64_t key, uint64_t col_a) : key_(key), col_a_(col_a) {}

  hash_t Hash() const noexcept { return common::HashUtil::Hash(key_); }
};

/**
 * This is the tuple tracking aggregate values. It simulates:
 *
 * SELECT key, SUM(col_a), SUM(col_a*2), SUM(col_a*10) ...
 */
struct AggTuple {
  int64_t key_, count1_, count2_, count3_;

  explicit AggTuple(const InputTuple &input) : key_(input.key_), count1_(0), count2_(0), count3_(0) { Advance(input); }

  void Advance(const InputTuple &input) {
    count1_++;
    count2_ += input.col_a_ * 2;
    count3_ += input.col_a_ * 10;
  }
};

// The function to determine whether an aggregate stored in the hash table and
// an input have equivalent keys.
static bool AggTupleKeyEq(const void *table_tuple, const void *probe_tuple) {
  auto *lhs = reinterpret_cast<const AggTuple *>(table_tuple);
  auto *rhs = reinterpret_cast<const InputTuple *>(probe_tuple);
  return lhs->key_ == rhs->key_;
}

static void Transpose(const HashTableEntry *agg_entries[], const uint64_t size, VectorProjectionIterator *const vpi) {
  for (uint32_t i = 0; i < size; i++, vpi->Advance()) {
    const auto *agg = agg_entries[i]->PayloadAs<AggTuple>();
    vpi->SetValue<int64_t, false>(0, agg->key_, false);
    vpi->SetValue<int64_t, false>(1, agg->count1_, false);
    vpi->SetValue<int64_t, false>(2, agg->count2_, false);
    vpi->SetValue<int64_t, false>(3, agg->count3_, false);
  }
}

class AggregationHashTableVectorIteratorTest : public SqlBasedTest {
 public:
  AggregationHashTableVectorIteratorTest() {
    std::vector<catalog::Schema::Column> cols = {
        {"key", type::TypeId::BIGINT, true, parser::ConstantValueExpression(type::TypeId::BIGINT)},
        {"count1", type::TypeId::BIGINT, true, parser::ConstantValueExpression(type::TypeId::BIGINT)},
        {"count2", type::TypeId::BIGINT, true, parser::ConstantValueExpression(type::TypeId::BIGINT)},
        {"count3", type::TypeId::BIGINT, true, parser::ConstantValueExpression(type::TypeId::BIGINT)},
    };
    schema_ = std::make_unique<catalog::Schema>(std::move(cols));
  }

  std::vector<const catalog::Schema::Column *> OutputSchema() {
    std::vector<const catalog::Schema::Column *> ret;
    for (const auto &col : schema_->GetColumns()) {
      ret.push_back(&col);
    }
    return ret;
  }

  static void PopulateAggHT(AggregationHashTable *aht, const uint32_t num_aggs, const uint32_t num_rows,
                            uint32_t cola = 1) {
    for (uint32_t i = 0; i < num_rows; i++) {
      auto input = InputTuple(i % num_aggs, cola);
      auto existing = reinterpret_cast<AggTuple *>(aht->Lookup(input.Hash(), AggTupleKeyEq, &input));
      if (existing == nullptr) {
        new (aht->AllocInputTuple(input.Hash())) AggTuple(input);
      } else {
        existing->Advance(input);
      }
    }
  }

 private:
  std::unique_ptr<catalog::Schema> schema_;
};

// NOLINTNEXTLINE
TEST_F(AggregationHashTableVectorIteratorTest, IterateEmptyAggregation) {
  auto exec_ctx = MakeExecCtx();
  // Empty table
  AggregationHashTable agg_ht(exec_ctx->GetExecutionSettings(), exec_ctx.get(), sizeof(AggTuple));

  // Iterate
  AHTVectorIterator iter(agg_ht, OutputSchema(), Transpose);
  for (; iter.HasNext(); iter.Next(Transpose)) {
    FAIL() << "Iteration should not occur on empty aggregation hash table";
  }
}

// NOLINTNEXTLINE
TEST_F(AggregationHashTableVectorIteratorTest, IterateSmallAggregation) {
  auto exec_ctx = MakeExecCtx();
  constexpr uint32_t num_aggs = 400;
  constexpr uint32_t group_size = 10;
  constexpr uint32_t num_tuples = num_aggs * group_size;

  // Insert 'num_tuples' tuples into an aggregation table to force the creation
  // of 'num_aggs' unique aggregates. Each aggregate should receive 'group_size'
  // tuples as input whose column value is 'cola'. The key range of aggregates
  // is [0, num_aggs).
  //
  // We need to ensure:
  // 1. After transposition, we receive exactly 'num_aggs' unique aggregates.
  // 2. For each aggregate, the associated count/sums is correct.

  AggregationHashTable agg_ht(exec_ctx->GetExecutionSettings(), exec_ctx.get(), sizeof(AggTuple));

  // Populate
  PopulateAggHT(&agg_ht, num_aggs, num_tuples, 1 /* cola */);

  std::unordered_set<uint64_t> reference;

  // Iterate
  AHTVectorIterator iter(agg_ht, OutputSchema(), Transpose);
  for (; iter.HasNext(); iter.Next(Transpose)) {
    auto *vpi = iter.GetVectorProjectionIterator();
    EXPECT_FALSE(vpi->IsFiltered());
    EXPECT_EQ(4u, vpi->GetVectorProjection()->GetColumnCount());
    EXPECT_GT(vpi->GetVectorProjection()->GetTotalTupleCount(), 0);

    for (; vpi->HasNext(); vpi->Advance()) {
      auto agg_key = *vpi->GetValue<int64_t, false>(0, nullptr);
      auto agg_count_1 = *vpi->GetValue<int64_t, false>(1, nullptr);
      auto agg_count_2 = *vpi->GetValue<int64_t, false>(2, nullptr);
      auto agg_count_3 = *vpi->GetValue<int64_t, false>(3, nullptr);
      EXPECT_LT(agg_key, num_aggs);
      EXPECT_EQ(group_size, agg_count_1);
      EXPECT_EQ(agg_count_1 * 2u, agg_count_2);
      EXPECT_EQ(agg_count_1 * 10u, agg_count_3);
      EXPECT_EQ(0u, reference.count(agg_key));
      reference.insert(agg_key);
    }
  }

  EXPECT_EQ(num_aggs, reference.size());
}

// NOLINTNEXTLINE
TEST_F(AggregationHashTableVectorIteratorTest, FilterPostAggregation) {
  auto exec_ctx = MakeExecCtx();
  constexpr uint32_t num_aggs = 4000;
  constexpr uint32_t group_size = 10;
  constexpr uint32_t num_tuples = num_aggs * group_size;

  constexpr int32_t agg_needle_key = 686;

  // Insert 'num_tuples' tuples into an aggregation table to force the creation
  // of 'num_aggs' unique aggregates. Each aggregate should receive 'group_size'
  // tuples as input whose column value is 'cola'. The key range of aggregates
  // is [0, num_aggs).
  //
  // Apply a filter to the output of the iterator looking for a unique key.
  // There should only be one match since keys are unique.

  AggregationHashTable agg_ht(exec_ctx->GetExecutionSettings(), exec_ctx.get(), sizeof(AggTuple));

  PopulateAggHT(&agg_ht, num_aggs, num_tuples, 1 /* cola */);

  // Iterate
  uint32_t num_needle_matches = 0;
  AHTVectorIterator iter(agg_ht, OutputSchema(), Transpose);
  for (; iter.HasNext(); iter.Next(Transpose)) {
    auto *vpi = iter.GetVectorProjectionIterator();
    vpi->RunFilter([&]() {
      auto agg_key = *vpi->GetValue<int64_t, false>(0, nullptr);
      return agg_key == agg_needle_key;
    });
    num_needle_matches += vpi->GetSelectedTupleCount();
  }

  EXPECT_EQ(1u, num_needle_matches);
}

// NOLINTNEXTLINE
TEST_F(AggregationHashTableVectorIteratorTest, DISABLED_Perf) {
  auto exec_ctx = MakeExecCtx();
  uint64_t taat_ret = 0, vaat_ret = 0;

  for (uint32_t size : {10, 100, 1000, 10000, 100000, 1000000, 10000000}) {
    // The table
    AggregationHashTable agg_ht(exec_ctx->GetExecutionSettings(), exec_ctx.get(), sizeof(AggTuple));

    // Populate
    PopulateAggHT(&agg_ht, size, size * 10, 1 /* cola */);

    constexpr int32_t filter_val = 1000;

    UNUSED_ATTRIBUTE auto vaat_ms = Bench(4, [&]() {
      vaat_ret = 0;
      TupleIdList tids(common::Constants::K_DEFAULT_VECTOR_SIZE);
      AHTVectorIterator iter(agg_ht, OutputSchema(), Transpose);
      for (; iter.HasNext(); iter.Next(Transpose)) {
        auto *vector_projection = iter.GetVectorProjectionIterator()->GetVectorProjection();
        tids.Resize(vector_projection->GetTotalTupleCount());
        tids.AddAll();
        VectorFilterExecutor::SelectLessThanVal(exec_ctx->GetExecutionSettings(), vector_projection, 0,
                                                GenericValue::CreateBigInt(filter_val), &tids);
        vector_projection->SetFilteredSelections(tids);
      }
    });

    UNUSED_ATTRIBUTE auto taat_ms = Bench(4, [&]() {
      taat_ret = 0;
      AHTIterator iter(agg_ht);
      for (; iter.HasNext(); iter.Next()) {
        auto *agg_row = reinterpret_cast<const AggTuple *>(iter.GetCurrentAggregateRow());
        if (agg_row->key_ < filter_val) {
          taat_ret++;
        }
      }
    });

    EXECUTION_LOG_TRACE("===== Size {} =====", size);
    EXECUTION_LOG_TRACE("Taat: {:.2f} ms ({}), Vaat: {:.2f} ({})", taat_ms, taat_ret, vaat_ms, vaat_ret);
  }
}

}  // namespace terrier::execution::sql::test
