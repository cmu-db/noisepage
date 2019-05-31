#include <memory>
#include <random>
#include <unordered_map>
#include <vector>

#include "execution/tpl_test.h"  // NOLINT

#include "execution/sql/aggregation_hash_table.h"
#include "execution/util/hash.h"

namespace tpl::sql::test {

/// This is the tuple tracking aggregate values
struct AggTuple {
  u64 key, count1, count2, count3;

  explicit AggTuple(u64 key) : key(key), count1(0), count2(0), count3(0) {}
};

/// An input tuple, this is what we use to probe and update aggregates
struct InputTuple {
  u64 key, col_a;

  explicit InputTuple(u64 key, u64 col_a) : key(key), col_a(col_a) {}

  hash_t Hash() const noexcept { return util::Hasher::Hash(reinterpret_cast<const u8 *>(&key), sizeof(key)); }
};

/// The function to determine whether two tuples have equivalent keys
static inline bool TupleKeyEq(const void *probe_tuple, const void *table_tuple) {
  auto *lhs = reinterpret_cast<const InputTuple *>(probe_tuple);
  auto *rhs = reinterpret_cast<const AggTuple *>(table_tuple);
  return lhs->key == rhs->key;
}

class AggregationHashTableTest : public TplTest {
 public:
  AggregationHashTableTest() : memory_(nullptr), agg_table_(&memory_, sizeof(AggTuple)) {}

  MemoryPool *memory() { return &memory_; }

  AggregationHashTable *agg_table() { return &agg_table_; }

 private:
  MemoryPool memory_;
  AggregationHashTable agg_table_;
};

TEST_F(AggregationHashTableTest, SimpleRandomInsertionTest) {
  const u32 num_tuples = 1000;

  // The reference table
  std::unordered_map<u64, std::unique_ptr<AggTuple>> ref_agg_table;

  std::mt19937 generator;
  std::uniform_int_distribution<u64> distribution;

  // Insert a few random tuples
  for (u32 idx = 0; idx < num_tuples; idx++) {
    InputTuple input(distribution(generator), 1);
    auto *existing = reinterpret_cast<AggTuple *>(
        agg_table()->Lookup(input.Hash(), TupleKeyEq, reinterpret_cast<const void *>(&input)));

    if (existing != nullptr) {
      // The reference table should have an equivalent aggregate tuple
      auto ref_iter = ref_agg_table.find(input.key);
      ASSERT_TRUE(ref_iter != ref_agg_table.end());
      ASSERT_EQ(ref_iter->second->count1, existing->count1);

      // Update aggregate
      existing->count1 += input.col_a;
      ref_iter->second->count1 += input.col_a;
    } else {
      // The reference table shouldn't have the aggregate
      auto ref_iter = ref_agg_table.find(input.key);
      ASSERT_TRUE(ref_iter == ref_agg_table.end());

      // Insert a new entry into the hash table and the reference table
      existing = new (agg_table()->Insert(input.Hash())) AggTuple(input.key);
      existing->count1 = 0;

      // Make a copy of what we inserted into the agg hash table
      ref_agg_table.emplace(input.key, std::make_unique<AggTuple>(*existing));
    }
  }
}

TEST_F(AggregationHashTableTest, SimplePartitionedInsertionTest) {
  const u32 num_tuples = 10000;

  std::mt19937 generator;
  std::uniform_int_distribution<u64> distribution;

  // Insert a few random tuples
  for (u32 idx = 0; idx < num_tuples; idx++) {
    InputTuple input(distribution(generator), 1);
    auto *existing = reinterpret_cast<AggTuple *>(
        agg_table()->Lookup(input.Hash(), TupleKeyEq, reinterpret_cast<const void *>(&input)));

    if (existing != nullptr) {
      existing->count1 += input.col_a;
    } else {
      // Insert a new entry into the hash table and the reference table
      existing = new (agg_table()->InsertPartitioned(input.Hash())) AggTuple(input.key);
      existing->count1 = 0;
    }
  }
}

}  // namespace tpl::sql::test
