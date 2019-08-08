#include <random>
#include <unordered_map>
#include <vector>

#include "execution/tpl_test.h"

#include "execution/sql/generic_hash_table.h"
#include "execution/util/hash.h"

namespace terrier::execution::sql::test {

class GenericHashTableTest : public TplTest {};

struct TestEntry : public HashTableEntry {
  u32 key{0}, value{0};
  TestEntry() : HashTableEntry() {}
  TestEntry(u32 key, u32 value) : HashTableEntry(), key(key), value(value) {}
};

// NOLINTNEXTLINE
TEST_F(GenericHashTableTest, EmptyIteratorTest) {
  GenericHashTable table;

  //
  // Test: iteration shouldn't begin on an uninitialized table
  //

  {
    GenericHashTableIterator<false> iter(table);
    EXPECT_FALSE(iter.HasNext());
  }

  //
  // Test: vectorized iteration shouldn't begin on an uninitialized table
  //

  {
    MemoryPool pool(nullptr);
    GenericHashTableVectorIterator<false> iter(table, &pool);
    EXPECT_FALSE(iter.HasNext());
  }

  table.SetSize(1000);

  //
  // Test: iteration shouldn't begin on an initialized, but empty table
  //

  {
    GenericHashTableIterator<false> iter(table);
    EXPECT_FALSE(iter.HasNext());
  }

  //
  // Test: vectorized iteration shouldn't begin on an initialized, but empty
  // table
  //

  {
    MemoryPool pool(nullptr);
    GenericHashTableVectorIterator<false> iter(table, &pool);
    EXPECT_FALSE(iter.HasNext());
  }
}

// NOLINTNEXTLINE
TEST_F(GenericHashTableTest, SimpleIterationTest) {
  //
  // Test: insert a bunch of entries into the hash table, ensure iteration finds
  //       them all.
  //

  using Key = u32;

  const u32 num_inserts = 500;

  std::random_device random;

  std::unordered_map<Key, TestEntry> reference;

  // The entries
  std::vector<TestEntry> entries;
  for (u32 idx = 0; idx < num_inserts; idx++) {
    TestEntry entry(random(), 20);
    entry.hash = util::Hasher::Hash(reinterpret_cast<const u8 *>(&entry.key), sizeof(entry.key));

    reference[entry.key] = entry;
    entries.emplace_back(entry);
  }

  // The table
  GenericHashTable table;
  table.SetSize(1000);

  // Insert
  for (u32 idx = 0; idx < num_inserts; idx++) {
    auto entry = &entries[idx];
    table.Insert<false>(entry, entry->hash);
  }

  auto check = [&](auto &iter) {
    u32 found_entries = 0;
    for (; iter.HasNext(); iter.Next()) {
      auto *row = reinterpret_cast<const TestEntry *>(iter.GetCurrentEntry());
      ASSERT_TRUE(row != nullptr);
      auto ref_iter = reference.find(row->key);
      ASSERT_NE(ref_iter, reference.end());
      EXPECT_EQ(ref_iter->second.key, row->key);
      EXPECT_EQ(ref_iter->second.value, row->value);
      found_entries++;
    }
    EXPECT_EQ(num_inserts, found_entries);
    EXPECT_EQ(reference.size(), found_entries);
  };

  {
    GenericHashTableIterator<false> iter(table);
    check(iter);
  }

  {
    MemoryPool pool(nullptr);
    GenericHashTableVectorIterator<false> iter(table, &pool);
    check(iter);
  }
}

// NOLINTNEXTLINE
TEST_F(GenericHashTableTest, LongChainIterationTest) {
  //
  // Test: insert a bunch of identifier entries into the hash table to form a
  //       long chain in a single bucket. Then, iteration should complete over
  //       all inserted entries.
  //

  const u32 num_inserts = 500;
  const u32 key = 10, value = 20;

  // The entries
  std::vector<TestEntry> entries;
  for (u32 idx = 0; idx < num_inserts; idx++) {
    TestEntry entry(key, value);
    entry.hash = util::Hasher::Hash(reinterpret_cast<const u8 *>(&entry.key), sizeof(entry.key));
    entries.emplace_back(entry);
  }

  // The table
  GenericHashTable table;
  table.SetSize(1000);

  // Insert
  for (u32 idx = 0; idx < num_inserts; idx++) {
    auto entry = &entries[idx];
    table.Insert<false>(entry, entry->hash);
  }

  auto check = [&](auto &iter) {
    u32 found_entries = 0;
    for (; iter.HasNext(); iter.Next()) {
      auto *row = reinterpret_cast<const TestEntry *>(iter.GetCurrentEntry());
      ASSERT_TRUE(row != nullptr);
      EXPECT_EQ(key, row->key);
      EXPECT_EQ(value, row->value);
      found_entries++;
    }
    EXPECT_EQ(num_inserts, found_entries);
  };

  {
    GenericHashTableIterator<false> iter(table);
    check(iter);
  }

  {
    MemoryPool pool(nullptr);
    GenericHashTableVectorIterator<false> iter(table, &pool);
    check(iter);
  }
}

// NOLINTNEXTLINE
TEST_F(GenericHashTableTest, DISABLED_PerfIterationTest) {
  const u32 num_inserts = 5000000;

  // The entries
  std::vector<TestEntry> entries;

  std::random_device random;
  for (u32 idx = 0; idx < num_inserts; idx++) {
    TestEntry entry(random(), 20);
    entry.hash = util::Hasher::Hash(reinterpret_cast<const u8 *>(&entry.key), sizeof(entry.key));

    entries.emplace_back(entry);
  }

  // The table
  GenericHashTable table;
  table.SetSize(num_inserts);

  // Insert
  for (u32 idx = 0; idx < num_inserts; idx++) {
    auto entry = &entries[idx];
    table.Insert<false>(entry, entry->hash);
  }

  auto check = [&](auto &iter) {
    u32 sum = 0;
    for (; iter.HasNext(); iter.Next()) {
      auto *row = reinterpret_cast<const TestEntry *>(iter.GetCurrentEntry());
      sum += row->value;
    }
    return sum;
  };

  u32 sum = 0;

  double taat_ms = 0;
#if 0
  Bench(5, [&]() {
    GenericHashTableIterator<false> iter(table);
    sum = check(iter);
  });
#endif

  EXECUTION_LOG_INFO("{}", sum);

  sum = 0;
  double vaat_ms = Bench(5, [&]() {
    MemoryPool pool(nullptr);
    GenericHashTableVectorIterator<false> iter(table, &pool);
    sum = check(iter);
  });

  EXECUTION_LOG_INFO("{}", sum);

  EXECUTION_LOG_INFO("TaaT: {:.2f}, VaaT: {:2f}", taat_ms, vaat_ms);
}

}  // namespace terrier::execution::sql::test
