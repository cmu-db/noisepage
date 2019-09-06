#include <random>
#include <unordered_map>
#include <vector>

#include "execution/tpl_test.h"

#include "execution/sql/generic_hash_table.h"
#include "execution/util/hash.h"

namespace terrier::execution::sql::test {

class GenericHashTableTest : public TplTest {};

struct TestEntry : public HashTableEntry {
  uint32_t key_{0}, value_{0};
  TestEntry() : HashTableEntry() {}
  TestEntry(uint32_t key, uint32_t value) : HashTableEntry(), key_(key), value_(value) {}
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

  using Key = uint32_t;

  const uint32_t num_inserts = 500;

  std::random_device random;

  std::unordered_map<Key, TestEntry> reference;

  // The entries
  std::vector<TestEntry> entries;
  for (uint32_t idx = 0; idx < num_inserts; idx++) {
    TestEntry entry(random(), 20);
    entry.hash_ = util::Hasher::Hash(reinterpret_cast<const uint8_t *>(&entry.key_), sizeof(entry.key_));

    reference[entry.key_] = entry;
    entries.emplace_back(entry);
  }

  // The table
  GenericHashTable table;
  table.SetSize(1000);

  // Insert
  for (uint32_t idx = 0; idx < num_inserts; idx++) {
    auto entry = &entries[idx];
    table.Insert<false>(entry, entry->hash_);
  }

  auto check = [&](auto &iter) {
    uint32_t found_entries = 0;
    for (; iter.HasNext(); iter.Next()) {
      auto *row = reinterpret_cast<const TestEntry *>(iter.GetCurrentEntry());
      ASSERT_TRUE(row != nullptr);
      auto ref_iter = reference.find(row->key_);
      ASSERT_NE(ref_iter, reference.end());
      EXPECT_EQ(ref_iter->second.key_, row->key_);
      EXPECT_EQ(ref_iter->second.value_, row->value_);
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

  const uint32_t num_inserts = 500;
  const uint32_t key = 10, value = 20;

  // The entries
  std::vector<TestEntry> entries;
  for (uint32_t idx = 0; idx < num_inserts; idx++) {
    TestEntry entry(key, value);
    entry.hash_ = util::Hasher::Hash(reinterpret_cast<const uint8_t *>(&entry.key_), sizeof(entry.key_));
    entries.emplace_back(entry);
  }

  // The table
  GenericHashTable table;
  table.SetSize(1000);

  // Insert
  for (uint32_t idx = 0; idx < num_inserts; idx++) {
    auto entry = &entries[idx];
    table.Insert<false>(entry, entry->hash_);
  }

  auto check = [&](auto &iter) {
    uint32_t found_entries = 0;
    for (; iter.HasNext(); iter.Next()) {
      auto *row = reinterpret_cast<const TestEntry *>(iter.GetCurrentEntry());
      ASSERT_TRUE(row != nullptr);
      EXPECT_EQ(key, row->key_);
      EXPECT_EQ(value, row->value_);
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

}  // namespace terrier::execution::sql::test
