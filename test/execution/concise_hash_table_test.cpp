#include <random>
#include <vector>

#include "execution/tpl_test.h"

#include "execution/sql/concise_hash_table.h"
#include "execution/util/hash.h"
#include "execution/util/macros.h"

namespace terrier::execution::sql::test {

/// This is the tuple we insert into the hash table
struct Tuple {
  u64 a, b, c, d;
};

/// The function to determine whether two tuples have equivalent keys
UNUSED static inline bool TupleKeyEq(UNUSED void *_, void *probe_tuple, void *table_tuple) {
  auto *lhs = reinterpret_cast<const Tuple *>(probe_tuple);
  auto *rhs = reinterpret_cast<const Tuple *>(table_tuple);
  return lhs->a == rhs->a;
}

class ConciseHashTableTest : public TplTest {
 public:
  HashTableEntry *TestEntry(const hash_t hash) {
    entry_.hash = hash;
    return &entry_;
  }

 private:
  HashTableEntry entry_;
};

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, InsertTest) {
  const u32 num_tuples = 10;
  const u32 probe_length = 1;

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  // Check minimum capacity is enforced
  EXPECT_EQ(ConciseHashTable::kMinNumSlots, table.capacity());

  // 0 should go into the zero-th slot
  auto *entry_0 = TestEntry(0);
  table.Insert(entry_0, entry_0->hash);
  EXPECT_EQ(0u, entry_0->cht_slot);

  // 1 should go into the second slot
  auto *entry_1 = TestEntry(1);
  table.Insert(entry_1, entry_1->hash);
  EXPECT_EQ(1u, entry_1->cht_slot);

  // 2 should into the third slot
  auto *entry_2 = TestEntry(2);
  table.Insert(entry_2, entry_2->hash);
  EXPECT_EQ(2u, entry_2->cht_slot);
}

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, InsertOverflowTest) {
  const u32 num_tuples = 20;
  const u32 probe_length = 1;

  //
  // Create a CHT with one slot group, 64 slots total
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(ConciseHashTable::kMinNumSlots, table.capacity());

  // 33 should go into the 33rd slot
  auto *entry_33 = TestEntry(33);
  table.Insert(entry_33, entry_33->hash);
  EXPECT_EQ(33u, entry_33->cht_slot);

  // A second 33 should go into the 34th slot
  auto *entry_33_v2 = TestEntry(33);
  table.Insert(entry_33_v2, entry_33_v2->hash);
  EXPECT_EQ(34u, entry_33_v2->cht_slot);

  // A fourth 33 should overflow since probe length is 2
  auto *entry_33_v3 = TestEntry(33);
  table.Insert(entry_33_v3, entry_33_v3->hash);
  EXPECT_EQ(34u, entry_33_v3->cht_slot);

  // 34 should go into the 35th slot (since the 34th is occupied by 33 v2)
  auto *entry_34 = TestEntry(34);
  table.Insert(entry_34, entry_34->hash);
  EXPECT_EQ(35u, entry_34->cht_slot);
}

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, MultiGroupInsertTest) {
  const u32 num_tuples = 100;
  const u32 probe_length = 1;

  //
  // Create a CHT with four slot-groups, each having 64 slots
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(ConciseHashTable::kMinNumSlots, table.capacity());

  // 33 goes in the first group, in the 33rd slot
  auto *entry_33 = TestEntry(33);
  table.Insert(entry_33, entry_33->hash);
  EXPECT_EQ(33u, entry_33->cht_slot);

  // 97 (64+33) goes in the second group in the 33rd group bit, but the 97th
  // overall slot
  auto *entry_97 = TestEntry(97);
  table.Insert(entry_97, entry_97->hash);
  EXPECT_EQ(97u, entry_97->cht_slot);

  // 161 (64+64+33) goes in the third group in the 33rd group bit, but the 130th
  // overall slot
  auto *entry_161 = TestEntry(161);
  table.Insert(entry_161, entry_161->hash);
  EXPECT_EQ(161u, entry_161->cht_slot);

  // 225 (64+64+64+33) goes in the fourth (and last) group, in the 33rd group
  // bit, but the 225th overall slot
  auto *entry_225 = TestEntry(225);
  table.Insert(entry_225, entry_225->hash);
  EXPECT_EQ(225u, entry_225->cht_slot);

  // Try inserting something into the 33rd slot but with a wrap around. This
  // should overflow onto the 34th slot since the 33rd is occupied by the first
  // '33' insertion.
  auto *entry_33_v2 = TestEntry(ConciseHashTable::kMinNumSlots + 33);
  table.Insert(entry_33_v2, entry_33_v2->hash);
  EXPECT_EQ(34u, entry_33_v2->cht_slot);

  // Inserting the previous hash again should overflow. The first '33' went into
  // the 33rd slot; the second went into the 34th slot (since probe limit is 1)
  auto *entry_33_v3 = TestEntry(ConciseHashTable::kMinNumSlots + 33);
  table.Insert(entry_33_v3, entry_33_v3->hash);
  EXPECT_EQ(34u, entry_33_v3->cht_slot);
}

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, CornerCaseTest) {
  const u32 num_tuples = 20;
  const u32 probe_length = 4;

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(ConciseHashTable::kMinNumSlots, table.capacity());

  // 63 should go into the 63rd slot
  auto *entry_63 = TestEntry(63);
  table.Insert(entry_63, entry_63->hash);
  EXPECT_EQ(63u, entry_63->cht_slot);

  // A second 63 should overflow even though the probe length is 4. Probing
  // doesn't cross slot group boundaries.
  auto *entry_63_v2 = TestEntry(63);
  table.Insert(entry_63_v2, entry_63_v2->hash);
  EXPECT_EQ(63u, entry_63_v2->cht_slot);

  // 62 should go into the 62nd slot
  auto *entry_62 = TestEntry(62);
  table.Insert(entry_62, entry_62->hash);
  EXPECT_EQ(62u, entry_62->cht_slot);

  // A second 62 should overflow onto the 63rd slot since probing doesn't cross
  // slot group boundaries
  auto *entry_62_v2 = TestEntry(62);
  table.Insert(entry_62_v2, entry_62_v2->hash);
  EXPECT_EQ(63u, entry_62_v2->cht_slot);
}

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, BuildTest) {
  const u32 num_tuples = 20;
  const u32 probe_length = 2;

  //
  // Table composed of single group with 64 bits
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(ConciseHashTable::kMinNumSlots, table.capacity());

  std::vector<ConciseHashTableSlot> inserted;

  for (u32 i = 1; i < 64; i += 2) {
    auto *entry = TestEntry(i);
    table.Insert(entry, entry->hash);
    inserted.push_back(entry->cht_slot);
  }

  table.Build();

  for (u32 i = 0; i < inserted.size(); i++) {
    EXPECT_EQ(i, table.NumFilledSlotsBefore(inserted[i]));
  }
}

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, MultiGroupBuildTest) {
  const u32 num_tuples = 40;
  const u32 probe_length = 2;

  //
  // Table composed of two groups totaling 128 bits
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples);

  EXPECT_EQ(ConciseHashTable::kMinNumSlots, table.capacity());

  std::vector<ConciseHashTableSlot> inserted;

  for (u32 i = 1; i < 128; i += 2) {
    auto *entry = TestEntry(i);
    table.Insert(entry, entry->hash);
    inserted.push_back(entry->cht_slot);
  }

  table.Build();

  for (u32 i = 0; i < inserted.size(); i++) {
    EXPECT_EQ(i, table.NumFilledSlotsBefore(inserted[i]));
  }
}

}  // namespace terrier::execution::sql::test
