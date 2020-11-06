#include <random>
#include <vector>

#include "common/hash_util.h"
#include "common/macros.h"
#include "execution/sql/concise_hash_table.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

/// This is the tuple we insert into the hash table
struct Tuple {
  uint64_t a_, b_, c_, d_;
};

/// The function to determine whether two tuples have equivalent keys
UNUSED_ATTRIBUTE static bool TupleKeyEq(UNUSED_ATTRIBUTE void *_, void *probe_tuple, void *table_tuple) {
  auto *lhs = reinterpret_cast<const Tuple *>(probe_tuple);
  auto *rhs = reinterpret_cast<const Tuple *>(table_tuple);
  return lhs->a_ == rhs->a_;
}

class ConciseHashTableTest : public TplTest {
 public:
  HashTableEntry *TestEntry(const hash_t hash) {
    entry_.hash_ = hash;
    return &entry_;
  }

 private:
  HashTableEntry entry_;
};

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, InsertTest) {
  const uint32_t num_tuples = 10;
  const uint32_t probe_length = 1;

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples, nullptr);

  // Check minimum capacity is enforced
  EXPECT_EQ(ConciseHashTable::MIN_NUM_SLOTS, table.GetCapacity());

  // 0 should go into the zero-th slot
  auto *entry_0 = TestEntry(0);
  table.Insert(entry_0);
  EXPECT_EQ(0u, entry_0->cht_slot_);

  // 1 should go into the second slot
  auto *entry_1 = TestEntry(1);
  table.Insert(entry_1);
  EXPECT_EQ(1u, entry_1->cht_slot_);

  // 2 should into the third slot
  auto *entry_2 = TestEntry(2);
  table.Insert(entry_2);
  EXPECT_EQ(2u, entry_2->cht_slot_);
}

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, InsertOverflowTest) {
  const uint32_t num_tuples = 20;
  const uint32_t probe_length = 1;

  //
  // Create a CHT with one slot group, 64 slots total
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples, nullptr);

  EXPECT_EQ(ConciseHashTable::MIN_NUM_SLOTS, table.GetCapacity());

  // 33 should go into the 33rd slot
  auto *entry_33 = TestEntry(33);
  table.Insert(entry_33);
  EXPECT_EQ(33u, entry_33->cht_slot_);

  // A second 33 should go into the 34th slot
  auto *entry_33_v2 = TestEntry(33);
  table.Insert(entry_33_v2);
  EXPECT_EQ(34u, entry_33_v2->cht_slot_);

  // A fourth 33 should overflow since probe length is 2
  auto *entry_33_v3 = TestEntry(33);
  table.Insert(entry_33_v3);
  EXPECT_EQ(34u, entry_33_v3->cht_slot_);

  // 34 should go into the 35th slot (since the 34th is occupied by 33 v2)
  auto *entry_34 = TestEntry(34);
  table.Insert(entry_34);
  EXPECT_EQ(35u, entry_34->cht_slot_);
}

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, MultiGroupInsertTest) {
  const uint32_t num_tuples = 100;
  const uint32_t probe_length = 1;

  //
  // Create a CHT with four slot-groups, each having 64 slots
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples, nullptr);

  EXPECT_EQ(ConciseHashTable::MIN_NUM_SLOTS, table.GetCapacity());

  // 33 goes in the first group, in the 33rd slot
  auto *entry_33 = TestEntry(33);
  table.Insert(entry_33);
  EXPECT_EQ(33u, entry_33->cht_slot_);

  // 97 (64+33) goes in the second group in the 33rd group bit, but the 97th
  // overall slot
  auto *entry_97 = TestEntry(97);
  table.Insert(entry_97);
  EXPECT_EQ(97u, entry_97->cht_slot_);

  // 161 (64+64+33) goes in the third group in the 33rd group bit, but the 130th
  // overall slot
  auto *entry_161 = TestEntry(161);
  table.Insert(entry_161);
  EXPECT_EQ(161u, entry_161->cht_slot_);

  // 225 (64+64+64+33) goes in the fourth (and last) group, in the 33rd group
  // bit, but the 225th overall slot
  auto *entry_225 = TestEntry(225);
  table.Insert(entry_225);
  EXPECT_EQ(225u, entry_225->cht_slot_);

  // Try inserting something into the 33rd slot but with a wrap around. This
  // should overflow onto the 34th slot since the 33rd is occupied by the first
  // '33' insertion.
  auto *entry_33_v2 = TestEntry(ConciseHashTable::MIN_NUM_SLOTS + 33);
  table.Insert(entry_33_v2);
  EXPECT_EQ(34u, entry_33_v2->cht_slot_);

  // Inserting the previous hash again should overflow. The first '33' went into
  // the 33rd slot; the second went into the 34th slot (since probe limit is 1)
  auto *entry_33_v3 = TestEntry(ConciseHashTable::MIN_NUM_SLOTS + 33);
  table.Insert(entry_33_v3);
  EXPECT_EQ(34u, entry_33_v3->cht_slot_);
}

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, CornerCaseTest) {
  const uint32_t num_tuples = 20;
  const uint32_t probe_length = 4;

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples, nullptr);

  EXPECT_EQ(ConciseHashTable::MIN_NUM_SLOTS, table.GetCapacity());

  // 63 should go into the 63rd slot
  auto *entry_63 = TestEntry(63);
  table.Insert(entry_63);
  EXPECT_EQ(63u, entry_63->cht_slot_);

  // A second 63 should overflow even though the probe length is 4. Probing
  // doesn't cross slot group boundaries.
  auto *entry_63_v2 = TestEntry(63);
  table.Insert(entry_63_v2);
  EXPECT_EQ(63u, entry_63_v2->cht_slot_);

  // 62 should go into the 62nd slot
  auto *entry_62 = TestEntry(62);
  table.Insert(entry_62);
  EXPECT_EQ(62u, entry_62->cht_slot_);

  // A second 62 should overflow onto the 63rd slot since probing doesn't cross
  // slot group boundaries
  auto *entry_62_v2 = TestEntry(62);
  table.Insert(entry_62_v2);
  EXPECT_EQ(63u, entry_62_v2->cht_slot_);
}

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, BuildTest) {
  const uint32_t num_tuples = 20;
  const uint32_t probe_length = 2;

  //
  // Table composed of single group with 64 bits
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples, nullptr);

  EXPECT_EQ(ConciseHashTable::MIN_NUM_SLOTS, table.GetCapacity());

  std::vector<ConciseHashTableSlot> inserted;

  for (uint32_t i = 1; i < 64; i += 2) {
    auto *entry = TestEntry(i);
    table.Insert(entry);
    inserted.push_back(entry->cht_slot_);
  }

  table.Build();

  for (uint32_t i = 0; i < inserted.size(); i++) {
    EXPECT_EQ(i, table.NumFilledSlotsBefore(inserted[i]));
  }
}

// NOLINTNEXTLINE
TEST_F(ConciseHashTableTest, MultiGroupBuildTest) {
  const uint32_t num_tuples = 40;
  const uint32_t probe_length = 2;

  //
  // Table composed of two groups totaling 128 bits
  //

  ConciseHashTable table(probe_length);
  table.SetSize(num_tuples, nullptr);

  EXPECT_EQ(ConciseHashTable::MIN_NUM_SLOTS, table.GetCapacity());

  std::vector<ConciseHashTableSlot> inserted;

  for (uint32_t i = 1; i < 128; i += 2) {
    auto *entry = TestEntry(i);
    table.Insert(entry);
    inserted.push_back(entry->cht_slot_);
  }

  table.Build();

  for (uint32_t i = 0; i < inserted.size(); i++) {
    EXPECT_EQ(i, table.NumFilledSlotsBefore(inserted[i]));
  }
}

}  // namespace noisepage::execution::sql::test
