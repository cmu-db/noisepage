#include "execution/sql/tuple_id_list.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class TupleIdListTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, Add) {
  constexpr uint32_t num_tids = 10;

  TupleIdList list(num_tids);

  // Initially, no TIDs
  for (uint32_t tid = 0; tid < num_tids; tid++) {
    EXPECT_FALSE(list.Contains(tid));
  }

  list.Add(3);
  EXPECT_TRUE(list.Contains(3));
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, AddAll) {
  constexpr uint32_t num_tids = 10;

  TupleIdList list(num_tids);

  list.AddAll();
  for (uint32_t tid = 0; tid < num_tids; tid++) {
    EXPECT_TRUE(list.Contains(tid));
  }
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, AddRange) {
  constexpr uint32_t num_tids = 10;

  TupleIdList list(num_tids);
  list.AddRange(4, 6);
  for (uint32_t tid = 0; tid < num_tids; tid++) {
    EXPECT_EQ(4 <= tid && tid < 6, list.Contains(tid));
  }
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, Enable) {
  constexpr uint32_t num_tids = 10;

  TupleIdList list(num_tids);
  list.Enable(4, false);
  EXPECT_FALSE(list.Contains(4));

  list.Enable(4, true);
  EXPECT_TRUE(list.Contains(4));

  list.Enable(4, false);
  EXPECT_FALSE(list.Contains(4));
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, Remove) {
  constexpr uint32_t num_tids = 10;

  TupleIdList list(num_tids);
  list.Add(4);
  list.Add(9);
  EXPECT_TRUE(list.Contains(4));
  EXPECT_TRUE(list.Contains(9));

  list.Remove(4);
  EXPECT_FALSE(list.Contains(4));
  EXPECT_TRUE(list.Contains(9));

  // Try removing a TID that doesn't exist
  list.Remove(7);
  list.Remove(9);
  EXPECT_FALSE(list.Contains(4));
  EXPECT_FALSE(list.Contains(7));
  EXPECT_FALSE(list.Contains(9));
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, Clear) {
  constexpr uint32_t num_tids = 10;

  TupleIdList list(num_tids);
  list.AddAll();
  list.Clear();
  for (uint32_t tid = 0; tid < num_tids; tid++) {
    EXPECT_FALSE(list.Contains(tid));
  }
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, IsFull) {
  TupleIdList list(10);
  EXPECT_FALSE(list.IsFull());

  list.Add(9);
  EXPECT_FALSE(list.IsFull());

  list.AddRange(0, 9);
  EXPECT_TRUE(list.IsFull());
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, Empty) {
  TupleIdList list(10);
  EXPECT_TRUE(list.IsEmpty());

  list.Add(3);
  EXPECT_FALSE(list.IsEmpty());

  list.Clear();
  EXPECT_TRUE(list.IsEmpty());

  list.Add(4);
  EXPECT_FALSE(list.IsEmpty());
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, Intersection) {
  TupleIdList list1(10), list2(10);

  // list1 = [1, 3, 4, 7, 8, 9];
  // list2 = [0, 2, 4, 8, 9];

  list1.Add(1);
  list1.AddRange(3, 5);
  list1.AddRange(7, 10);

  list2.Add(0);
  list2.Add(2);
  list2.Add(4);
  list2.AddRange(8, 10);

  // list1 = list1 ∩ list2 = [4, 8, 9]
  list1.IntersectWith(list2);

  for (uint32_t tid = 0; tid < 10; tid++) {
    EXPECT_EQ(tid == 4 || tid == 8 || tid == 9, list1.Contains(tid));
  }
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, Union) {
  TupleIdList list1(10), list2(10);

  // list1 = [1, 3, 4, 7, 8, 9];
  // list2 = [0, 2, 4, 8, 9];

  list1.Add(1);
  list1.AddRange(3, 5);
  list1.AddRange(7, 10);

  list2.Add(0);
  list2.Add(2);
  list2.Add(4);
  list2.AddRange(8, 10);

  // list1 = list1 ∪ list2 = [0, 1, 2, 3, 4, 7, 8, 9]
  list1.UnionWith(list2);
  for (uint32_t tid = 0; tid < 10; tid++) {
    EXPECT_EQ(!(tid == 5 || tid == 6), list1.Contains(tid));
  }
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, UnsetFrom) {
  TupleIdList list1(10), list2(10);

  // list1 = [1, 3, 4, 7, 8, 9];
  // list2 = [0, 2, 4, 8, 9];

  list1.Add(1);
  list1.AddRange(3, 5);
  list1.AddRange(7, 10);

  list2.Add(0);
  list2.Add(2);
  list2.Add(4);
  list2.AddRange(8, 10);

  // list1 = list1 - list2 = [1, 3, 7]
  list1.UnsetFrom(list2);
  for (uint32_t tid = 0; tid < 10; tid++) {
    EXPECT_EQ(tid == 1 || tid == 3 || tid == 7, list1.Contains(tid));
  }
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, Selectivity) {
  TupleIdList list(10);

  EXPECT_FLOAT_EQ(0.0, list.ComputeSelectivity());

  list.Add(0);
  EXPECT_FLOAT_EQ(0.1, list.ComputeSelectivity());

  list.AddRange(0, 5);
  EXPECT_FLOAT_EQ(0.5, list.ComputeSelectivity());

  list.AddRange(0, 10);
  EXPECT_FLOAT_EQ(1.0, list.ComputeSelectivity());
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, Iterate) {
  TupleIdList list(10);
  for (uint32_t i = 0; i < list.GetCapacity(); i++) {
    list.Enable(i, i % 2 == 0);
  }
  list.ForEach([](auto tid) { EXPECT_EQ(tid % 2, 0); });
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, IterateRange) {
  // list = []
  TupleIdList list(10);
  for (uint32_t i = 0; i < list.GetCapacity(); i++) {
    list.Enable(i, i % 2 == 0);
  }

  // list = [0, 2, 4, 6, 8]
  // let's iterate just [2, 4, 6]

  std::vector<uint32_t> output;
  for (auto iter = list.Begin() + 1, end = iter + 3; iter != end; ++iter) {
    output.push_back(*iter);
  }
  EXPECT_EQ(3, output.size());
  EXPECT_EQ(2, output[0]);
  EXPECT_EQ(4, output[1]);
  EXPECT_EQ(6, output[2]);
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, ConvertToSelectionVector) {
  uint16_t sel[common::Constants::K_DEFAULT_VECTOR_SIZE];

  TupleIdList list(10);
  list.Add(0);
  list.AddRange(3, 7);

  uint32_t n = list.ToSelectionVector(sel);
  EXPECT_EQ(5u, n);
  EXPECT_EQ(0u, sel[0]);
  EXPECT_EQ(3u, sel[1]);
  EXPECT_EQ(4u, sel[2]);
  EXPECT_EQ(5u, sel[3]);
  EXPECT_EQ(6u, sel[4]);
}

// NOLINTNEXTLINE
TEST_F(TupleIdListTest, BuildFromSelectionVector) {
  TupleIdList list(10);

  // Empty selection vector
  {
    sel_t sel[] = {};
    list.BuildFromSelectionVector(sel, sizeof(sel) / sizeof(sel[0]));
    EXPECT_TRUE(list.IsEmpty());
  }

  list.Clear();

  // Simple
  {
    sel_t sel[] = {0, 4, 9};
    list.BuildFromSelectionVector(sel, sizeof(sel) / sizeof(sel[0]));
    EXPECT_EQ(3u, list.GetTupleCount());
    EXPECT_TRUE(list.Contains(0));
    EXPECT_TRUE(list.Contains(4));
    EXPECT_TRUE(list.Contains(9));
  }

  list.Clear();

  // All
  {
    sel_t sel[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    list.BuildFromSelectionVector(sel, sizeof(sel) / sizeof(sel[0]));
    EXPECT_TRUE(list.IsFull());
  }
}

}  // namespace noisepage::execution::sql::test
