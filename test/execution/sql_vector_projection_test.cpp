#include <memory>
#include <vector>

#include "execution/sql/tuple_id_list.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/sql/vector_projection.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class VectorProjectionTest : public TplTest {};

class VectorProjectionDeathTest : public VectorProjectionTest {};

// NOLINTNEXTLINE
TEST_F(VectorProjectionTest, Empty) {
  VectorProjection vector_projection;

  EXPECT_EQ(0u, vector_projection.GetColumnCount());
  EXPECT_EQ(0u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(0u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
  vector_projection.CheckIntegrity();
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionTest, InitializeEmpty) {
  VectorProjection vector_projection;
  vector_projection.InitializeEmpty({TypeId::SmallInt, TypeId::Double});

  EXPECT_EQ(2u, vector_projection.GetColumnCount());
  EXPECT_EQ(TypeId::SmallInt, vector_projection.GetColumnType(0));
  EXPECT_EQ(TypeId::Double, vector_projection.GetColumnType(1));
  EXPECT_EQ(0u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(0u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());

  for (uint32_t i = 0; i < vector_projection.GetColumnCount(); i++) {
    EXPECT_EQ(0u, vector_projection.GetColumn(i)->GetCount());
    EXPECT_EQ(nullptr, vector_projection.GetColumn(i)->GetFilteredTupleIdList());
  }

  vector_projection.CheckIntegrity();
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionTest, Initialize) {
  VectorProjection vector_projection;
  vector_projection.Initialize({TypeId::Float, TypeId::Integer, TypeId::Date});

  EXPECT_EQ(3u, vector_projection.GetColumnCount());
  EXPECT_EQ(TypeId::Float, vector_projection.GetColumnType(0));
  EXPECT_EQ(TypeId::Integer, vector_projection.GetColumnType(1));
  EXPECT_EQ(TypeId::Date, vector_projection.GetColumnType(2));
  EXPECT_EQ(0u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(0u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());

  for (uint32_t i = 0; i < vector_projection.GetColumnCount(); i++) {
    EXPECT_EQ(0u, vector_projection.GetColumn(i)->GetCount());
    EXPECT_EQ(nullptr, vector_projection.GetColumn(i)->GetFilteredTupleIdList());
  }

  vector_projection.CheckIntegrity();
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionTest, Reinitialize) {
  VectorProjection vector_projection;

  // Create vector projection with [smallint, double]
  vector_projection.Initialize({TypeId::SmallInt, TypeId::Double});
  EXPECT_EQ(2u, vector_projection.GetColumnCount());
  EXPECT_EQ(TypeId::SmallInt, vector_projection.GetColumnType(0));
  EXPECT_EQ(TypeId::Double, vector_projection.GetColumnType(1));
  EXPECT_EQ(0u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(0u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
  vector_projection.CheckIntegrity();

  // Reinitialize with different schema: [integer, bigint, float]
  vector_projection.InitializeEmpty({TypeId::Integer, TypeId::BigInt, TypeId::Float});
  EXPECT_EQ(3u, vector_projection.GetColumnCount());
  EXPECT_EQ(TypeId::Integer, vector_projection.GetColumnType(0));
  EXPECT_EQ(TypeId::BigInt, vector_projection.GetColumnType(1));
  EXPECT_EQ(TypeId::Float, vector_projection.GetColumnType(2));
  EXPECT_EQ(0u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(0u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
  vector_projection.CheckIntegrity();
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionTest, Selection) {
  VectorProjection vector_projection;
  vector_projection.Initialize({TypeId::BigInt, TypeId::Double});
  vector_projection.Reset(20);

  // a = [i for i in range(0, 20, 3)] = [0, 3, 6, 9, 12, ...]
  // b = [123.45 for i in range(20)] = [123.45, 123.45, 123.45, ...]
  VectorOps::Generate(vector_projection.GetColumn(0), 0, 3);
  VectorOps::Fill(vector_projection.GetColumn(1), GenericValue::CreateDouble(123.45));

  EXPECT_EQ(20u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(20u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
  EXPECT_FLOAT_EQ(1.0, vector_projection.ComputeSelectivity());

  // Try to filter once
  {
    auto tid_list = TupleIdList(vector_projection.GetTotalTupleCount());
    tid_list = {2, 3, 5, 7, 11, 13, 17, 19};
    vector_projection.SetFilteredSelections(tid_list);

    EXPECT_EQ(20u, vector_projection.GetTotalTupleCount());
    EXPECT_EQ(tid_list.GetTupleCount(), vector_projection.GetSelectedTupleCount());
    EXPECT_NE(nullptr, vector_projection.GetFilteredTupleIdList());
    EXPECT_FLOAT_EQ(tid_list.ComputeSelectivity(), vector_projection.ComputeSelectivity());
    for (uint32_t i = 0; i < tid_list.GetTupleCount(); i++) {
      auto tid = tid_list[i];
      EXPECT_EQ(GenericValue::CreateBigInt(tid * 3), vector_projection.GetColumn(0)->GetValue(i));
    }
  }

  // Filter again with a different selection
  {
    auto tid_list = TupleIdList(vector_projection.GetTotalTupleCount());
    vector_projection.SetFilteredSelections(tid_list);

    EXPECT_EQ(20u, vector_projection.GetTotalTupleCount());
    EXPECT_EQ(tid_list.GetTupleCount(), vector_projection.GetSelectedTupleCount());
    EXPECT_NE(nullptr, vector_projection.GetFilteredTupleIdList());
    EXPECT_FLOAT_EQ(tid_list.ComputeSelectivity(), vector_projection.ComputeSelectivity());
  }

  vector_projection.CheckIntegrity();
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionDeathTest, InvalidFilter) {
#ifndef NDEBUG
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";

  VectorProjection vector_projection;
  vector_projection.Initialize({TypeId::BigInt, TypeId::Double});
  vector_projection.Reset(20);

  // Filtered TID list is too small
  {
    auto tid_list = TupleIdList(vector_projection.GetTotalTupleCount() - 5);
    ASSERT_DEATH(vector_projection.SetFilteredSelections(tid_list), "capacity");
  }

  // Filtered TID list is too large
  {
    auto tid_list = TupleIdList(vector_projection.GetTotalTupleCount() + 5);
    ASSERT_DEATH(vector_projection.SetFilteredSelections(tid_list), "capacity");
  }
#endif
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionDeathTest, InvalidShape) {
#ifndef NDEBUG
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";

  VectorProjection vector_projection;
  vector_projection.Initialize({TypeId::BigInt, TypeId::Double});
  vector_projection.Reset(20);

  // Vectors have different sizes
  {
    // Use second vector because first is used to determine projection size, in which case the
    // TID list capacity assertion will trip, not the all-vectors-have-same-size.
    vector_projection.GetColumn(1)->Resize(2);
    ASSERT_DEATH(vector_projection.CheckIntegrity(), "Vector size");
  }
#endif
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionTest, Reset) {
  VectorProjection vector_projection;
  vector_projection.Initialize({TypeId::TinyInt});
  vector_projection.Reset(20);

  auto tid_list = TupleIdList(vector_projection.GetTotalTupleCount());
  tid_list = {7, 11, 13};
  vector_projection.SetFilteredSelections(tid_list);

  EXPECT_FALSE(vector_projection.IsEmpty());
  EXPECT_TRUE(vector_projection.IsFiltered());
  EXPECT_EQ(tid_list.GetTupleCount(), vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(20u, vector_projection.GetTotalTupleCount());
  EXPECT_NE(nullptr, vector_projection.GetFilteredTupleIdList());
  EXPECT_FLOAT_EQ(tid_list.ComputeSelectivity(), vector_projection.ComputeSelectivity());
  vector_projection.CheckIntegrity();

  vector_projection.Reset(0);

  EXPECT_TRUE(vector_projection.IsEmpty());
  EXPECT_FALSE(vector_projection.IsFiltered());
  EXPECT_EQ(0u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(0u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
  EXPECT_FLOAT_EQ(0.0, vector_projection.ComputeSelectivity());

  vector_projection.Reset(40);

  EXPECT_FALSE(vector_projection.IsEmpty());
  EXPECT_FALSE(vector_projection.IsFiltered());
  EXPECT_EQ(40u, vector_projection.GetTotalTupleCount());
  EXPECT_EQ(40u, vector_projection.GetSelectedTupleCount());
  EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
  EXPECT_FLOAT_EQ(1.0, vector_projection.ComputeSelectivity());

  vector_projection.CheckIntegrity();
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionTest, Pack) {
  VectorProjection vector_projection;
  vector_projection.Initialize({TypeId::TinyInt, TypeId::SmallInt, TypeId::Float});
  vector_projection.Reset(20);

  VectorOps::Fill(vector_projection.GetColumn(0), GenericValue::CreateTinyInt(4));
  VectorOps::Fill(vector_projection.GetColumn(1), GenericValue::CreateSmallInt(8));
  VectorOps::Fill(vector_projection.GetColumn(2), GenericValue::CreateFloat(16.0));

  // Try packing an unfiltered projection
  {
    vector_projection.Pack();

    EXPECT_FALSE(vector_projection.IsEmpty());
    EXPECT_FALSE(vector_projection.IsFiltered());
    EXPECT_EQ(20u, vector_projection.GetSelectedTupleCount());
    EXPECT_EQ(20u, vector_projection.GetTotalTupleCount());
    EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
    vector_projection.CheckIntegrity();
  }

  // Try packing with a filter
  {
    auto tid_list = TupleIdList(vector_projection.GetTotalTupleCount());
    tid_list = {7, 11, 13};
    vector_projection.SetFilteredSelections(tid_list);

    vector_projection.Pack();

    EXPECT_FALSE(vector_projection.IsEmpty());
    EXPECT_FALSE(vector_projection.IsFiltered());
    EXPECT_EQ(tid_list.GetTupleCount(), vector_projection.GetSelectedTupleCount());
    EXPECT_EQ(tid_list.GetTupleCount(), vector_projection.GetTotalTupleCount());
    EXPECT_EQ(nullptr, vector_projection.GetFilteredTupleIdList());
    EXPECT_FLOAT_EQ(1.0, vector_projection.ComputeSelectivity());
    vector_projection.CheckIntegrity();

    for (uint32_t i = 0; i < vector_projection.GetColumnCount(); i++) {
      auto *vec = vector_projection.GetColumn(i);
      EXPECT_EQ(tid_list.GetTupleCount(), vec->GetCount());
    }
  }
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionTest, ProjectColumns) {
  // Create a vector projection with [tiny_int, small_int, integer] columns.
  // col0 = [0,1,2,3,4,5,6,7,8,9]
  // col1 = [10,11,12,13,14,15,16,17,18,19]
  // col2 = [20,21,22,23,24,25,26,27,28,29]
  VectorProjection vector_projection;
  vector_projection.Initialize({TypeId::TinyInt, TypeId::SmallInt, TypeId::Integer});
  vector_projection.Reset(10);
  VectorOps::Generate(vector_projection.GetColumn(0), 0, 1);
  VectorOps::Generate(vector_projection.GetColumn(1), 10, 1);
  VectorOps::Generate(vector_projection.GetColumn(2), 20, 1);

  // Apply simple filter.
  TupleIdList tid_list(vector_projection.GetTotalTupleCount());
  tid_list = {0, 1, 2, 3, 9};
  vector_projection.SetFilteredSelections(tid_list);

  // Project only the first small integer column.
  VectorProjection col_1_projection;
  vector_projection.ProjectColumns({1}, &col_1_projection);
  EXPECT_EQ(1, col_1_projection.GetColumnCount());
  EXPECT_EQ(TypeId::SmallInt, col_1_projection.GetColumn(0)->GetTypeId());
  EXPECT_FALSE(col_1_projection.IsEmpty());
  EXPECT_TRUE(col_1_projection.IsFiltered());
  EXPECT_EQ(vector_projection.GetTotalTupleCount(), col_1_projection.GetTotalTupleCount());
  EXPECT_EQ(vector_projection.GetSelectedTupleCount(), col_1_projection.GetSelectedTupleCount());
  // Check vector contents.
  auto col1 = col_1_projection.GetColumn(0);
  EXPECT_EQ(GenericValue::CreateSmallInt(10), col1->GetValue(0));
  EXPECT_EQ(GenericValue::CreateSmallInt(11), col1->GetValue(1));
  EXPECT_EQ(GenericValue::CreateSmallInt(12), col1->GetValue(2));
  EXPECT_EQ(GenericValue::CreateSmallInt(13), col1->GetValue(3));
  EXPECT_EQ(GenericValue::CreateSmallInt(19), col1->GetValue(4));
  col_1_projection.CheckIntegrity();

  // Try on an unfiltered vector projection.
  vector_projection.Reset(10);
  VectorProjection col_0_2_projection;
  vector_projection.ProjectColumns({0, 2}, &col_0_2_projection);
  EXPECT_EQ(2, col_0_2_projection.GetColumnCount());
  EXPECT_EQ(TypeId::TinyInt, col_0_2_projection.GetColumn(0)->GetTypeId());
  EXPECT_EQ(TypeId::Integer, col_0_2_projection.GetColumn(1)->GetTypeId());
  EXPECT_FALSE(col_0_2_projection.IsEmpty());
  EXPECT_FALSE(col_0_2_projection.IsFiltered());
  EXPECT_EQ(vector_projection.GetTotalTupleCount(), col_0_2_projection.GetTotalTupleCount());
  EXPECT_EQ(vector_projection.GetSelectedTupleCount(), col_0_2_projection.GetSelectedTupleCount());
  col_0_2_projection.CheckIntegrity();
}

}  // namespace noisepage::execution::sql::test
