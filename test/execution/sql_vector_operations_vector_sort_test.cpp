#include <vector>

#include "common/error/exception.h"
#include "execution/sql/constant_vector.h"
#include "execution/sql/tuple_id_list.h"
#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/sql_test.h"

namespace noisepage::execution::sql::test {

class VectorSortTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(VectorSortTest, NoNullsNoFilter) {
  // All elements selected
  auto vec = MakeIntegerVector({1, 10, 2, 9, 3, 8, 4, 7, 5, 6},
                               {false, false, false, false, false, false, false, false, false, false});

  sel_t sorted[common::Constants::K_DEFAULT_VECTOR_SIZE];
  VectorOps::Sort(*vec, sorted);

  auto *data = reinterpret_cast<const int32_t *>(vec->GetData());
  EXPECT_TRUE(
      std::is_sorted(sorted, sorted + vec->GetSize(), [&](auto idx1, auto idx2) { return data[idx1] <= data[idx2]; }));
}

// NOLINTNEXTLINE
TEST_F(VectorSortTest, NoNullsWithFilter) {
  // Simple vector
  auto vec = MakeIntegerVector({1, 10, 2, 9, 3, 8, 4, 7, 5, 6},
                               {false, false, false, false, false, false, false, false, false, false});

  // Filter: vec = [10,2,8]
  TupleIdList selections(vec->GetSize());
  selections = {1, 2, 5};
  vec->SetFilteredTupleIdList(&selections, selections.GetTupleCount());

  // Sorted: vec = [2,8,10]
  sel_t sorted[common::Constants::K_DEFAULT_VECTOR_SIZE];
  VectorOps::Sort(*vec, sorted);

  auto *data = reinterpret_cast<const int32_t *>(vec->GetData());
  EXPECT_EQ(3u, vec->GetCount());
  EXPECT_EQ(2u, data[sorted[0]]);
  EXPECT_EQ(8u, data[sorted[1]]);
  EXPECT_EQ(10u, data[sorted[2]]);
}

// NOLINTNEXTLINE
TEST_F(VectorSortTest, NullsNoFilter) {
  // vec = [NULL,10,2,9,NULL,8,NULL,NULL,NULL,6]
  auto vec = MakeIntegerVector({1, 10, 2, 9, 3, 8, 4, 7, 5, 6},
                               {false, false, false, false, false, false, false, false, false, false});
  vec->SetNull(0, true);
  vec->SetNull(4, true);
  vec->SetNull(6, true);
  vec->SetNull(7, true);
  vec->SetNull(8, true);

  // Sorted: vec = [NULL,NULL,NULL,NULL,2,6,8,9,10], sorted = [0,4,6,7,8,2,9,5,3,1]
  sel_t sorted[common::Constants::K_DEFAULT_VECTOR_SIZE];
  VectorOps::Sort(*vec, sorted);

  EXPECT_EQ(0u, sorted[0]);
  EXPECT_EQ(4u, sorted[1]);
  EXPECT_EQ(6u, sorted[2]);
  EXPECT_EQ(7u, sorted[3]);
  EXPECT_EQ(8u, sorted[4]);
  EXPECT_EQ(2u, sorted[5]);
  EXPECT_EQ(9u, sorted[6]);
  EXPECT_EQ(5u, sorted[7]);
  EXPECT_EQ(3u, sorted[8]);
  EXPECT_EQ(1u, sorted[9]);
}

// NOLINTNEXTLINE
TEST_F(VectorSortTest, NullsAndFilter) {
  // vec = [-1.2,NULL,NULL,3.45,NULL,-67.89,NULL,NULL,NULL,123.45]
  auto vec = MakeFloatVector({-1.2F, 0, 0, 3.45, 0, -67.89F, 0, 0, 0, 123.45F},
                             {false, true, true, false, true, false, true, true, true, false});
  vec->SetNull(1, true);
  vec->SetNull(2, true);
  vec->SetNull(4, true);
  vec->SetNull(6, true);
  vec->SetNull(7, true);
  vec->SetNull(8, true);

  // vec = [-1.2,NULL,NULL,3.45,123.45]
  TupleIdList selections(vec->GetSize());
  selections = {0, 1, 2, 3, 9};
  vec->SetFilteredTupleIdList(&selections, selections.GetTupleCount());

  // Sorted: vec = [NULL,NULL,-1.2,3.45,123.45], sorted = [1,2,0,3,9]
  sel_t sorted[common::Constants::K_DEFAULT_VECTOR_SIZE];
  VectorOps::Sort(*vec, sorted);

  EXPECT_EQ(1u, sorted[0]);
  EXPECT_EQ(2u, sorted[1]);
  EXPECT_EQ(0u, sorted[2]);
  EXPECT_EQ(3u, sorted[3]);
  EXPECT_EQ(9u, sorted[4]);
}

}  // namespace noisepage::execution::sql::test
