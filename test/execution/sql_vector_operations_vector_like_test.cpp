#include "common/error/exception.h"
#include "execution/sql/constant_vector.h"
#include "execution/sql/tuple_id_list.h"
#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/sql_test.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class VectorLikeTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(VectorLikeTest, InputVerification) {
  exec::ExecutionSettings exec_settings{};
  // Left input is invalid type
  {
    auto a = MakeIntegerVector(10);
    auto b = MakeVarcharVector(10);
    auto tid_list = TupleIdList(a->GetSize());
    EXPECT_THROW(VectorOps::SelectLike(exec_settings, *a, *b, &tid_list), ExecutionException);
  }

  // Right input is invalid type
  {
    auto a = MakeVarcharVector(10);
    auto b = MakeFloatVector(10);
    auto tid_list = TupleIdList(a->GetSize());
    EXPECT_THROW(VectorOps::SelectLike(exec_settings, *a, *b, &tid_list), ExecutionException);
  }
}

// NOLINTNEXTLINE
TEST_F(VectorLikeTest, LikeConstant) {
  exec::ExecutionSettings exec_settings{};
  auto strings =
      MakeVarcharVector({"first", "second", "third", "fourth", "fifth"}, {false, false, false, false, false});
  auto pattern = ConstantVector(GenericValue::CreateVarchar("%d"));
  auto tid_list = TupleIdList(strings->GetSize());

  // strings == pattern = [1, 2]
  tid_list.AddAll();
  VectorOps::SelectLike(exec_settings, *strings, pattern, &tid_list);
  EXPECT_EQ(2u, tid_list.GetTupleCount());
  EXPECT_EQ(1u, tid_list[0]);
  EXPECT_EQ(2u, tid_list[1]);

  // strings != pattern = [0, 3, 4]
  tid_list.AddAll();
  VectorOps::SelectNotLike(exec_settings, *strings, pattern, &tid_list);
  EXPECT_EQ(3u, tid_list.GetTupleCount());
  EXPECT_EQ(0u, tid_list[0]);
  EXPECT_EQ(3u, tid_list[1]);
  EXPECT_EQ(4u, tid_list[2]);

  // strings = ["first", NULL, "third", NULL, NULL]
  strings->SetNull(1, true);
  strings->SetNull(3, true);
  strings->SetNull(4, true);

  // strings == pattern = [2]
  tid_list.AddAll();
  VectorOps::SelectLike(exec_settings, *strings, pattern, &tid_list);
  EXPECT_EQ(1u, tid_list.GetTupleCount());
  EXPECT_EQ(2u, tid_list[0]);

  // strings != pattern = [2]
  tid_list.AddAll();
  VectorOps::SelectNotLike(exec_settings, *strings, pattern, &tid_list);
  EXPECT_EQ(1u, tid_list.GetTupleCount());
  EXPECT_EQ(0u, tid_list[0]);

  tid_list.Clear();
  VectorOps::SelectNotLike(exec_settings, *strings, pattern, &tid_list);
  EXPECT_EQ(0u, tid_list.GetTupleCount());
}

// NOLINTNEXTLINE
TEST_F(VectorLikeTest, LikeVectorOfPatterns) {
  exec::ExecutionSettings exec_settings{};
  auto strings =
      MakeVarcharVector({"first", "second", "third", "fourth", "fifth"}, {false, false, false, false, false});
  auto patterns = MakeVarcharVector({"_%", "s_cnd", "third", "f%%_th", "fifth "}, {true, false, false, false, false});
  auto tid_list = TupleIdList(strings->GetSize());

  // strings == patterns = [2, 3]
  tid_list.AddAll();
  VectorOps::SelectLike(exec_settings, *strings, *patterns, &tid_list);
  EXPECT_EQ(2u, tid_list.GetTupleCount());
  EXPECT_EQ(2u, tid_list[0]);
  EXPECT_EQ(3u, tid_list[1]);
}

}  // namespace noisepage::execution::sql::test
