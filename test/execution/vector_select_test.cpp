#include <vector>

#include "common/exception.h"
#include "execution/sql/constant_vector.h"
#include "execution/sql/tuple_id_list.h"
#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/sql_test.h"
#include "execution/util/exception.h"

namespace terrier::execution::sql {

class VectorSelectTest : public TplTest {};

TEST_F(VectorSelectTest, MismatchedInputTypes) {
  auto a = MakeTinyIntVector(2);
  auto b = MakeBigIntVector(2);
  auto result = TupleIdList(a->GetSize());

  exec::ExecutionContext ctx(catalog::db_oid_t(0), nullptr, nullptr, nullptr, nullptr);

  result.AddAll();
  EXPECT_THROW(VectorOps::SelectEqual(common::ManagedPointer<exec::ExecutionContext>(&ctx), *a, *b, &result),
               TypeMismatchException);
}

TEST_F(VectorSelectTest, MismatchedSizes) {
  auto a = MakeTinyIntVector(54);
  auto b = MakeBigIntVector(19);
  auto result = TupleIdList(a->GetSize());
  result.AddAll();

  exec::ExecutionContext ctx(catalog::db_oid_t(0), nullptr, nullptr, nullptr, nullptr);

  EXPECT_THROW(VectorOps::SelectEqual(common::ManagedPointer<exec::ExecutionContext>(&ctx), *a, *b, &result),
               Exception);
}

TEST_F(VectorSelectTest, MismatchedCounts) {
  auto a = MakeTinyIntVector(10);
  auto b = MakeBigIntVector(10);

  auto filter_a = TupleIdList(a->GetCount());
  auto filter_b = TupleIdList(b->GetCount());

  filter_a = {0, 1, 2};
  filter_b = {9, 8};

  a->SetFilteredTupleIdList(&filter_a, filter_a.GetTupleCount());
  b->SetFilteredTupleIdList(&filter_b, filter_b.GetTupleCount());

  auto result = TupleIdList(a->GetSize());
  result.AddAll();

  exec::ExecutionContext ctx(catalog::db_oid_t(0), nullptr, nullptr, nullptr, nullptr);

  EXPECT_THROW(VectorOps::SelectEqual(common::ManagedPointer<exec::ExecutionContext>(&ctx), *a, *b, &result),
               Exception);
}

TEST_F(VectorSelectTest, InvalidTIDListSize) {
  auto a = MakeTinyIntVector(10);
  auto b = MakeBigIntVector(10);

  auto result = TupleIdList(1);
  exec::ExecutionContext ctx(catalog::db_oid_t(0), nullptr, nullptr, nullptr, nullptr);

  result.AddAll();

  EXPECT_THROW(VectorOps::SelectEqual(common::ManagedPointer<exec::ExecutionContext>(&ctx), *a, *b, &result),
               Exception);
}

TEST_F(VectorSelectTest, BasicSelect) {
  // a = [NULL, 1, 6, NULL, 4, 5]
  // b = [0, NULL, 4, NULL, 5, 5]
  auto a = MakeTinyIntVector({0, 1, 6, 3, 4, 5}, {true, false, false, true, false, false});
  auto b = MakeTinyIntVector({0, 1, 4, 3, 5, 5}, {false, true, false, true, false, false});
  auto vec_2 = ConstantVector(GenericValue::CreateTinyInt(2));

  exec::ExecutionContext ctx(catalog::db_oid_t(0), nullptr, nullptr, nullptr, nullptr);
  auto managed_ctx = common::ManagedPointer<exec::ExecutionContext>(&ctx);

  for (auto type_id :
       {TypeId::TinyInt, TypeId::SmallInt, TypeId::Integer, TypeId::BigInt, TypeId::Float, TypeId::Double}) {
    a->Cast(managed_ctx, type_id);
    b->Cast(managed_ctx, type_id);
    vec_2.Cast(managed_ctx, type_id);

    TupleIdList input_list(a->GetSize());
    input_list.AddAll();

    // a < 2
    VectorOps::SelectLessThan(managed_ctx, vec_2, *a, &input_list);
    EXPECT_EQ(1u, input_list.GetTupleCount());
    EXPECT_EQ(1u, input_list[0]);

    input_list.AddAll();

    // 2 < a
    VectorOps::SelectLessThan(managed_ctx, vec_2, *a, &input_list);
    EXPECT_EQ(3u, input_list.GetTupleCount());
    EXPECT_EQ(2u, input_list[0]);
    EXPECT_EQ(4u, input_list[1]);
    EXPECT_EQ(5u, input_list[2]);

    input_list.AddAll();

    // 2 == a
    VectorOps::SelectEqual(managed_ctx, vec_2, *a, &input_list);
    EXPECT_TRUE(input_list.IsEmpty());

    input_list.AddAll();

    // a != b = [2, 4]
    VectorOps::SelectNotEqual(managed_ctx, *a, *b, &input_list);
    EXPECT_EQ(2u, input_list.GetTupleCount());
    EXPECT_EQ(2u, input_list[0]);
    EXPECT_EQ(4u, input_list[1]);

    input_list.AddAll();

    // b == a = [5]
    VectorOps::SelectEqual(managed_ctx, *b, *a, &input_list);
    EXPECT_EQ(1u, input_list.GetTupleCount());
    EXPECT_EQ(5u, input_list[0]);

    input_list.AddAll();

    // a < b = [4]
    VectorOps::SelectLessThan(managed_ctx, *a, *b, &input_list);
    EXPECT_EQ(1u, input_list.GetTupleCount());
    EXPECT_EQ(4u, input_list[0]);

    input_list.AddAll();

    // a <= b = [4, 5]
    VectorOps::SelectLessThanEqual(managed_ctx, *a, *b, &input_list);
    EXPECT_EQ(2, input_list.GetTupleCount());
    EXPECT_EQ(4u, input_list[0]);
    EXPECT_EQ(5u, input_list[1]);

    input_list.AddAll();

    // a > b = [2]
    VectorOps::SelectGreaterThan(managed_ctx, *a, *b, &input_list);
    EXPECT_EQ(1, input_list.GetTupleCount());
    EXPECT_EQ(2u, input_list[0]);

    input_list.AddAll();

    // a >= b = [2]
    VectorOps::SelectGreaterThanEqual(managed_ctx, *a, *b, &input_list);
    EXPECT_EQ(2, input_list.GetTupleCount());
    EXPECT_EQ(2u, input_list[0]);
    EXPECT_EQ(5u, input_list[1]);
  }
}

TEST_F(VectorSelectTest, SelectNullConstant) {
  // a = [0, 1, NULL, NULL, 4, 5]
  auto a = MakeIntegerVector({0, 1, 2, 3, 4, 5}, {false, false, true, true, false, false});
  auto null_constant = ConstantVector(GenericValue::CreateNull(a->GetTypeId()));
  exec::ExecutionContext ctx(catalog::db_oid_t(0), nullptr, nullptr, nullptr, nullptr);
  auto managed_ctx = common::ManagedPointer<exec::ExecutionContext>(&ctx);

#define NULL_TEST(OP)                                             \
  /* a <OP> NULL */                                               \
  {                                                               \
    TupleIdList list(a->GetSize());                               \
    list.AddAll();                                                \
    VectorOps::Select##OP(managed_ctx, *a, null_constant, &list); \
    EXPECT_TRUE(list.IsEmpty());                                  \
  }                                                               \
  /* NULL <OP> a */                                               \
  {                                                               \
    TupleIdList list(a->GetSize());                               \
    list.AddAll();                                                \
    VectorOps::Select##OP(managed_ctx, *a, null_constant, &list); \
    EXPECT_TRUE(list.IsEmpty());                                  \
  }

  NULL_TEST(Equal)
  NULL_TEST(GreaterThan)
  NULL_TEST(GreaterThanEqual)
  NULL_TEST(LessThan)
  NULL_TEST(LessThanEqual)
  NULL_TEST(NotEqual)

#undef NULL_TEST
}

TEST_F(VectorSelectTest, StringSelection) {
  auto a = MakeVarcharVector(
      {"His palm's are sweaty", {}, "arms are heavy", "vomit on his sweater already", "mom's spaghetti"},
      {false, true, false, false, false});
  auto b = MakeVarcharVector(
      {"He's nervous", "but on the surface he looks calm and ready", {}, "to drop bombs", "but he keeps on forgetting"},
      {false, false, true, false, false});
  auto tid_list = TupleIdList(a->GetSize());

  exec::ExecutionContext ctx(catalog::db_oid_t(0), nullptr, nullptr, nullptr, nullptr);
  auto managed_ctx = common::ManagedPointer<exec::ExecutionContext>(&ctx);

  // a == b = []
  tid_list.AddAll();
  VectorOps::SelectEqual(managed_ctx, *a, *b, &tid_list);
  EXPECT_EQ(0u, tid_list.GetTupleCount());

  // a != b = [0, 3, 4]
  tid_list.AddAll();
  VectorOps::SelectNotEqual(managed_ctx, *a, *b, &tid_list);
  EXPECT_EQ(3u, tid_list.GetTupleCount());
  EXPECT_EQ(0u, tid_list[0]);
  EXPECT_EQ(3u, tid_list[1]);
  EXPECT_EQ(4u, tid_list[2]);

  // a < b = [0]
  tid_list.AddAll();
  VectorOps::SelectLessThan(managed_ctx, *a, *b, &tid_list);
  EXPECT_EQ(0u, tid_list.GetTupleCount());

  // a > b = [1, 3, 4]
  tid_list.AddAll();
  VectorOps::SelectGreaterThan(managed_ctx, *a, *b, &tid_list);
  EXPECT_EQ(3u, tid_list.GetTupleCount());
  EXPECT_EQ(0u, tid_list[0]);
  EXPECT_EQ(3u, tid_list[1]);
  EXPECT_EQ(4u, tid_list[2]);
}

TEST_F(VectorSelectTest, IsNullAndIsNotNull) {
  auto vec = MakeFloatVector({1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0}, {false, true, false, true, true, false, false});
  auto tid_list = TupleIdList(vec->GetSize());

  // Try first with a full TID list

  // IS NULL(vec) = [1, 3, 4]
  tid_list.AddAll();
  VectorOps::IsNull(*vec, &tid_list);
  EXPECT_EQ(3u, tid_list.GetTupleCount());
  EXPECT_EQ(1u, tid_list[0]);
  EXPECT_EQ(3u, tid_list[1]);
  EXPECT_EQ(4u, tid_list[2]);

  // IS_NOT_NULL(vec) = [0, 2, 5, 6]
  tid_list.AddAll();
  VectorOps::IsNotNull(*vec, &tid_list);
  EXPECT_EQ(4u, tid_list.GetTupleCount());
  EXPECT_EQ(0u, tid_list[0]);
  EXPECT_EQ(2u, tid_list[1]);
  EXPECT_EQ(5u, tid_list[2]);
  EXPECT_EQ(6u, tid_list[3]);

  // Try with a partial input list

  tid_list.Clear();
  tid_list.Add(1);
  tid_list.Add(4);
  VectorOps::IsNull(*vec, &tid_list);
  EXPECT_EQ(2u, tid_list.GetTupleCount());
  EXPECT_EQ(1u, tid_list[0]);
  EXPECT_EQ(4u, tid_list[1]);

  tid_list.Clear();
  tid_list.AddRange(2, 5);
  VectorOps::IsNotNull(*vec, &tid_list);
  EXPECT_EQ(1u, tid_list.GetTupleCount());
  EXPECT_EQ(2u, tid_list[0]);
}

}  // namespace terrier::execution::sql
