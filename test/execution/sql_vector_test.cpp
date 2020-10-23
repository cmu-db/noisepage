#include <numeric>
#include <vector>

#include "common/error/exception.h"
#include "execution/exec/execution_settings.h"
#include "execution/sql/vector.h"
#include "execution/sql_test.h"
#include "execution/util/bit_util.h"

namespace noisepage::execution::sql::test {

class VectorTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(VectorTest, CheckEmpty) {
  // Creating an empty vector should have zero count, zero size, no selection vector, and should
  // clean itself up upon destruction.
  auto vec1 = MakeIntegerVector(0);
  EXPECT_EQ(0u, vec1->GetSize());
  EXPECT_EQ(0u, vec1->GetCount());
  EXPECT_EQ(nullptr, vec1->GetFilteredTupleIdList());
  vec1->CheckIntegrity();
}

// NOLINTNEXTLINE
TEST_F(VectorTest, Clear) {
  auto vec = MakeTinyIntVector(10);

  EXPECT_EQ(10u, vec->GetSize());
  EXPECT_EQ(10u, vec->GetCount());
  EXPECT_EQ(nullptr, vec->GetFilteredTupleIdList());

  for (uint32_t i = 0; i < vec->GetSize(); i++) {
    EXPECT_EQ(GenericValue::CreateTinyInt(0), vec->GetValue(i));
  }
  vec->CheckIntegrity();
}

// NOLINTNEXTLINE
TEST_F(VectorTest, InitFromArray) {
  const uint32_t num_elems = 5;

  // Try simple arithmetic vector
  {
    float arr[num_elems] = {-1.2, -34.56, 6.7, 8.91011, 1213.1415};

    Vector vec(TypeId::Float);
    vec.Reference(reinterpret_cast<byte *>(arr), nullptr, num_elems);
    EXPECT_EQ(num_elems, vec.GetSize());
    EXPECT_EQ(num_elems, vec.GetCount());
    EXPECT_EQ(nullptr, vec.GetFilteredTupleIdList());

    for (uint32_t i = 0; i < num_elems; i++) {
      auto val = vec.GetValue(i);
      EXPECT_EQ(GenericValue::CreateReal(arr[i]), val);
    }
    vec.CheckIntegrity();
  }

  // Now a string array
  {
    VarlenHeap varlens;
    noisepage::storage::VarlenEntry arr[num_elems] = {varlens.AddVarlen("go loko"), varlens.AddVarlen("hot-line bling"),
                                                      varlens.AddVarlen("kawhi"), varlens.AddVarlen("6ix"),
                                                      varlens.AddVarlen("king city")};
    Vector vec(TypeId::Varchar);
    vec.Reference(reinterpret_cast<byte *>(arr), nullptr, num_elems);
    EXPECT_EQ(num_elems, vec.GetSize());
    EXPECT_EQ(num_elems, vec.GetCount());
    EXPECT_EQ(nullptr, vec.GetFilteredTupleIdList());

    for (uint32_t i = 0; i < num_elems; i++) {
      auto val = vec.GetValue(i);
      EXPECT_EQ(GenericValue::CreateVarchar(arr[i].StringView()), val);
    }
    vec.CheckIntegrity();
  }
}

// NOLINTNEXTLINE
TEST_F(VectorTest, GetAndSet) {
  auto vec = MakeBooleanVector(1);

  // vec[0] = false
  vec->SetValue(0, GenericValue::CreateBoolean(false));
  EXPECT_EQ(GenericValue::CreateBoolean(false), vec->GetValue(0));

  // vec[0] = true (NULL)
  vec->SetNull(0, true);
  EXPECT_TRUE(vec->GetValue(0).IsNull());

  // vec[0] = true
  vec->SetValue(0, GenericValue::CreateBoolean(true));
  EXPECT_EQ(GenericValue::CreateBoolean(true), vec->GetValue(0));

  vec->CheckIntegrity();
}

// NOLINTNEXTLINE
TEST_F(VectorTest, GetAndSetNumeric) {
#define GEN_TEST(TYPE)                                          \
  {                                                             \
    auto vec = Make##TYPE##Vector(1);                           \
    vec->SetValue(0, GenericValue::Create##TYPE(1));            \
    EXPECT_EQ(GenericValue::Create##TYPE(1), vec->GetValue(0)); \
    vec->SetNull(0, true);                                      \
    EXPECT_TRUE(vec->IsNull(0));                                \
    EXPECT_TRUE(vec->GetValue(0).IsNull());                     \
    vec->SetValue(0, GenericValue::Create##TYPE(2));            \
    EXPECT_EQ(GenericValue::Create##TYPE(2), vec->GetValue(0)); \
    vec->CheckIntegrity();                                      \
  }

  GEN_TEST(TinyInt);
  GEN_TEST(SmallInt);
  GEN_TEST(Integer);
  GEN_TEST(BigInt);
  GEN_TEST(Float);
  GEN_TEST(Double);

#undef GEN_TEST
}

// NOLINTNEXTLINE
TEST_F(VectorTest, GetAndSetString) {
  auto vec = MakeVarcharVector(1);
  vec->SetValue(0, GenericValue::CreateVarchar("hello"));
  EXPECT_EQ(GenericValue::CreateVarchar("hello"), vec->GetValue(0));
  vec->SetNull(0, true);
  EXPECT_TRUE(vec->IsNull(0));
  EXPECT_TRUE(vec->GetValue(0).IsNull());
  vec->CheckIntegrity();
}

// NOLINTNEXTLINE
TEST_F(VectorTest, SetSelectionVector) {
  // vec = [0, 1, 2, 3, NULL, 5, 6, 7, 8, 9]
  auto vec = MakeTinyIntVector(10);
  for (uint64_t i = 0; i < vec->GetSize(); i++) {
    vec->SetValue(i, GenericValue::CreateTinyInt(i));
  }
  vec->SetNull(4, true);

  EXPECT_FLOAT_EQ(1.0, vec->ComputeSelectivity());

  // After selection, vec = [0, NULL, 5, 9]
  auto filter = TupleIdList(vec->GetCount());
  filter = {0, 4, 5, 9};
  vec->SetFilteredTupleIdList(&filter, filter.GetTupleCount());

  // Verify
  EXPECT_EQ(4u, vec->GetCount());
  EXPECT_EQ(10u, vec->GetSize());
  EXPECT_EQ(&filter, vec->GetFilteredTupleIdList());
  EXPECT_FLOAT_EQ(0.4, vec->ComputeSelectivity());

  // Check indexing post-selection
  EXPECT_EQ(GenericValue::CreateTinyInt(0), vec->GetValue(0));
  EXPECT_TRUE(vec->IsNull(1));
  EXPECT_EQ(GenericValue::CreateTinyInt(5), vec->GetValue(2));
  EXPECT_EQ(GenericValue::CreateTinyInt(9), vec->GetValue(3));
  vec->CheckIntegrity();
}

// NOLINTNEXTLINE
TEST_F(VectorTest, Reference) {
  // vec = [0, 1, NULL, 3, 4, 5, 6, 7, 8, 9]
  auto vec = MakeIntegerVector(10);
  for (uint64_t i = 0; i < vec->GetSize(); i++) {
    vec->SetValue(i, GenericValue::CreateInteger(i));
  }
  vec->SetNull(2, true);

  // Create a new vector that references the one we just created. We intentionally create it with a
  // different type to ensure we switch types.

  Vector vec2(vec->GetTypeId(), false, false);
  vec2.Reference(vec.get());
  EXPECT_EQ(TypeId::Integer, vec2.GetTypeId());
  EXPECT_EQ(vec->GetSize(), vec2.GetSize());
  EXPECT_EQ(vec->GetCount(), vec2.GetCount());
  for (uint64_t i = 0; i < vec2.GetSize(); i++) {
    if (i == 2) {
      EXPECT_TRUE(vec2.IsNull(i));
    } else {
      EXPECT_FALSE(vec2.IsNull(i));
      EXPECT_EQ(vec->GetValue(i), vec2.GetValue(i));
    }
  }
}

// NOLINTNEXTLINE
TEST_F(VectorTest, Move) {
  // vec = [0, 1, 2, 3, NULL, 5, 6, 7, 8, 9]
  auto vec = MakeIntegerVector(10);
  for (uint64_t i = 0; i < vec->GetSize(); i++) {
    vec->SetValue(i, GenericValue::CreateInteger(i));
  }
  vec->SetNull(4, true);

  // Filtered vector, vec = [0, 1, NULL, 7, 8]
  auto filter = TupleIdList(vec->GetCount());
  filter = {0, 1, 4, 7, 8};
  vec->SetFilteredTupleIdList(&filter, filter.GetTupleCount());

  // Move the original vector to the target
  // target = [(0), (1), 2, 3, (NULL), 5, 6, (7), (8), 9], bracketed elements are selected
  auto target = MakeIntegerVector(vec->GetSize());
  vec->MoveTo(target.get());

  // First, the old vector should empty
  EXPECT_EQ(0u, vec->GetSize());
  EXPECT_EQ(0u, vec->GetCount());
  EXPECT_EQ(nullptr, vec->GetFilteredTupleIdList());
  EXPECT_EQ(nullptr, vec->GetData());

  // The new vector should own the data
  EXPECT_EQ(10u, target->GetSize());
  EXPECT_EQ(filter.GetTupleCount(), target->GetCount());
  EXPECT_EQ(&filter, target->GetFilteredTupleIdList());
  EXPECT_NE(nullptr, target->GetData());

  for (uint64_t i = 0; i < target->GetCount(); i++) {
    if (i == 2) {
      EXPECT_TRUE(target->IsNull(i));
    } else {
      EXPECT_FALSE(target->IsNull(i));
      EXPECT_EQ(GenericValue::CreateInteger(filter[i]), target->GetValue(i));
    }
  }
}

// NOLINTNEXTLINE
TEST_F(VectorTest, Copy) {
  constexpr uint32_t num_elems = 10;
  exec::ExecutionSettings exec_settings{};

  for (auto type_id :
       {TypeId::TinyInt, TypeId::SmallInt, TypeId::Integer, TypeId::BigInt, TypeId::Float, TypeId::Double}) {
    // vec = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    auto vec = MakeVector(type_id, num_elems);
    for (uint64_t i = 0; i < vec->GetSize(); i++) {
      vec->SetValue(i, GenericValue::CreateTinyInt(i).CastTo(exec_settings, type_id));
    }

    // Filtered vec = [0, 1, 3, 7, 8]
    auto filter = TupleIdList(vec->GetCount());
    filter = {0, 1, 3, 7, 8};
    vec->SetFilteredTupleIdList(&filter, filter.GetTupleCount());

    auto target = MakeVector(type_id, num_elems);
    vec->CopyTo(target.get());

    // Copying is a "densifying" operation; the count and size should be 5, and
    // there shouldn't be a selection vector present in the target.
    EXPECT_EQ(filter.GetTupleCount(), target->GetSize());
    EXPECT_EQ(filter.GetTupleCount(), target->GetCount());
    EXPECT_EQ(nullptr, target->GetFilteredTupleIdList());

    for (uint64_t i = 0; i < target->GetCount(); i++) {
      EXPECT_EQ(vec->GetValue(i).CastTo(exec_settings, type_id), target->GetValue(i));
    }
  }
}

// NOLINTNEXTLINE
TEST_F(VectorTest, CopyWithOffset) {
  // vec = [0, 1, 2, 3, NULL, 5, 6, 7, NULL, 9]
  auto vec = MakeIntegerVector(10);
  for (uint64_t i = 0; i < vec->GetSize(); i++) {
    vec->SetValue(i, GenericValue::CreateInteger(i));
  }
  vec->SetNull(4, true);
  vec->SetNull(8, true);

  // Filtered vec = [0, 2, NULL, 6, NULL]
  auto filter = TupleIdList(vec->GetCount());
  filter = {0, 2, 4, 6, 8};
  vec->SetFilteredTupleIdList(&filter, filter.GetTupleCount());

  // We copy all elements [2, 5). Then target = [NULL, 6 NULL]
  const uint32_t offset = 2;
  auto target = MakeIntegerVector(vec->GetSize());
  vec->CopyTo(target.get(), offset);

  // Copying is a "densifying" operation; the count and size should match, and
  // there shouldn't be a selection vector present in the target.
  EXPECT_EQ(3u, target->GetSize());
  EXPECT_EQ(3u, target->GetCount());
  EXPECT_EQ(nullptr, target->GetFilteredTupleIdList());

  EXPECT_TRUE(target->IsNull(0));
  EXPECT_EQ(GenericValue::CreateInteger(6), target->GetValue(1));
  EXPECT_TRUE(target->IsNull(2));
}

// NOLINTNEXTLINE
TEST_F(VectorTest, CopyStringVector) {
  // vec = ['val-0','val-1','val-2','val-3','val-4','val-5','val-6','val-7','val-8','val-9']
  auto vec = MakeVarcharVector(10);
  for (uint64_t i = 0; i < vec->GetSize(); i++) {
    vec->SetValue(i, GenericValue::CreateVarchar("val-" + std::to_string(i)));
  }

  // Filtered vec = ['val-0',NULL,'val-4','val-6','val-8']
  auto filter = TupleIdList(vec->GetCount());
  filter = {0, 2, 4, 6, 8};
  vec->SetFilteredTupleIdList(&filter, filter.GetTupleCount());
  vec->SetNull(1, true);

  // Copying is a "densifying" operation; the count and size should match, and
  // there shouldn't be a selection vector present in the target.
  auto target = MakeVarcharVector(vec->GetSize());
  vec->CopyTo(target.get());

  // Force deletion of source vector to ensure target has actually copied strings into its own heap
  vec.reset();

  EXPECT_EQ(filter.GetTupleCount(), target->GetSize());
  EXPECT_EQ(filter.GetTupleCount(), target->GetCount());
  EXPECT_EQ(nullptr, target->GetFilteredTupleIdList());
  EXPECT_EQ(GenericValue::CreateVarchar("val-0"), target->GetValue(0));
  EXPECT_TRUE(target->IsNull(1));
  EXPECT_EQ(GenericValue::CreateVarchar("val-4"), target->GetValue(2));
  EXPECT_EQ(GenericValue::CreateVarchar("val-6"), target->GetValue(3));
  EXPECT_EQ(GenericValue::CreateVarchar("val-8"), target->GetValue(4));
}

// NOLINTNEXTLINE
TEST_F(VectorTest, Append) {
  // vec1 = [1.0, NULL, 3.0]
  auto vec1 = MakeDoubleVector(3);
  vec1->SetValue(0, GenericValue::CreateDouble(1.0));
  vec1->SetNull(1, true);
  vec1->SetValue(2, GenericValue::CreateDouble(3.0));

  // vec2 = [10.0, 11.0]
  auto vec2 = MakeDoubleVector(2);
  vec2->SetValue(0, GenericValue::CreateDouble(10.0));
  vec2->SetValue(1, GenericValue::CreateDouble(11.0));

  // vec2 = [10.0, 11.0, 1.0, NULL, 3.0]
  vec2->Append(*vec1);

  EXPECT_EQ(5u, vec2->GetSize());
  EXPECT_EQ(5u, vec2->GetCount());
  EXPECT_EQ(nullptr, vec2->GetFilteredTupleIdList());

  EXPECT_EQ(GenericValue::CreateDouble(10.0), vec2->GetValue(0));
  EXPECT_EQ(GenericValue::CreateDouble(11.0), vec2->GetValue(1));
  EXPECT_EQ(GenericValue::CreateDouble(1.0), vec2->GetValue(2));
  EXPECT_TRUE(vec2->IsNull(3));
  EXPECT_EQ(GenericValue::CreateDouble(3.0), vec2->GetValue(4));
}

// NOLINTNEXTLINE
TEST_F(VectorTest, AppendWithSelectionVector) {
  // vec1 = [1.0, NULL, 3.0]
  auto vec1 = MakeFloatVector(3);
  vec1->SetValue(0, GenericValue::CreateFloat(1.0));
  vec1->SetNull(1, true);
  vec1->SetValue(2, GenericValue::CreateFloat(3.0));

  // Filtered vec1 = [NULL, 3.0]
  auto filter = TupleIdList(vec1->GetCount());
  filter = {1, 2};
  vec1->SetFilteredTupleIdList(&filter, filter.GetTupleCount());

  // vec2 = [10.0, 11.0]
  auto vec2 = MakeFloatVector(2);
  vec2->SetValue(0, GenericValue::CreateFloat(10.0));
  vec2->SetValue(1, GenericValue::CreateFloat(11.0));

  // vec2 = [10.0, 11.0, NULL, 3.0]
  vec2->Append(*vec1);

  EXPECT_EQ(4u, vec2->GetSize());
  EXPECT_EQ(4u, vec2->GetCount());
  EXPECT_EQ(nullptr, vec2->GetFilteredTupleIdList());

  EXPECT_EQ(GenericValue::CreateFloat(10.0), vec2->GetValue(0));
  EXPECT_EQ(GenericValue::CreateFloat(11.0), vec2->GetValue(1));
  EXPECT_TRUE(vec2->IsNull(2));
  EXPECT_EQ(GenericValue::CreateFloat(3.0), vec2->GetValue(3));
}

// NOLINTNEXTLINE
TEST_F(VectorTest, Pack) {
  // vec = [NULL,1,2,3,4,5,6,7,8,NULL]
  auto vec = MakeSmallIntVector({0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                                {true, false, false, false, false, false, false, false, false, true});

  // Try to flatten an already flattened vector
  {
    vec->Pack();
    EXPECT_EQ(10u, vec->GetCount());
    EXPECT_EQ(10u, vec->GetSize());
    EXPECT_EQ(nullptr, vec->GetFilteredTupleIdList());
    EXPECT_TRUE(vec->IsNull(0));
    EXPECT_TRUE(vec->IsNull(vec->GetSize() - 1));
  }

  // Try flattening with a filtered vector
  {
    // vec = [NULL,3,5,7]
    auto tids = TupleIdList(vec->GetSize());
    tids = {0, 3, 5, 7};
    vec->SetFilteredTupleIdList(&tids, tids.GetTupleCount());
    vec->Pack();

    EXPECT_EQ(4u, vec->GetCount());
    EXPECT_EQ(4u, vec->GetSize());
    EXPECT_EQ(nullptr, vec->GetFilteredTupleIdList());
    EXPECT_TRUE(vec->IsNull(0));
    EXPECT_EQ(GenericValue::CreateSmallInt(3), vec->GetValue(1));
    EXPECT_EQ(GenericValue::CreateSmallInt(5), vec->GetValue(2));
    EXPECT_EQ(GenericValue::CreateSmallInt(7), vec->GetValue(3));
  }
}

// NOLINTNEXTLINE
TEST_F(VectorTest, GetNonNullSelections) {
  // vec1 = [1,2,3,4,5,6,7,8,9,10,11,12]
  auto vec1 = MakeFloatVector({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
                              {false, false, false, false, false, false, false, false, false, false, false, false});

  TupleIdList non_null_tids(vec1->GetSize());
  TupleIdList null_tids(vec1->GetSize());

  // Initially, no NULLs
  {
    vec1->GetNonNullSelections(&non_null_tids, &null_tids);
    EXPECT_TRUE(null_tids.IsEmpty());
    EXPECT_TRUE(non_null_tids.IsFull());
  }

  // vec1      = [1,2,3,4,NULL,6,7,8,9,10,NULL,12]
  // NULLs     = [4,10]
  // non-NULLs = [0,1,2,3,5,6,7,8,9,11]
  {
    vec1->SetNull(4, true);
    vec1->SetNull(10, true);
    vec1->GetNonNullSelections(&non_null_tids, &null_tids);
    EXPECT_EQ(2, null_tids.GetTupleCount());
    EXPECT_EQ(4u, null_tids[0]);
    EXPECT_EQ(10u, null_tids[1]);
    EXPECT_EQ(vec1->GetSize() - 2, non_null_tids.GetTupleCount());
    EXPECT_FALSE(non_null_tids.Contains(4));
    EXPECT_FALSE(non_null_tids.Contains(10));
  }

  // vec1       = [1,3,NULL,7,9]
  // selections = [0,2,4,6,8]
  // NULLs      = [2]
  // non-NULLs  = [0,2,6,8]
  {
    TupleIdList selections(vec1->GetSize());
    selections = {0, 2, 4, 6, 8};
    vec1->SetFilteredTupleIdList(&selections, selections.GetTupleCount());
    vec1->GetNonNullSelections(&non_null_tids, &null_tids);
    EXPECT_EQ(1, null_tids.GetTupleCount());
    EXPECT_EQ(4u, null_tids[0]);
    EXPECT_EQ(4u, non_null_tids.GetTupleCount());
    EXPECT_EQ(0u, non_null_tids[0]);
    EXPECT_EQ(2u, non_null_tids[1]);
    EXPECT_EQ(6u, non_null_tids[2]);
    EXPECT_EQ(8u, non_null_tids[3]);
  }
}

// NOLINTNEXTLINE
TEST_F(VectorTest, Print) {
  {
    auto vec = MakeBooleanVector({false, true, true, false}, {false, false, false, false});
    EXPECT_EQ("Boolean=[False,True,True,False]", vec->ToString());
  }

#define CHECK_NUMERIC_VECTOR_PRINT(TYPE)                                          \
  {                                                                               \
    auto vec = Make##TYPE##Vector({10, 20, 30, 40}, {false, true, false, false}); \
    EXPECT_EQ(#TYPE "=[10,NULL,30,40]", vec->ToString());                         \
  };

  CHECK_NUMERIC_VECTOR_PRINT(TinyInt);
  CHECK_NUMERIC_VECTOR_PRINT(SmallInt);
  CHECK_NUMERIC_VECTOR_PRINT(Integer);
  CHECK_NUMERIC_VECTOR_PRINT(BigInt);
#undef CHECK_NUMERIC_VECTOR_PRINT

  {
    auto vec = MakeVarcharVector({"first", "second", "third"}, {false, true, false});
    EXPECT_EQ("VarChar=['first',NULL,'third']", vec->ToString());
  }
}

}  // namespace noisepage::execution::sql::test
