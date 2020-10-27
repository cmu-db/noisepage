#include <numeric>
#include <vector>

#include "common/error/exception.h"
#include "execution/sql/vector.h"
#include "execution/sql_test.h"
#include "execution/util/bit_util.h"

namespace noisepage::execution::sql::test {

class VectorCastTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(VectorCastTest, Cast) {
  exec::ExecutionSettings exec_settings{};

  // vec(i8) = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
  auto vec = MakeTinyIntVector(10);
  for (uint64_t i = 0; i < vec->GetSize(); i++) {
    vec->SetValue(i, GenericValue::CreateTinyInt(i));
  }

  // vec(i8) = [1, 2, NULL, 8]
  auto filter = TupleIdList(vec->GetCount());
  filter = {1, 2, 7, 8};
  vec->SetFilteredTupleIdList(&filter, filter.GetTupleCount());
  vec->SetNull(2, true);

  // Case 1: try up-cast from int8_t -> int32_t with valid values
  EXPECT_NO_THROW(vec->Cast(exec_settings, TypeId::Integer));
  EXPECT_EQ(TypeId::Integer, vec->GetTypeId());
  EXPECT_EQ(10u, vec->GetSize());
  EXPECT_EQ(filter.GetTupleCount(), vec->GetCount());
  EXPECT_EQ(&filter, vec->GetFilteredTupleIdList());
  EXPECT_EQ(GenericValue::CreateInteger(1), vec->GetValue(0));
  EXPECT_EQ(GenericValue::CreateInteger(2), vec->GetValue(1));
  EXPECT_TRUE(vec->IsNull(2));
  EXPECT_EQ(GenericValue::CreateInteger(8), vec->GetValue(3));

  // Case 2: try down-cast int32_t -> int16_t with valid values
  EXPECT_NO_THROW(vec->Cast(exec_settings, TypeId::SmallInt));
  EXPECT_TRUE(vec->GetTypeId() == TypeId::SmallInt);
  EXPECT_EQ(10u, vec->GetSize());
  EXPECT_EQ(filter.GetTupleCount(), vec->GetCount());
  EXPECT_EQ(&filter, vec->GetFilteredTupleIdList());
  EXPECT_EQ(GenericValue::CreateSmallInt(1), vec->GetValue(0));
  EXPECT_EQ(GenericValue::CreateSmallInt(2), vec->GetValue(1));
  EXPECT_TRUE(vec->IsNull(2));
  EXPECT_EQ(GenericValue::CreateSmallInt(8), vec->GetValue(3));

  // Case 3: try down-cast int16_t -> int8_t with one value out-of-range
  // vec = [1, 150, NULL, 8] -- 150 is in an invalid int8_t
  vec->SetValue(1, GenericValue::CreateSmallInt(150));
  EXPECT_THROW(vec->Cast(exec_settings, TypeId::TinyInt), ExecutionException);
}

// NOLINTNEXTLINE
TEST_F(VectorCastTest, CastWithNulls) {
  exec::ExecutionSettings exec_settings{};

  // vec(int) = [0, 1, 2, 3, NULL, 5, 6, 7, NULL, 9]
  auto vec = MakeIntegerVector(10);
  for (uint64_t i = 0; i < vec->GetSize(); i++) {
    vec->SetValue(i, GenericValue::CreateInteger(i));
  }
  vec->SetNull(4, true);
  vec->SetNull(8, true);

  // After casting vec(int) to vec(bigint), the NULL values are retained
  // vec(bigint) = [0, 1, 2, 3, NULL, 5, 6, 7, NULL, 9]

  EXPECT_NO_THROW(vec->Cast(exec_settings, TypeId::BigInt));
  EXPECT_EQ(TypeId::BigInt, vec->GetTypeId());
  EXPECT_EQ(10u, vec->GetSize());
  EXPECT_EQ(10u, vec->GetCount());
  EXPECT_EQ(nullptr, vec->GetFilteredTupleIdList());

  for (uint64_t i = 0; i < vec->GetSize(); i++) {
    if (i == 4 || i == 8) {
      EXPECT_TRUE(vec->IsNull(i));
    } else {
      EXPECT_EQ(GenericValue::CreateBigInt(i), vec->GetValue(i));
    }
  }
}

// NOLINTNEXTLINE
TEST_F(VectorCastTest, NumericDowncast) {
  exec::ExecutionSettings exec_settings{};

#define CHECK_CAST(SRC_TYPE, DEST_TYPE, DEST_CPP_TYPE)                                             \
  {                                                                                                \
    const uint32_t num_elems = 20;                                                                 \
    auto vec = Make##SRC_TYPE##Vector(num_elems);                                                  \
    for (uint32_t i = 0; i < vec->GetSize(); i++) {                                                \
      vec->SetValue(i, GenericValue::Create##SRC_TYPE(i));                                         \
    }                                                                                              \
    EXPECT_NO_THROW(vec->Cast(exec_settings, TypeId::DEST_TYPE));                                  \
    EXPECT_EQ(TypeId::DEST_TYPE, vec->GetTypeId());                                                \
    EXPECT_EQ(num_elems, vec->GetSize());                                                          \
    EXPECT_EQ(num_elems, vec->GetCount());                                                         \
    EXPECT_EQ(nullptr, vec->GetFilteredTupleIdList());                                             \
    for (uint64_t i = 0; i < vec->GetSize(); i++) {                                                \
      EXPECT_EQ(GenericValue::Create##DEST_TYPE(static_cast<DEST_CPP_TYPE>(i)), vec->GetValue(i)); \
    }                                                                                              \
  }

  CHECK_CAST(Double, Boolean, bool);
  CHECK_CAST(Float, Boolean, bool);
  CHECK_CAST(BigInt, Boolean, bool);
  CHECK_CAST(Integer, Boolean, bool);
  CHECK_CAST(SmallInt, Boolean, bool);
  CHECK_CAST(TinyInt, Boolean, bool);

  CHECK_CAST(Double, TinyInt, int8_t);
  CHECK_CAST(Float, TinyInt, int8_t);
  CHECK_CAST(BigInt, TinyInt, int8_t);
  CHECK_CAST(Integer, TinyInt, int8_t);
  CHECK_CAST(SmallInt, TinyInt, int8_t);

  CHECK_CAST(Double, SmallInt, int16_t);
  CHECK_CAST(Float, SmallInt, int16_t);
  CHECK_CAST(BigInt, SmallInt, int16_t);
  CHECK_CAST(Integer, SmallInt, int16_t);

  CHECK_CAST(Double, Integer, int32_t);
  CHECK_CAST(Float, Integer, int32_t);
  CHECK_CAST(BigInt, Integer, int32_t);

  CHECK_CAST(Double, BigInt, int64_t);
  CHECK_CAST(Float, BigInt, int64_t);

  CHECK_CAST(Double, Float, float);

#undef CHECK_CAST
}

// NOLINTNEXTLINE
TEST_F(VectorCastTest, DateCast) {
  exec::ExecutionSettings exec_settings{};

  // a = [NULL, "1980-01-01", "2016-01-27", NULL, "2000-01-01", "2015-08-01"]
  auto a = MakeDateVector({Date::FromYMD(1980, 1, 1), Date::FromYMD(1980, 1, 1), Date::FromYMD(2016, 1, 27),
                           Date::FromYMD(1980, 1, 1), Date::FromYMD(2000, 1, 1), Date::FromYMD(2015, 8, 1)},
                          {true, false, false, true, false, false});

  EXPECT_THROW(a->Cast(exec_settings, TypeId::TinyInt), NotImplementedException);
  EXPECT_THROW(a->Cast(exec_settings, TypeId::SmallInt), NotImplementedException);
  EXPECT_THROW(a->Cast(exec_settings, TypeId::Integer), NotImplementedException);
  EXPECT_THROW(a->Cast(exec_settings, TypeId::BigInt), NotImplementedException);
  EXPECT_THROW(a->Cast(exec_settings, TypeId::Float), NotImplementedException);
  EXPECT_THROW(a->Cast(exec_settings, TypeId::Double), NotImplementedException);
  EXPECT_NO_THROW(a->Cast(exec_settings, TypeId::Varchar));

  EXPECT_EQ(TypeId::Varchar, a->GetTypeId());
  EXPECT_TRUE(a->IsNull(0));
  EXPECT_EQ(GenericValue::CreateVarchar("1980-01-01"), a->GetValue(1));
  EXPECT_EQ(GenericValue::CreateVarchar("2016-01-27"), a->GetValue(2));
  EXPECT_TRUE(a->IsNull(3));
  EXPECT_EQ(GenericValue::CreateVarchar("2000-01-01"), a->GetValue(4));
  EXPECT_EQ(GenericValue::CreateVarchar("2015-08-01"), a->GetValue(5));
}

// NOLINTNEXTLINE
TEST_F(VectorCastTest, CastStringToFloat) {
  exec::ExecutionSettings exec_settings{};

  // a = [NULL, "-123.45", "6.75", NULL, "0.8", "910"]
  auto a = MakeVarcharVector({{}, "-123.45", "6.75", {}, "0.8", "910"}, {true, false, false, true, false, false});

  EXPECT_NO_THROW(a->Cast(exec_settings, TypeId::Float));

  EXPECT_EQ(TypeId::Float, a->GetTypeId());
  EXPECT_TRUE(a->IsNull(0));
  EXPECT_EQ(GenericValue::CreateFloat(-123.45), a->GetValue(1));
  EXPECT_EQ(GenericValue::CreateFloat(6.75), a->GetValue(2));
  EXPECT_TRUE(a->IsNull(3));
  EXPECT_EQ(GenericValue::CreateFloat(0.8), a->GetValue(4));
  EXPECT_EQ(GenericValue::CreateFloat(910), a->GetValue(5));
}

// NOLINTNEXTLINE
TEST_F(VectorCastTest, CastStringToDouble) {
  exec::ExecutionSettings exec_settings{};

  // a = [NULL, "-123.45", "6.75", NULL, "0.8", "910"]
  auto a = MakeVarcharVector({{}, "-123.45", "6.75", {}, "0.8", "910"}, {true, false, false, true, false, false});

  EXPECT_NO_THROW(a->Cast(exec_settings, TypeId::Double));

  EXPECT_EQ(TypeId::Double, a->GetTypeId());
  EXPECT_TRUE(a->IsNull(0));
  EXPECT_EQ(GenericValue::CreateDouble(-123.45), a->GetValue(1));
  EXPECT_EQ(GenericValue::CreateDouble(6.75), a->GetValue(2));
  EXPECT_TRUE(a->IsNull(3));
  EXPECT_EQ(GenericValue::CreateDouble(0.8), a->GetValue(4));
  EXPECT_EQ(GenericValue::CreateDouble(910), a->GetValue(5));
}

// NOLINTNEXTLINE
TEST_F(VectorCastTest, CastStringToFloatParseError) {
  exec::ExecutionSettings exec_settings{};

  // a = [NULL, "-123.45", "6.75E", NULL, "0.8", "910"]
  auto a = MakeVarcharVector({{}, "-123.45", "6.75E", {}, "0.8", "910"}, {true, false, false, true, false, false});

  // Casting should fail because "6.75E" is not a valid number
  EXPECT_THROW(a->Cast(exec_settings, TypeId::Float), ExecutionException);
}

}  // namespace noisepage::execution::sql::test
