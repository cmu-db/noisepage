#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/sql_test.h"

namespace noisepage::execution::sql::test {

class VectorFillTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(VectorFillTest, SimpleNonNull) {
  // Fill a vector with the given type with the given value of that type
#define CHECK_SIMPLE_FILL(TYPE, FILL_VALUE)                             \
  {                                                                     \
    auto vec = Make##TYPE##Vector(10);                                  \
    VectorOps::Fill(vec.get(), GenericValue::Create##TYPE(FILL_VALUE)); \
    for (uint64_t i = 0; i < vec->GetSize(); i++) {                     \
      auto val = vec->GetValue(i);                                      \
      EXPECT_FALSE(val.IsNull());                                       \
      EXPECT_EQ(GenericValue::Create##TYPE(FILL_VALUE), val);           \
    }                                                                   \
  }

  CHECK_SIMPLE_FILL(Boolean, true);
  CHECK_SIMPLE_FILL(TinyInt, int8_t{-24});
  CHECK_SIMPLE_FILL(SmallInt, int16_t{47});
  CHECK_SIMPLE_FILL(Integer, int32_t{1234});
  CHECK_SIMPLE_FILL(BigInt, int64_t{-24987});
  CHECK_SIMPLE_FILL(Float, float{-3.10});
  CHECK_SIMPLE_FILL(Double, double{-3.14});
  CHECK_SIMPLE_FILL(Varchar, "P-Money In The Bank");
#undef CHECK_SIMPLE_FILL
}

// NOLINTNEXTLINE
TEST_F(VectorFillTest, NullValue) {
  // Fill with a NULL value, ensure the whole vector is filled with NULLs
  auto vec = MakeIntegerVector(10);
  VectorOps::Fill(vec.get(), GenericValue::CreateNull(vec->GetTypeId()));

  for (uint64_t i = 0; i < vec->GetCount(); i++) {
    EXPECT_TRUE(vec->IsNull(i));
  }
}

// NOLINTNEXTLINE
TEST_F(VectorFillTest, ExplicitNull) {
  // Fill a vector with the given type with the given value of that type
#define CHECK_SIMPLE_FILL(TYPE)                     \
  {                                                 \
    auto vec = Make##TYPE##Vector(10);              \
    VectorOps::FillNull(vec.get());                 \
    for (uint64_t i = 0; i < vec->GetSize(); i++) { \
      auto val = vec->GetValue(i);                  \
      EXPECT_TRUE(val.IsNull());                    \
    }                                               \
  }

  CHECK_SIMPLE_FILL(Boolean);
  CHECK_SIMPLE_FILL(TinyInt);
  CHECK_SIMPLE_FILL(SmallInt);
  CHECK_SIMPLE_FILL(Integer);
  CHECK_SIMPLE_FILL(BigInt);
  CHECK_SIMPLE_FILL(Float);
  CHECK_SIMPLE_FILL(Double);
  CHECK_SIMPLE_FILL(Varchar);
#undef CHECK_SIMPLE_FILL
}

}  // namespace noisepage::execution::sql::test
