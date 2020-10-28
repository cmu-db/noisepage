#include "execution/sql/constant_vector.h"
#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/sql_test.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class VectorBitwiseTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(VectorBitwiseTest, InPlaceBitwiseAND) {
  exec::ExecutionSettings exec_settings{};

#define GEN_CASE(TYPE, CPP_TYPE)                                                                         \
  {                                                                                                      \
    auto a = Make##TYPE##Vector(100);                                                                    \
    VectorOps::Generate(a.get(), 0, 2);                                                                  \
    VectorOps::BitwiseAndInPlace(exec_settings, a.get(), ConstantVector(GenericValue::Create##TYPE(3))); \
    EXPECT_EQ(100, a->GetSize());                                                                        \
    EXPECT_EQ(100, a->GetCount());                                                                       \
    EXPECT_EQ(nullptr, a->GetFilteredTupleIdList());                                                     \
    auto *a_data = reinterpret_cast<CPP_TYPE *>(a->GetData());                                           \
    for (uint64_t i = 0; i < a->GetCount(); i++) {                                                       \
      EXPECT_FALSE(a->IsNull(i));                                                                        \
      EXPECT_LE(a_data[i], 3);                                                                           \
    }                                                                                                    \
  }

  GEN_CASE(TinyInt, int8_t);
  GEN_CASE(SmallInt, int16_t);
  GEN_CASE(Integer, int32_t);
  GEN_CASE(BigInt, int64_t);
  GEN_CASE(Pointer, uintptr_t);
#undef GEN_CASE
}

}  // namespace noisepage::execution::sql::test
