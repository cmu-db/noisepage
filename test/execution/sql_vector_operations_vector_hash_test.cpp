#include <random>

#include "execution/sql/operators/hash_operators.h"
#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/sql_test.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class VectorHashTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(VectorHashTest, NumericHashes) {
#define GEN_HASH_TEST(TYPE_ID, CPP_TYPE)                                                                            \
  {                                                                                                                 \
    auto input = Make##TYPE_ID##Vector(257);                                                                        \
    auto hashes = Vector(TypeId::Hash, true, false);                                                                \
    /* Fill input */                                                                                                \
    std::random_device r;                                                                                           \
    for (uint64_t i = 0; i < input->GetSize(); i++) {                                                               \
      input->SetValue(i, GenericValue::Create##TYPE_ID(r()));                                                       \
    }                                                                                                               \
    /* Hash */                                                                                                      \
    VectorOps::Hash(*input, &hashes);                                                                               \
    EXPECT_EQ(input->GetSize(), hashes.GetSize());                                                                  \
    EXPECT_EQ(input->GetCount(), hashes.GetCount());                                                                \
    EXPECT_EQ(nullptr, hashes.GetFilteredTupleIdList());                                                            \
    /* Check output */                                                                                              \
    auto raw_input = reinterpret_cast<CPP_TYPE *>(input->GetData());                                                \
    VectorOps::Exec(*input, [&](uint64_t i, uint64_t k) {                                                           \
      EXPECT_EQ(reinterpret_cast<hash_t *>(hashes.GetData())[i], Hash<CPP_TYPE>{}(raw_input[i], input->IsNull(i))); \
    });                                                                                                             \
  }

  GEN_HASH_TEST(TinyInt, int8_t);
  GEN_HASH_TEST(SmallInt, int16_t);
  GEN_HASH_TEST(Integer, int32_t);
  GEN_HASH_TEST(BigInt, int64_t);
  GEN_HASH_TEST(Float, float);
  GEN_HASH_TEST(Double, double);

#undef GEN_HASH_TEST
}

// NOLINTNEXTLINE
TEST_F(VectorHashTest, HashWithNullInput) {
  // input = [2001-01-01, 2002-01-01, NULL, 2004-01-01, NULL]
  auto input = MakeDateVector({Date::FromYMD(2001, 01, 01), Date::FromYMD(2002, 01, 01), Date::FromYMD(2003, 01, 01),
                               Date::FromYMD(2004, 01, 01), Date::FromYMD(2005, 01, 01)},
                              {false, false, true, false, true});
  auto hash = Vector(TypeId::Hash, true, false);

  VectorOps::Hash(*input, &hash);

  EXPECT_EQ(input->GetSize(), hash.GetSize());
  EXPECT_EQ(input->GetCount(), hash.GetCount());
  EXPECT_EQ(nullptr, hash.GetFilteredTupleIdList());

  auto raw_input = reinterpret_cast<Date *>(input->GetData());
  auto raw_hash = reinterpret_cast<hash_t *>(hash.GetData());
  EXPECT_EQ(Hash<Date>{}(raw_input[0], input->IsNull(0)), raw_hash[0]);
  EXPECT_EQ(Hash<Date>{}(raw_input[1], input->IsNull(1)), raw_hash[1]);
  EXPECT_EQ(Hash<Date>{}(raw_input[2], input->IsNull(2)), raw_hash[2]);
  EXPECT_EQ(hash_t{0}, raw_hash[2]);  // The second element is NULL, so hash=0.
  EXPECT_EQ(Hash<Date>{}(raw_input[3], input->IsNull(3)), raw_hash[3]);
  EXPECT_EQ(Hash<Date>{}(raw_input[4], input->IsNull(4)), raw_hash[4]);
  EXPECT_EQ(hash_t{0}, raw_hash[4]);  // The last element is NULL, so hash=0.
}

// NOLINTNEXTLINE
TEST_F(VectorHashTest, StringHash) {
  // input = [s, NULL, s, s]
  const char *refs[] = {"short", "medium sized", "quite long indeed, but why, so?",
                        "I'm trying to right my wrongs, but it's funny, them same wrongs help me write this song"};
  auto input = MakeVarcharVector({refs[0], refs[1], refs[2], refs[3]}, {false, true, false, false});
  auto hash = MakeVector(TypeId::Hash, input->GetSize());

  VectorOps::Hash(*input, hash.get());

  EXPECT_EQ(input->GetSize(), hash->GetSize());
  EXPECT_EQ(input->GetCount(), hash->GetCount());
  EXPECT_EQ(nullptr, hash->GetFilteredTupleIdList());

  auto raw_input = reinterpret_cast<const storage::VarlenEntry *>(input->GetData());
  auto raw_hash = reinterpret_cast<hash_t *>(hash->GetData());
  EXPECT_EQ(Hash<storage::VarlenEntry>{}(raw_input[0], input->IsNull(0)), raw_hash[0]);
  EXPECT_EQ(Hash<storage::VarlenEntry>{}(raw_input[1], input->IsNull(1)), raw_hash[1]);
  EXPECT_EQ(hash_t{0}, raw_hash[1]);  // The second element is NULL, so hash=0.
  EXPECT_EQ(Hash<storage::VarlenEntry>{}(raw_input[2], input->IsNull(2)), raw_hash[2]);
  EXPECT_EQ(Hash<storage::VarlenEntry>{}(raw_input[3], input->IsNull(3)), raw_hash[3]);
}

}  // namespace noisepage::execution::sql::test
