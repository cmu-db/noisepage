#include <random>
#include <string>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::util::test {

class HashTest : public TplTest {};

// Check an input value using a given hashing method
#define CHECK_HASH_METHOD_ON_INPUT(METHOD, INPUT)         \
  {                                                       \
    auto hash_val1 = common::HashUtil::Hash(INPUT);       \
    auto hash_val2 = common::HashUtil::Hash(INPUT);       \
    EXPECT_EQ(hash_val1, hash_val2);                      \
  }                                                       \
  {                                                       \
    auto seed = random_seed();                            \
    auto hash_val1 = common::HashUtil::Hash(INPUT, seed); \
    auto hash_val2 = common::HashUtil::Hash(INPUT, seed); \
    EXPECT_EQ(hash_val1, hash_val2);                      \
  }

// Check an input value against all possible hashing methods
#define CHECK_HASH_ON_INPUT(INPUT)                          \
  {                                                         \
    CHECK_HASH_METHOD_ON_INPUT(HashMethod::Crc, INPUT);     \
    CHECK_HASH_METHOD_ON_INPUT(HashMethod::Murmur, INPUT);  \
    CHECK_HASH_METHOD_ON_INPUT(HashMethod::xxHash3, INPUT); \
  }

// NOLINTNEXTLINE
TEST_F(HashTest, IntegerHash) {
  std::random_device random_seed;

  CHECK_HASH_ON_INPUT(int8_t{-1});
  CHECK_HASH_ON_INPUT(int16_t{-22});
  CHECK_HASH_ON_INPUT(int32_t{-333});
  CHECK_HASH_ON_INPUT(int64_t{-4444});
  CHECK_HASH_ON_INPUT(uint8_t{1});
  CHECK_HASH_ON_INPUT(uint16_t{22});
  CHECK_HASH_ON_INPUT(uint32_t{333});
  CHECK_HASH_ON_INPUT(uint64_t{444});
}

// NOLINTNEXTLINE
TEST_F(HashTest, FloatingPointHash) {
  std::random_device random_seed;

  CHECK_HASH_ON_INPUT(float{0});
  CHECK_HASH_ON_INPUT(float{-213.89});
  CHECK_HASH_ON_INPUT(double{0});
  CHECK_HASH_ON_INPUT(double{230984.234});
  CHECK_HASH_ON_INPUT(double{-230984.234});
}

#undef CHECK_HASH_ON_INPUT
#undef CHECK_HASH_METHOD_ON_INPUT

// NOLINTNEXTLINE
TEST_F(HashTest, StringHash) {
  const auto small_input = "This is a kinda long string";
  auto large_input = std::string();
  for (uint32_t i = 0; i < 40; i++) {
    large_input += small_input;
  }

#define CHECK_HASH_ON_INPUT(INPUT)                  \
  {                                                 \
    auto hash_val1 = common::HashUtil::Hash(INPUT); \
    auto hash_val2 = common::HashUtil::Hash(INPUT); \
    EXPECT_EQ(hash_val1, hash_val2);                \
  }

  CHECK_HASH_ON_INPUT("Fixed input");
  CHECK_HASH_ON_INPUT(small_input);
  CHECK_HASH_ON_INPUT(large_input);

#undef CHECK_HASH_ON_INPUT
}

}  // namespace noisepage::execution::util::test
