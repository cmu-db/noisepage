#include "common/hash_util.h"

#include <cstring>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <thread>  // NOLINT
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

namespace noisepage {

// NOLINTNEXTLINE
TEST(HashUtilTests, HashTest) {
  // INT
  std::vector<int> vals0 = {std::numeric_limits<int>::min(), 0, std::numeric_limits<int>::max() - 1};
  for (const auto &val : vals0) {
    auto copy = val;  // NOLINT
    EXPECT_EQ(common::HashUtil::Hash(val), common::HashUtil::Hash(copy));
    EXPECT_NE(common::HashUtil::Hash(val + 1), common::HashUtil::Hash(val));
  }

  // FLOAT
  std::vector<float> vals1 = {std::numeric_limits<float>::min(), 0.0F, std::numeric_limits<float>::max() - 1.0F};
  for (const auto &val : vals1) {
    auto copy = val;  // NOLINT
    EXPECT_EQ(common::HashUtil::Hash(val), common::HashUtil::Hash(copy));
    // This fails for max float value
    // EXPECT_NE(common::HashUtil::Hash(val+1.0f), common::HashUtil::Hash(val));
  }

  // CHAR
  std::vector<char> vals2 = {'f', 'u', 'c', 'k', 't', 'k'};
  for (const auto &val : vals2) {
    auto copy = val;  // NOLINT
    EXPECT_EQ(common::HashUtil::Hash(val), common::HashUtil::Hash(copy));
    EXPECT_NE(common::HashUtil::Hash(val + 1), common::HashUtil::Hash(val));
  }

  // STRING
  std::vector<std::string> vals3 = {"XXX", "YYY", "ZZZ"};
  for (const auto &val : vals3) {
    auto copy = val;  // NOLINT
    EXPECT_EQ(common::HashUtil::Hash(val), common::HashUtil::Hash(copy));
    EXPECT_NE(common::HashUtil::Hash("WUTANG"), common::HashUtil::Hash(val));
  }
}

// NOLINTNEXTLINE
TEST(HashUtilTests, HashStringsTest) {
  const std::string_view val = "ABCXYZ";
  // EXPECT_EQ(common::HashUtil::Hash(val), common::HashUtil::Hash(val));
  common::hash_t hash0 = common::HashUtil::Hash(val);
  common::hash_t hash1 = common::HashUtil::Hash("ABCXYZ");
  EXPECT_EQ(hash0, hash1);
}

// NOLINTNEXTLINE
TEST(HashUtilTests, HashMixedTest) {
  // There is nothing special about this test. It's just a sanity
  // check for me to make sure that things are working correctly in
  // another part of the system.
  enum class Wutang { RZA, GZA, RAEKWON, METHODMAN, GHOSTFACE, ODB, INSPECTAH };

  common::hash_t hash0 = common::HashUtil::Hash(Wutang::RAEKWON);
  common::hash_t hash1 = common::HashUtil::Hash(Wutang::RAEKWON);
  EXPECT_EQ(hash0, hash1);

  Wutang val0 = Wutang::ODB;
  hash0 = common::HashUtil::CombineHashes(hash0, common::HashUtil::Hash(val0));
  hash1 = common::HashUtil::CombineHashes(hash1, common::HashUtil::Hash(val0));
  EXPECT_EQ(hash0, hash1);

  std::string val1 = "protect-yo-neck.csv";
  hash0 = common::HashUtil::CombineHashes(hash0, common::HashUtil::Hash(val1));
  hash1 = common::HashUtil::CombineHashes(hash1, common::HashUtil::Hash(val1));
  EXPECT_EQ(hash0, hash1);

  char val2 = ',';
  hash0 = common::HashUtil::CombineHashes(hash0, common::HashUtil::Hash(val2));
  hash1 = common::HashUtil::CombineHashes(hash1, common::HashUtil::Hash(val2));
  EXPECT_EQ(hash0, hash1);
}

// NOLINTNEXTLINE
TEST(HashUtilTests, CombineHashesTest) {
  // INT
  std::vector<int> vals0 = {0, 1, 1 << 20};
  common::hash_t hash0 = 0;
  common::hash_t hash1 = 0;
  for (const auto &val : vals0) {
    hash0 = common::HashUtil::CombineHashes(hash0, common::HashUtil::Hash(val));
    hash1 = common::HashUtil::CombineHashes(hash1, common::HashUtil::Hash(val));
    EXPECT_EQ(hash0, hash1);
  }

  // STRING
  std::vector<std::string> vals1 = {"XXX", "YYY", "ZZZ"};
  for (const auto &val : vals1) {
    hash0 = common::HashUtil::CombineHashes(hash0, common::HashUtil::Hash(val));
    hash1 = common::HashUtil::CombineHashes(hash1, common::HashUtil::Hash(val));
    EXPECT_EQ(hash0, hash1);
  }

  // MIXED
  ASSERT_EQ(vals0.size(), vals1.size());
  for (int i = 0; i < static_cast<int>(vals0.size()); i++) {
    hash0 = common::HashUtil::CombineHashes(hash0, common::HashUtil::Hash(vals0[i]));
    hash1 = common::HashUtil::CombineHashes(hash1, common::HashUtil::Hash(vals0[i]));
    EXPECT_EQ(hash0, hash1);

    hash0 = common::HashUtil::CombineHashes(hash0, common::HashUtil::Hash(vals1[i]));
    hash1 = common::HashUtil::CombineHashes(hash1, common::HashUtil::Hash(vals1[i]));
    EXPECT_EQ(hash0, hash1);
  }
}

// NOLINTNEXTLINE
TEST(HashUtilTests, CombineHashInRangeTest) {
  std::vector<std::string> vals0 = {"XXX", "YYY", "ZZZ"};
  common::hash_t hash0 = 0;
  for (const auto &val : vals0) {
    auto copy = val;  // NOLINT
    hash0 = common::HashUtil::CombineHashes(hash0, common::HashUtil::Hash(copy));
  }

  common::hash_t hash1 = 0;
  hash1 = common::HashUtil::CombineHashInRange(hash1, vals0.begin(), vals0.end());

  EXPECT_EQ(hash0, hash1);
}

// NOLINTNEXTLINE
TEST(HashUtilTests, SumHashesTest) {
  common::hash_t hash0 = common::HashUtil::Hash("ABC");
  common::hash_t hash1 = common::HashUtil::Hash("XYZ");

  common::hash_t combined0 = common::HashUtil::SumHashes(hash0, hash1);
  common::hash_t combined1 = common::HashUtil::SumHashes(hash1, hash0);

  EXPECT_EQ(combined0, combined1);
}

}  // namespace noisepage
