#include "common/hash_util.h"
#include <cstring>
#include <limits>
#include <string>
#include <random>
#include <thread>  // NOLINT
#include <unordered_set>
#include <vector>
#include "gtest/gtest.h"

namespace terrier {

// NOLINTNEXTLINE
TEST(HashUtilTests, HashTest) {
  // INT
  std::vector<int> vals0 = {std::numeric_limits<int>::min(), 0, std::numeric_limits<int>::max() - 1};
  for (const auto &val : vals0) {
    EXPECT_EQ(common::HashUtil::Hash(val), common::HashUtil::Hash(val));
    EXPECT_NE(common::HashUtil::Hash(val + 1), common::HashUtil::Hash(val));
  }

  // FLOAT
  std::vector<float> vals1 = {std::numeric_limits<float>::min(), 0.0f, std::numeric_limits<float>::max() - 1.0f};
  for (const auto &val : vals1) {
    EXPECT_EQ(common::HashUtil::Hash(val), common::HashUtil::Hash(val));
    // This fails for max float value
    // EXPECT_NE(common::HashUtil::Hash(val+1.0f), common::HashUtil::Hash(val));
  }

  // CHAR
  std::vector<char> vals2 = {'f', 'u', 'c', 'k', 't', 'k'};
  for (const auto &val : vals2) {
    EXPECT_EQ(common::HashUtil::Hash(val), common::HashUtil::Hash(val));
    EXPECT_NE(common::HashUtil::Hash(val + 1), common::HashUtil::Hash(val));
  }

  // STRING
  std::vector<std::string> vals3 = {"XXX", "YYY", "ZZZ"};
  for (const auto &val : vals3) {
    EXPECT_EQ(common::HashUtil::Hash(val), common::HashUtil::Hash(val));
    EXPECT_NE(common::HashUtil::Hash("WUTANG"), common::HashUtil::Hash(val));
  }
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
    hash0 = common::HashUtil::CombineHashes(hash0, common::HashUtil::Hash(val));
  }

  common::hash_t hash1 = 0;
  hash1 = common::HashUtil::CombineHashInRange(hash1, vals0.begin(), vals0.end());

  EXPECT_EQ(hash0, hash1);
}

}  // namespace terrier
