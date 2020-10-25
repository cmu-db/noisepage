#include <string>

#include "execution/sql/operators/like_operators.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class LikeOperatorsTests : public TplTest {};

// NOLINTNEXTLINE
TEST_F(LikeOperatorsTests, ShortString) {
  // 'abc' LIKE 'a' = false
  std::string s = "abc";
  std::string p = "a";
  EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT

  // 'abc' LIKE 'ab' = false
  s = "abc";
  p = "ab";
  EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT

  // 'abc' LIKE 'axc' = false
  s = "abc";
  p = "axc";
  EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT

  // 'abc' LIKE 'abc' = true
  s = "abc";
  p = "abc";
  EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT
}

// NOLINTNEXTLINE
TEST_F(LikeOperatorsTests, SingleCharacter_Wildcard) {
  // Character after single-character wildcard doesn't match
  std::string s = "forbes \\avenue";
  std::string p = "forb_ \\\\venue";
  EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT

  // Now it does
  p = "%b_s \\\\avenue";
  EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT

  // N '_'s must match N characters

  // mismatched 'M'
  s = "P Money";
  p = "__money";
  EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT

  // Match
  p = "__Money";
  EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT

  // Match
  p = "__M___y";
  EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT
}

// NOLINTNEXTLINE
TEST_F(LikeOperatorsTests, MultiCharacter_Wildcard) {
  // Must consume all '%'
  std::string s = "Money In The Bank";
  std::string p = "_%%%%%%_";
  EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT

  p = "_%%%%%%%%%";
  EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT

  // Consume all '%', but also match last non-wildcard character
  p = "_%%%%%%%%x";
  EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT

  p = "_%%%%%%%%k";
  EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT

  // Pattern ending in escape
  s = "Money In The Bank\\";
  p = "_%%%%%%%%k\\\\";
  EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));  // NOLINT
}

}  // namespace noisepage::execution::sql::test
