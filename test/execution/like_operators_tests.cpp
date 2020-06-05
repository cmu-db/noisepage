#include <string>

#include "execution/sql/operators/like_operators.h"
#include "execution/tpl_test.h"

namespace terrier::execution::sql {

class LikeOperatorsTests : public TplTest {};

TEST_F(LikeOperatorsTests, ShortString) {
// 'abc' LIKE 'a' = false
std::string s = "abc";
std::string p = "a";
EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));

// 'abc' LIKE 'ab' = false
s = "abc";
p = "ab";
EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));

// 'abc' LIKE 'axc' = false
s = "abc";
p = "axc";
EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));

// 'abc' LIKE 'abc' = true
s = "abc";
p = "abc";
EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));
}

TEST_F(LikeOperatorsTests, SingleCharacter_Wildcard) {
// Character after single-character wildcard doesn't match
std::string s = "forbes \\avenue";
std::string p = "forb_ \\\\venue";
EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));

// Now it does
p = "%b_s \\\\avenue";
EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));

// N '_'s must match N characters

// mismatched 'M'
s = "P Money";
p = "__money";
EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));

// Match
p = "__Money";
EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));

// Match
p = "__M___y";
EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));
}

TEST_F(LikeOperatorsTests, MultiCharacter_Wildcard) {
// Must consume all '%'
std::string s = "Money In The Bank";
std::string p = "_%%%%%%_";
EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));

p = "_%%%%%%%%%";
EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));

// Consume all '%', but also match last non-wildcard character
p = "_%%%%%%%%x";
EXPECT_FALSE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));

p = "_%%%%%%%%k";
EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));

// Pattern ending in escape
s = "Money In The Bank\\";
p = "_%%%%%%%%k\\\\";
EXPECT_TRUE(Like{}(storage::VarlenEntry::Create(s), storage::VarlenEntry::Create(p)));
}

}  // namespace terrier::execution::sql
