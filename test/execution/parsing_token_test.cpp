#include <functional>

#include "execution/parsing/scanner.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::parsing::test {

class TokenTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(TokenTest, ComparisonOpTest) {
  EXPECT_FALSE(Token::IsCompareOp(Token::Type::PLUS));
  EXPECT_FALSE(Token::IsCompareOp(Token::Type::MINUS));
  EXPECT_FALSE(Token::IsCompareOp(Token::Type::AND));
  EXPECT_FALSE(Token::IsCompareOp(Token::Type::PERCENT));
  EXPECT_FALSE(Token::IsCompareOp(Token::Type::OR));

  // These should be valid
  EXPECT_TRUE(Token::IsCompareOp(Token::Type::EQUAL_EQUAL));
  EXPECT_TRUE(Token::IsCompareOp(Token::Type::GREATER));
  EXPECT_TRUE(Token::IsCompareOp(Token::Type::GREATER_EQUAL));
  EXPECT_TRUE(Token::IsCompareOp(Token::Type::BANG_EQUAL));
  EXPECT_TRUE(Token::IsCompareOp(Token::Type::LESS));
  EXPECT_TRUE(Token::IsCompareOp(Token::Type::LESS_EQUAL));
}

}  // namespace noisepage::execution::parsing::test
