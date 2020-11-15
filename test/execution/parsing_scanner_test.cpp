#include <algorithm>
#include <functional>
#include <string>
#include <vector>

#include "execution/parsing/scanner.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::parsing::test {

class ScannerTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(ScannerTest, EmptySourceTest) {
  Scanner scanner("", 0);
  for (unsigned i = 0; i < 10; i++) {
    EXPECT_EQ(Token::Type::EOS, scanner.Next());
  }
}

// NOLINTNEXTLINE
TEST_F(ScannerTest, SimpleSourceTest) {
  const std::string source = "var x = 10";
  Scanner scanner(source.data(), source.length());

  // 'var'
  EXPECT_EQ(Token::Type::VAR, scanner.Next());

  // 'x'
  EXPECT_EQ(Token::Type::IDENTIFIER, scanner.Peek());
  EXPECT_EQ(Token::Type::IDENTIFIER, scanner.Next());
  EXPECT_EQ("x", scanner.CurrentLiteral());
  EXPECT_EQ(1u, scanner.CurrentPosition().line_);

  // The following "+ 1" is because source positions are 1-based
  EXPECT_EQ(source.find('x') + 1, scanner.CurrentPosition().column_);

  // '='
  EXPECT_EQ(Token::Type::EQUAL, scanner.Peek());
  EXPECT_EQ(Token::Type::EQUAL, scanner.Next());

  // '10'
  EXPECT_EQ(Token::Type::INTEGER, scanner.Peek());
  EXPECT_EQ(Token::Type::INTEGER, scanner.Next());

  // Done
  EXPECT_EQ(Token::Type::EOS, scanner.Peek());
  EXPECT_EQ(Token::Type::EOS, scanner.Next());
}

namespace {

struct TestCase {
  const std::string source_;
  std::vector<Token::Type> expected_tokens_;
  std::function<void(Scanner *scanner, uint32_t token_idx)> check_;
};

}  // namespace

void CheckEquality(uint32_t test_idx, const std::vector<Token::Type> &expected,
                   const std::vector<Token::Type> &actual) {
  EXPECT_EQ(expected.size(), actual.size());
  for (unsigned i = 0; i < std::min(actual.size(), expected.size()); i++) {
    if (expected[i] != actual[i]) {
      EXPECT_EQ(expected[i], actual[i]) << "Test " << test_idx << ": expected token type '"
                                        << Token::GetString(expected[i]) << "' at pos " << i << " but got '"
                                        << Token::GetString(actual[i]) << "'";
    }
  }
}

void RunTests(const std::vector<TestCase> &tests) {
  for (unsigned test_idx = 0; test_idx < tests.size(); test_idx++) {
    const auto &test = tests[test_idx];
    Scanner scanner(test.source_.data(), test.source_.length());

    std::vector<Token::Type> actual;

    uint32_t token_idx = 0;
    for (auto token = scanner.Next(); token != Token::Type::EOS; token = scanner.Next(), token_idx++) {
      actual.push_back(token);

      if (test.check_ != nullptr) {
        test.check_(&scanner, token_idx);
      }
    }

    // Expect final sizes should be the same
    CheckEquality(test_idx, test.expected_tokens_, actual);
  }
}

// NOLINTNEXTLINE
TEST_F(ScannerTest, VariableSyntaxTest) {
  std::vector<TestCase> tests = {
      // Variable with no type
      {"var x = 10",
       {Token::Type::VAR, Token::Type::IDENTIFIER, Token::Type::EQUAL, Token::Type::INTEGER},
       [](Scanner *scanner, uint32_t token_idx) {
         if (token_idx == 1) {
           EXPECT_EQ("x", scanner->CurrentLiteral());
         } else if (token_idx == 3) {
           EXPECT_EQ("10", scanner->CurrentLiteral());
         }
       }},

      // Variable with type
      {"var x:int32_t = 10",
       {Token::Type::VAR, Token::Type::IDENTIFIER, Token::Type::COLON, Token::Type::IDENTIFIER, Token::Type::EQUAL,
        Token::Type::INTEGER},
       [](Scanner *scanner, uint32_t token_idx) {
         if (token_idx == 3) {
           EXPECT_EQ("int32_t", scanner->CurrentLiteral());
         }
       }},
      // Variable with float number
      {"var x = 10.123", {Token::Type::VAR, Token::Type::IDENTIFIER, Token::Type::EQUAL, Token::Type::FLOAT}, nullptr}};

  RunTests(tests);
}

// NOLINTNEXTLINE
TEST_F(ScannerTest, IfSyntaxTest) {
  std::vector<TestCase> tests = {
      {"if (x == 0) { }",
       {Token::Type::IF, Token::Type::LEFT_PAREN, Token::Type::IDENTIFIER, Token::Type::EQUAL_EQUAL,
        Token::Type::INTEGER, Token::Type::RIGHT_PAREN, Token::Type::LEFT_BRACE, Token::Type::RIGHT_BRACE}},
  };

  RunTests(tests);
}

// NOLINTNEXTLINE
TEST_F(ScannerTest, ForSyntaxTest) {
  std::vector<TestCase> tests = {
      // Simple for
      {"for () {}",
       {Token::Type::FOR, Token::Type::LEFT_PAREN, Token::Type::RIGHT_PAREN, Token::Type::LEFT_BRACE,
        Token::Type::RIGHT_BRACE},
       nullptr},

      // For as a while loop
      {"for (i < 10) {}",
       {Token::Type::FOR, Token::Type::LEFT_PAREN, Token::Type::IDENTIFIER, Token::Type::LESS, Token::Type::INTEGER,
        Token::Type::RIGHT_PAREN, Token::Type::LEFT_BRACE, Token::Type::RIGHT_BRACE},
       [](Scanner *scanner, uint32_t token_idx) {
         // Check that the fourth token is the number "10"
         if (token_idx == 4) {
           EXPECT_EQ(Token::Type::INTEGER, scanner->CurrentToken());
           EXPECT_EQ("10", scanner->CurrentLiteral());
         }
       }},

      // For as a while loop with simple body
      {"for (i < 10) { println(\"hi\") }",
       {Token::Type::FOR, Token::Type::LEFT_PAREN, Token::Type::IDENTIFIER, Token::Type::LESS, Token::Type::INTEGER,
        Token::Type::RIGHT_PAREN, Token::Type::LEFT_BRACE, Token::Type::IDENTIFIER, Token::Type::LEFT_PAREN,
        Token::Type::STRING, Token::Type::RIGHT_PAREN, Token::Type::RIGHT_BRACE},
       [](Scanner *scanner, uint32_t token_idx) {
         // Check that the fourth token is the number "10"
         if (token_idx == 9) {
           EXPECT_EQ(Token::Type::STRING, scanner->CurrentToken());
           EXPECT_EQ("hi", scanner->CurrentLiteral());
         }
       }},

      // Full blown for loop
      {"for (var x = 0; x < 10; x = x + 1) {}",
       {Token::Type::FOR, Token::Type::LEFT_PAREN, Token::Type::VAR, Token::Type::IDENTIFIER, Token::Type::EQUAL,
        Token::Type::INTEGER, Token::Type::SEMI, Token::Type::IDENTIFIER, Token::Type::LESS, Token::Type::INTEGER,
        Token::Type::SEMI, Token::Type::IDENTIFIER, Token::Type::EQUAL, Token::Type::IDENTIFIER, Token::Type::PLUS,
        Token::Type::INTEGER, Token::Type::RIGHT_PAREN, Token::Type::LEFT_BRACE, Token::Type::RIGHT_BRACE},
       nullptr},
  };

  RunTests(tests);
}

// NOLINTNEXTLINE
TEST_F(ScannerTest, FunctionSyntaxTest) {
  std::vector<TestCase> tests = {
      // Empty function test
      {"fun test(){}",
       {Token::Type::FUN, Token::Type::IDENTIFIER, Token::Type::LEFT_PAREN, Token::Type::RIGHT_PAREN,
        Token::Type::LEFT_BRACE, Token::Type::RIGHT_BRACE},
       [](Scanner *scanner, uint32_t token_idx) {
         if (token_idx == 1) {
           EXPECT_EQ("test", scanner->CurrentLiteral());
         }
       }},

      // Variable with type
      {"fun test(a:int32_t){}",
       {Token::Type::FUN, Token::Type::IDENTIFIER, Token::Type::LEFT_PAREN, Token::Type::IDENTIFIER, Token::Type::COLON,
        Token::Type::IDENTIFIER, Token::Type::RIGHT_PAREN, Token::Type::LEFT_BRACE, Token::Type::RIGHT_BRACE},
       [](Scanner *scanner, uint32_t token_idx) {
         if (token_idx == 3) {
           EXPECT_EQ("a", scanner->CurrentLiteral());
         }
       }}};

  RunTests(tests);
}

// NOLINTNEXTLINE
TEST_F(ScannerTest, UnaryOpSyntaxTest) {
  std::vector<TestCase> tests = {
      // Empty function test
      {"fun test(){ return -1 }",
       {Token::Type::FUN, Token::Type::IDENTIFIER, Token::Type::LEFT_PAREN, Token::Type::RIGHT_PAREN,
        Token::Type::LEFT_BRACE, Token::Type::RETURN, Token::Type::MINUS, Token::Type::INTEGER,
        Token::Type::RIGHT_BRACE}},
      {"fun test(){ return !false }",
       {Token::Type::FUN, Token::Type::IDENTIFIER, Token::Type::LEFT_PAREN, Token::Type::RIGHT_PAREN,
        Token::Type::LEFT_BRACE, Token::Type::RETURN, Token::Type::BANG, Token::Type::FALSE, Token::Type::RIGHT_BRACE}},
  };

  RunTests(tests);
}

// NOLINTNEXTLINE
TEST_F(ScannerTest, BinOpSyntaxTest) {
  std::vector<TestCase> tests = {
      // Empty function test
      {"fun test(){ return 1 & 2 }",
       {Token::Type::FUN, Token::Type::IDENTIFIER, Token::Type::LEFT_PAREN, Token::Type::RIGHT_PAREN,
        Token::Type::LEFT_BRACE, Token::Type::RETURN, Token::Type::INTEGER, Token::Type::AMPERSAND,
        Token::Type::INTEGER, Token::Type::RIGHT_BRACE}},
      {"fun test(){ return 1 ^ 2 }",
       {Token::Type::FUN, Token::Type::IDENTIFIER, Token::Type::LEFT_PAREN, Token::Type::RIGHT_PAREN,
        Token::Type::LEFT_BRACE, Token::Type::RETURN, Token::Type::INTEGER, Token::Type::BIT_XOR, Token::Type::INTEGER,
        Token::Type::RIGHT_BRACE}},
      {"fun test(){ return 1 | 2 }",
       {Token::Type::FUN, Token::Type::IDENTIFIER, Token::Type::LEFT_PAREN, Token::Type::RIGHT_PAREN,
        Token::Type::LEFT_BRACE, Token::Type::RETURN, Token::Type::INTEGER, Token::Type::BIT_OR, Token::Type::INTEGER,
        Token::Type::RIGHT_BRACE}},
  };

  RunTests(tests);
}

// NOLINTNEXTLINE
TEST_F(ScannerTest, CommentTest) {
  std::vector<TestCase> tests = {
      // Empty function test
      {"fun test(){ /* comment */ return 1 & 2 }",
       {Token::Type::FUN, Token::Type::IDENTIFIER, Token::Type::LEFT_PAREN, Token::Type::RIGHT_PAREN,
        Token::Type::LEFT_BRACE, Token::Type::RETURN, Token::Type::INTEGER, Token::Type::AMPERSAND,
        Token::Type::INTEGER, Token::Type::RIGHT_BRACE}},

      {R"(
        fun test() {
          /******** start
          *
           var x = 10
          *
          **********/
        }
        )",
       {Token::Type::FUN, Token::Type::IDENTIFIER, Token::Type::LEFT_PAREN, Token::Type::RIGHT_PAREN,
        Token::Type::LEFT_BRACE, Token::Type::RIGHT_BRACE}}};

  RunTests(tests);
}

}  // namespace noisepage::execution::parsing::test
