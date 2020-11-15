#include <algorithm>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::parsing::test {

class ParserTest : public TplTest {
 public:
  ParserTest() : region_("parser_test"), reporter_(&region_), ctx_(&region_, &reporter_) {}

  ast::Context *GetContext() { return &ctx_; }
  sema::ErrorReporter *Reporter() { return &reporter_; }

 private:
  util::Region region_;
  sema::ErrorReporter reporter_;
  ast::Context ctx_;
};

// NOLINTNEXTLINE
TEST_F(ParserTest, RegularForStmtTest) {
  const std::string source = R"(
    fun main() -> nil { for (var idx = 0; idx < 10; idx = idx + 1) { } }
  )";
  Scanner scanner(source);
  Parser parser(&scanner, GetContext());

  // Attempt parse
  auto *ast = parser.Parse();
  ASSERT_NE(nullptr, ast);
  ASSERT_FALSE(Reporter()->HasErrors());

  // No errors, move down AST
  ASSERT_TRUE(ast->IsFile());
  ASSERT_EQ(std::size_t{1}, ast->As<ast::File>()->Declarations().size());

  // Only one function decl
  auto *decl = ast->As<ast::File>()->Declarations()[0];
  ASSERT_TRUE(decl->IsFunctionDecl());

  auto *func_decl = decl->As<ast::FunctionDecl>();
  ASSERT_NE(nullptr, func_decl->Function());
  ASSERT_NE(nullptr, func_decl->Function()->Body());
  ASSERT_EQ(std::size_t{1}, func_decl->Function()->Body()->Statements().size());

  // Only one for statement, all elements are non-null
  auto *for_stmt = func_decl->Function()->Body()->Statements()[0]->SafeAs<ast::ForStmt>();
  ASSERT_NE(nullptr, for_stmt);
  ASSERT_NE(nullptr, for_stmt->Init());
  ASSERT_TRUE(for_stmt->Init()->IsDeclStmt());
  ASSERT_TRUE(for_stmt->Init()->As<ast::DeclStmt>()->Declaration()->IsVariableDecl());
  ASSERT_NE(nullptr, for_stmt->Condition());
  ASSERT_NE(nullptr, for_stmt->Next());
}

// NOLINTNEXTLINE
TEST_F(ParserTest, ExhaustiveForStmtTest) {
  struct Test {
    const std::string source_;
    bool init_null_, cond_null_, next_null_;
    Test(std::string source, bool init_null, bool cond_null, bool next_null)
        : source_(std::move(source)), init_null_(init_null), cond_null_(cond_null), next_null_(next_null) {}
  };

  // All possible permutations of init, condition, and next statements in loops
  // clang-format off
  const Test tests[] = {
      {"fun main() -> nil { for (var idx = 0; idx < 10; idx = idx + 1) { } }", false, false, false},
      {"fun main() -> nil { for (var idx = 0; idx < 10; ) { } }",              false, false, true},
      {"fun main() -> nil { for (var idx = 0; ; idx = idx + 1) { } }",         false, true, false},
      {"fun main() -> nil { for (var idx = 0; ; ) { } }",                      false, true, true},
      {"fun main() -> nil { for (; idx < 10; idx = idx + 1) { } }",            true, false, false},
      {"fun main() -> nil { for (; idx < 10; ) { } }",                         true, false, true},
      {"fun main() -> nil { for (; ; idx = idx + 1) { } }",                    true, true, false},
      {"fun main() -> nil { for (; ; ) { } }",                                 true, true, true},
  };
  // clang-format on

  for (const auto &test : tests) {
    Scanner scanner(test.source_);
    Parser parser(&scanner, GetContext());

    // Attempt parse
    auto *ast = parser.Parse();
    ASSERT_NE(nullptr, ast);
    ASSERT_FALSE(Reporter()->HasErrors());

    // No errors, move down AST
    ASSERT_TRUE(ast->IsFile());
    ASSERT_EQ(std::size_t{1}, ast->As<ast::File>()->Declarations().size());

    // Only one function decl
    auto *decl = ast->As<ast::File>()->Declarations()[0];
    ASSERT_TRUE(decl->IsFunctionDecl());

    auto *func_decl = decl->As<ast::FunctionDecl>();
    ASSERT_NE(nullptr, func_decl->Function());
    ASSERT_NE(nullptr, func_decl->Function()->Body());
    ASSERT_EQ(std::size_t{1}, func_decl->Function()->Body()->Statements().size());

    // Only one for statement, all elements are non-null
    auto *for_stmt = func_decl->Function()->Body()->Statements()[0]->SafeAs<ast::ForStmt>();
    ASSERT_NE(nullptr, for_stmt);
    ASSERT_EQ(test.init_null_, for_stmt->Init() == nullptr);
    ASSERT_EQ(test.cond_null_, for_stmt->Condition() == nullptr);
    ASSERT_EQ(test.next_null_, for_stmt->Next() == nullptr);
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTest, RegularForStmt_NoInitTest) {
  const std::string source = R"(
    fun main() -> nil {
      var idx = 0
      for (; idx < 10; idx = idx + 1) { }
    }
  )";
  Scanner scanner(source);
  Parser parser(&scanner, GetContext());

  // Attempt parse
  auto *ast = parser.Parse();
  ASSERT_NE(nullptr, ast);
  ASSERT_FALSE(Reporter()->HasErrors());

  // No errors, move down AST
  ASSERT_TRUE(ast->IsFile());
  ASSERT_EQ(std::size_t{1}, ast->As<ast::File>()->Declarations().size());

  // Only one function decl
  auto *decl = ast->As<ast::File>()->Declarations()[0];
  ASSERT_TRUE(decl->IsFunctionDecl());

  auto *func_decl = decl->As<ast::FunctionDecl>();
  ASSERT_NE(nullptr, func_decl->Function());
  ASSERT_NE(nullptr, func_decl->Function()->Body());
  ASSERT_EQ(std::size_t{2}, func_decl->Function()->Body()->Statements().size());

  // Two statements in function

  // First is the variable declaration
  auto &block = func_decl->Function()->Body()->Statements();
  ASSERT_TRUE(block[0]->IsDeclStmt());
  ASSERT_TRUE(block[0]->As<ast::DeclStmt>()->Declaration()->IsVariableDecl());

  // Next is the for statement
  auto *for_stmt = block[1]->SafeAs<ast::ForStmt>();
  ASSERT_NE(nullptr, for_stmt);
  ASSERT_EQ(nullptr, for_stmt->Init());
  ASSERT_NE(nullptr, for_stmt->Condition());
  ASSERT_NE(nullptr, for_stmt->Next());
}

// NOLINTNEXTLINE
TEST_F(ParserTest, RegularForStmt_WhileTest) {
  const std::string for_while_sources[] = {
      R"(
      fun main() -> nil {
        var idx = 0
        for (idx < 10) { idx = idx + 1 }
      }
      )",
      R"(
      fun main() -> nil {
        var idx = 0
        for (; idx < 10; ) { idx = idx + 1 }
      }
      )",
  };

  for (const auto &source : for_while_sources) {
    Scanner scanner(source);
    Parser parser(&scanner, GetContext());

    // Attempt parse
    auto *ast = parser.Parse();
    ASSERT_NE(nullptr, ast);
    ASSERT_FALSE(Reporter()->HasErrors());

    // No errors, move down AST
    ASSERT_TRUE(ast->IsFile());
    ASSERT_EQ(std::size_t{1}, ast->As<ast::File>()->Declarations().size());

    // Only one function decl
    auto *decl = ast->As<ast::File>()->Declarations()[0];
    ASSERT_TRUE(decl->IsFunctionDecl());

    auto *func_decl = decl->As<ast::FunctionDecl>();
    ASSERT_NE(nullptr, func_decl->Function());
    ASSERT_NE(nullptr, func_decl->Function()->Body());
    ASSERT_EQ(std::size_t{2}, func_decl->Function()->Body()->Statements().size());

    // Two statements in function

    // First is the variable declaration
    auto &block = func_decl->Function()->Body()->Statements();
    ASSERT_TRUE(block[0]->IsDeclStmt());
    ASSERT_TRUE(block[0]->As<ast::DeclStmt>()->Declaration()->IsVariableDecl());

    // Next is the for statement
    auto *for_stmt = block[1]->SafeAs<ast::ForStmt>();
    ASSERT_NE(nullptr, for_stmt);
    ASSERT_EQ(nullptr, for_stmt->Init());
    ASSERT_NE(nullptr, for_stmt->Condition());
    ASSERT_EQ(nullptr, for_stmt->Next());
  }
}

// NOLINTNEXTLINE
TEST_F(ParserTest, RegularForInStmtTest) {
  const std::string source = R"(
    fun main() -> nil {
      for (idx in range()) { }
    }
  )";
  Scanner scanner(source);
  Parser parser(&scanner, GetContext());

  // Attempt parse
  auto *ast = parser.Parse();
  ASSERT_NE(nullptr, ast);
  ASSERT_FALSE(Reporter()->HasErrors());

  // No errors, move down AST
  ASSERT_TRUE(ast->IsFile());
  ASSERT_EQ(std::size_t{1}, ast->As<ast::File>()->Declarations().size());

  // Only one function decl
  auto *decl = ast->As<ast::File>()->Declarations()[0];
  ASSERT_TRUE(decl->IsFunctionDecl());

  auto *func_decl = decl->As<ast::FunctionDecl>();
  ASSERT_NE(nullptr, func_decl->Function());
  ASSERT_NE(nullptr, func_decl->Function()->Body());
  ASSERT_EQ(std::size_t{1}, func_decl->Function()->Body()->Statements().size());

  // Only statement is the for-in statement
  auto &block = func_decl->Function()->Body()->Statements();
  auto *for_in_stmt = block[0]->SafeAs<ast::ForInStmt>();
  ASSERT_NE(nullptr, for_in_stmt);
  ASSERT_NE(nullptr, for_in_stmt->Target());
  ASSERT_NE(nullptr, for_in_stmt->Iterable());
}

// NOLINTNEXTLINE
TEST_F(ParserTest, ArrayTypeTest) {
  struct TestCase {
    std::string source_;
    bool valid_;
  };

  TestCase tests[] = {
      // Array with unknown length = valid
      {"fun main(arr: [*]int32) -> nil { }", true},
      // Array with known length = valid
      {"fun main() -> nil { var arr: [10]int32 }", true},
      // Array with missing length field = invalid
      {"fun main(arr: []int32) -> nil { }", false},
  };

  for (const auto &test_case : tests) {
    Scanner scanner(test_case.source_);
    Parser parser(&scanner, GetContext());

    // Attempt parse
    auto *ast = parser.Parse();
    ASSERT_NE(nullptr, ast);
    EXPECT_EQ(test_case.valid_, !Reporter()->HasErrors());
    Reporter()->Reset();
  }
}

}  // namespace noisepage::execution::parsing::test
