#include "execution/ast/ast_traversal_visitor.h"

#include <string>

#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/sema/sema.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::ast::test {

class AstTraversalVisitorTest : public TplTest {
 public:
  AstTraversalVisitorTest() : region_("ast_test"), error_reporter_(&region_), context_(&region_, &error_reporter_) {}

  AstNode *GenerateAst(const std::string &src) {
    parsing::Scanner scanner(src);
    parsing::Parser parser(&scanner, Ctx());

    if (ErrorReporter()->HasErrors()) {
      std::cerr << ErrorReporter()->SerializeErrors() << std::endl;  // NOLINT
      return nullptr;
    }

    auto *root = parser.Parse();

    sema::Sema sema(Ctx());
    auto check = sema.Run(root);

    EXPECT_FALSE(check);

    return root;
  }

 private:
  sema::ErrorReporter *ErrorReporter() { return &error_reporter_; }

  ast::Context *Ctx() { return &context_; }

 private:
  util::Region region_;
  sema::ErrorReporter error_reporter_;
  ast::Context context_;
};

namespace {

// Visit to find FOR loops only
template <bool FindInfinite = false>
class ForFinder : public AstTraversalVisitor<ForFinder<FindInfinite>> {
  using SelfT = ForFinder<FindInfinite>;

 public:
  explicit ForFinder(ast::AstNode *root) : AstTraversalVisitor<SelfT>(root), num_fors_(0) {}

  void VisitForStmt(ast::ForStmt *stmt) {
    if constexpr (FindInfinite) {
      bool is_finite_for = (stmt->Condition() == nullptr);
      num_fors_ += static_cast<uint32_t>(is_finite_for);
    } else {  // NOLINT
      num_fors_++;
    }
    AstTraversalVisitor<SelfT>::VisitForStmt(stmt);
  }

  uint32_t NumFors() const { return num_fors_; }

 private:
  uint32_t num_fors_;
};

}  // namespace

// NOLINTNEXTLINE
TEST_F(AstTraversalVisitorTest, CountForLoopsTest) {
  // No for-loops
  {
    const auto src = R"(
    fun test(x: uint32) -> uint32 {
      var y : uint32 = 20
      return x * y
    })";

    auto *root = GenerateAst(src);

    ForFinder finder(root);
    finder.Run();

    EXPECT_EQ(0u, finder.NumFors());
  }

  // 1 for-loop
  {
    const auto src = R"(
    fun test(x: int) -> int {
      for (x < 10) { }
      return 0
    })";

    auto *root = GenerateAst(src);

    ForFinder finder(root);
    finder.Run();

    EXPECT_EQ(1u, finder.NumFors());
  }

  // 4 nested for-loops
  {
    const auto src = R"(
    fun test(x: int) -> int {
      for (x < 10) {
        for {
          for {
            for { }
          }
        }
      }
      return 0
    })";

    auto *root = GenerateAst(src);

    ForFinder<false> finder(root);
    ForFinder<true> inf_finder(root);

    finder.Run();
    inf_finder.Run();

    EXPECT_EQ(4u, finder.NumFors());
    EXPECT_EQ(3u, inf_finder.NumFors());
  }

  // 4 sequential for-loops
  {
    const auto src = R"(
    fun test(x: int) -> int {
      for {}
      for {}
      for {}
      for {}
      return 0
    })";

    auto *root = GenerateAst(src);

    ForFinder finder(root);
    finder.Run();

    EXPECT_EQ(4u, finder.NumFors());
  }
}

namespace {

// Visitor to find function declarations or, optionally, all function literals
// (i.e., lambdas) as well.
template <bool CountLiterals = false>
class FunctionFinder : public AstTraversalVisitor<FunctionFinder<CountLiterals>> {
  using SelfT = FunctionFinder<CountLiterals>;

 public:
  explicit FunctionFinder(ast::AstNode *root) : AstTraversalVisitor<SelfT>(root), num_funcs_(0) {}

  void VisitFunctionDecl(ast::FunctionDecl *decl) {
    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (!CountLiterals) {
      num_funcs_++;
    }
    AstTraversalVisitor<SelfT>::VisitFunctionDecl(decl);
  }

  void VisitFunctionLitExpr(ast::FunctionLitExpr *expr) {
    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (CountLiterals) {
      num_funcs_++;
    }
    AstTraversalVisitor<SelfT>::VisitFunctionLitExpr(expr);
  }

  uint32_t NumFuncs() const { return num_funcs_; }

 private:
  uint32_t num_funcs_;
};

}  // namespace

// NOLINTNEXTLINE
TEST_F(AstTraversalVisitorTest, CountFunctionsTest) {
  // Function declarations only
  {
    const auto src = R"(
      fun f1(x: int) -> void { }
      fun f2(x: int) -> void { }
    )";

    auto *root = GenerateAst(src);

    FunctionFinder<false> find_func_decls(root);
    FunctionFinder<true> find_all_funcs(root);

    find_func_decls.Run();
    find_all_funcs.Run();

    EXPECT_EQ(2u, find_func_decls.NumFuncs());
    EXPECT_EQ(2u, find_all_funcs.NumFuncs());
  }

  // Function declarations and literals
  {
    const auto src = R"(
      fun f1(x: int) -> void { }
      fun f2(x: int) -> void {
        var x = fun(xx:int) -> int { return xx * 2 }
      }
    )";

    auto *root = GenerateAst(src);

    FunctionFinder<false> find_func_decls(root);
    FunctionFinder<true> find_all_funcs(root);

    find_func_decls.Run();
    find_all_funcs.Run();

    EXPECT_EQ(2u, find_func_decls.NumFuncs());
    EXPECT_EQ(3u, find_all_funcs.NumFuncs());
  }
}

namespace {

class IfFinder : public AstTraversalVisitor<IfFinder> {
 public:
  explicit IfFinder(ast::AstNode *root) : AstTraversalVisitor(root), num_ifs_(0) {}

  void VisitIfStmt(ast::IfStmt *stmt) {
    num_ifs_++;
    AstTraversalVisitor<IfFinder>::VisitIfStmt(stmt);
  }

  uint32_t NumIfs() const { return num_ifs_; }

 private:
  uint32_t num_ifs_;
};

}  // namespace

// NOLINTNEXTLINE
TEST_F(AstTraversalVisitorTest, CountIfTest) {
  // Nestes Ifs
  {
    const auto src = R"(
      fun f1(x: int) -> void {
        if (x < 10) {
          if (x < 5) {
            if (x < 2) { }
            else {}
          }
        } else if (x < 20) {
          if (x < 15) { }
          else if (x < 12) { }
        } else { }
      }
    )";

    auto *root = GenerateAst(src);

    IfFinder finder(root);

    finder.Run();

    EXPECT_EQ(6u, finder.NumIfs());
  }

  // Serial Ifs
  {
    const auto src = R"(
      fun f1(x: int) -> void {
        if (x < 10) { }
        else if (x < 5) { }

        if (x < 2) { }
        else { }

        if (x < 20) { }
        if (x < 15) { }
        else { }
      }
    )";

    auto *root = GenerateAst(src);

    IfFinder finder(root);

    finder.Run();

    EXPECT_EQ(5u, finder.NumIfs());
  }
}

}  // namespace noisepage::execution::ast::test
