#include <string>

#include "execution/tpl_test.h"  // NOLINT

#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/ast_traversal_visitor.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/sema/sema.h"

namespace tpl::ast::test {

class AstTraversalVisitorTest : public TplTest {
 public:
  AstTraversalVisitorTest() : region_("ast_test"), pos_() {}

  util::Region *region() { return &region_; }

  const SourcePosition &empty_pos() const { return pos_; }

  AstNode *GenerateAst(const std::string &src) {
    sema::ErrorReporter error(region());
    ast::Context ctx(region(), &error);

    parsing::Scanner scanner(src);
    parsing::Parser parser(&scanner, &ctx);

    if (error.HasErrors()) {
      error.PrintErrors();
      return nullptr;
    }

    auto *root = parser.Parse();

    sema::Sema sema(&ctx);
    auto check = sema.Run(root);

    if (error.HasErrors()) {
      error.PrintErrors();
      return nullptr;
    }

    EXPECT_FALSE(check);

    return root;
  }

 private:
  util::Region region_;
  SourcePosition pos_;
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
      bool is_finite_for = (stmt->condition() == nullptr);
      num_fors_ += static_cast<u32>(is_finite_for);
    } else {  // NOLINT
      num_fors_++;
    }
    AstTraversalVisitor<SelfT>::VisitForStmt(stmt);
  }

  u32 num_fors() const { return num_fors_; }

 private:
  u32 num_fors_;
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

    EXPECT_EQ(0u, finder.num_fors());
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

    EXPECT_EQ(1u, finder.num_fors());
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

    EXPECT_EQ(4u, finder.num_fors());
    EXPECT_EQ(3u, inf_finder.num_fors());
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

    EXPECT_EQ(4u, finder.num_fors());
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
    if constexpr (!CountLiterals) {
      num_funcs_++;
    }
    AstTraversalVisitor<SelfT>::VisitFunctionDecl(decl);
  }

  void VisitFunctionLitExpr(ast::FunctionLitExpr *expr) {
    if constexpr (CountLiterals) {
      num_funcs_++;
    }
    AstTraversalVisitor<SelfT>::VisitFunctionLitExpr(expr);
  }

  u32 num_funcs() const { return num_funcs_; }

 private:
  u32 num_funcs_;
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

    EXPECT_EQ(2u, find_func_decls.num_funcs());
    EXPECT_EQ(2u, find_all_funcs.num_funcs());
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

    EXPECT_EQ(2u, find_func_decls.num_funcs());
    EXPECT_EQ(3u, find_all_funcs.num_funcs());
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

  u32 num_ifs() const { return num_ifs_; }

 private:
  u32 num_ifs_;
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

    EXPECT_EQ(6u, finder.num_ifs());
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

    EXPECT_EQ(5u, finder.num_ifs());
  }
}

}  // namespace tpl::ast::test
