#include "execution/ast/ast_dump.h"

#include <set>
#include <string>
#include <vector>

#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/ast_traversal_visitor.h"
#include "execution/parsing/parser.h"
#include "execution/parsing/scanner.h"
#include "execution/sema/sema.h"
#include "execution/tpl_test.h"
#include "loggers/execution_logger.h"

namespace noisepage::execution::ast::test {

/**
 * Magic macro so that we can easily add Visit methods to our extractor class
 * and ensure that we compile correctly.
 */
#define EXTRACT_KINDNAME_METHOD(ASTNODE)              \
  void Visit##ASTNODE(ast::ASTNODE *node) {           \
    kindnames_.insert(node->KindName());              \
    AstTraversalVisitor<SelfT>::Visit##ASTNODE(node); \
  }

/**
 * This is a helper class that extracts all of the names of the nodes
 * found in the given AST root node. It stores them in an unordered set
 * so each kind name is only stored once.
 * @tparam FindInfinite
 */
template <bool FindInfinite = false>
class ExtractKindNames : public AstTraversalVisitor<ExtractKindNames<FindInfinite>> {
  using SelfT = ExtractKindNames<FindInfinite>;

 public:
  explicit ExtractKindNames(ast::AstNode *root) : AstTraversalVisitor<SelfT>(root) {}

  // Sometimes these fields get excluded in the dump output if the
  // code is simple. To simplify the test, we just ignore them.
  // EXTRACT_KINDNAME_METHOD(FieldDecl);
  // EXTRACT_KINDNAME_METHOD(FunctionTypeRepr);
  // EXTRACT_KINDNAME_METHOD(IdentifierExpr);
  // EXTRACT_KINDNAME_METHOD(DeclStmt);
  // EXTRACT_KINDNAME_METHOD(ArrayTypeRepr);

  EXTRACT_KINDNAME_METHOD(FunctionDecl);
  EXTRACT_KINDNAME_METHOD(BlockStmt);
  EXTRACT_KINDNAME_METHOD(StructDecl);
  EXTRACT_KINDNAME_METHOD(VariableDecl);
  EXTRACT_KINDNAME_METHOD(UnaryOpExpr);
  EXTRACT_KINDNAME_METHOD(ReturnStmt);
  EXTRACT_KINDNAME_METHOD(CallExpr);
  EXTRACT_KINDNAME_METHOD(ImplicitCastExpr);
  EXTRACT_KINDNAME_METHOD(AssignmentStmt);
  EXTRACT_KINDNAME_METHOD(File);
  EXTRACT_KINDNAME_METHOD(FunctionLitExpr);
  EXTRACT_KINDNAME_METHOD(ForStmt);
  EXTRACT_KINDNAME_METHOD(ForInStmt);
  EXTRACT_KINDNAME_METHOD(BinaryOpExpr);
  EXTRACT_KINDNAME_METHOD(LitExpr);
  EXTRACT_KINDNAME_METHOD(StructTypeRepr);
  EXTRACT_KINDNAME_METHOD(PointerTypeRepr);
  EXTRACT_KINDNAME_METHOD(ComparisonOpExpr);
  EXTRACT_KINDNAME_METHOD(IfStmt);
  EXTRACT_KINDNAME_METHOD(ExpressionStmt);
  EXTRACT_KINDNAME_METHOD(IndexExpr);

  /**
   * Return the unordered set of the kind names found in this AST
   * @return
   */
  std::set<std::string> GetKindNames() const { return kindnames_; }

 private:
  std::set<std::string> kindnames_;
};

/**
 * Tests to make sure that the AstDump utility code works as expected.
 * These tests simply making sure that certain AstNodes and constant values
 * appear in the output. It does not check whether that output is correctly
 * formatted.
 */
class AstDumpTest : public TplTest {
 public:
  AstDumpTest() : region_("ast_test"), pos_() {}

  util::Region *Region() { return &region_; }

  const SourcePosition &EmptyPos() const { return pos_; }

  AstNode *GenerateAst(const std::string &src) {
    sema::ErrorReporter error(Region());
    ast::Context ctx(Region(), &error);

    parsing::Scanner scanner(src);
    parsing::Parser parser(&scanner, &ctx);

    if (error.HasErrors()) {
      EXECUTION_LOG_ERROR(error.SerializeErrors());
      return nullptr;
    }

    auto *root = parser.Parse();

    sema::Sema sema(&ctx);
    auto check = sema.Run(root);

    if (error.HasErrors()) {
      EXECUTION_LOG_ERROR(error.SerializeErrors());
      return nullptr;
    }

    EXPECT_FALSE(check);

    return root;
  }

  /**
   * For the given TPL source code, generate the AST and dump it out
   * We then use the ExtractKindNames utility to find all of the names of the
   * nodes in the tree and make sure that they appear in the dump. We also
   * check whether the given list of constant strings appear as well.
   * @param constants
   */
  void CheckDump(const std::string &src, const std::vector<std::string> &constants) {
    // Create the AST
    EXECUTION_LOG_DEBUG("Generating AST:\n{}", src);
    auto *root = GenerateAst(src);
    ASSERT_NE(root, nullptr);

    // Get the expected token strings
    ExtractKindNames extractor(root);
    extractor.Run();
    auto tokens = extractor.GetKindNames();

    // Generate the dump!
    auto dump = AstDump::Dump(root);
    EXPECT_FALSE(dump.empty());
    EXECUTION_LOG_DEBUG("Dump:\n{}", dump);

    // Check that the expected tokens and constants are in the dump
    for (const auto &token : tokens) {
      EXECUTION_LOG_DEBUG("Looking for token '{}'", token);
      EXPECT_NE(dump.find(token), std::string::npos) << "Missing token '" << token << "'";
    }
    for (const auto &constant : constants) {
      EXPECT_NE(dump.find(constant), std::string::npos) << "Missing constant '" << constant << "'";
    }
  }

 private:
  util::Region region_;
  SourcePosition pos_;
};

// NOLINTNEXTLINE
TEST_F(AstDumpTest, IfTest) {
  const auto src = R"(
    fun f1(xyz: int) -> void {
      if (xyz < 67890) {
        if (xyz < 12345) {
          if (xyz < 1) { }
          else {}
        }
      }
    }
  )";

  std::vector<std::string> constants = {
      "xyz",
      "12345",
      "67890",
  };

  CheckDump(src, constants);
}

// NOLINTNEXTLINE
TEST_F(AstDumpTest, ForLoopTest) {
  const auto src = R"(
    fun test(xxxxxx: int) -> int {
      for (xxxxxx + 777777 < 888888) { }
      return 999999
    })";

  std::vector<std::string> constants = {"xxxxxx", "777777", "888888", "999999"};

  CheckDump(src, constants);
}

// NOLINTNEXTLINE
TEST_F(AstDumpTest, FunctionTest) {
  const auto src = R"(
    fun XXXXXX(x: int) -> void { }
    fun yyyyyy(x: int) -> void { }
  )";

  std::vector<std::string> constants = {
      "XXXXXX",
      "yyyyyy",
  };

  CheckDump(src, constants);
}

// NOLINTNEXTLINE
TEST_F(AstDumpTest, VariableTest) {
  const auto src = R"(
    fun main() -> int64 {
      var a: int8 = 99
      var b: int16 = 999
      var c: int32 = 9999
      var d: int64 = 99999
      return a + b + c + d
    })";

  std::vector<std::string> constants = {
      "99",
      "999",
      "9999",
      "99999",
  };

  CheckDump(src, constants);
}

// NOLINTNEXTLINE
TEST_F(AstDumpTest, CallTest) {
  const auto src = R"(
    fun AAAA(yyyy: int) -> int {
      var date1 = @dateToSql(2019, 10, 04)
      var date2 = @dateToSql(2019, 10, 4)
      if (date1 != date2) {
        return yyyy
      }
      return 1
    }
    fun main() -> int {
      var xxxx = 10
      return AAAA(xxxx)
    })";

  std::vector<std::string> constants = {
      "AAAA", "xxxx", "yyyy", "date1", "date2",
  };

  CheckDump(src, constants);
}

// NOLINTNEXTLINE
TEST_F(AstDumpTest, TypeTest) {
  const auto src = R"(
      fun main() -> int {
        var res : int = 0

        var boolVar1 : bool = true
        var boolVar2 = @boolToSql(true)

        var intVar1 = @intToSql(5)
        var intVar2 : int = 5.5 // FloatToInt

        var floatVar1 = @floatToSql(5.5)
        var floatVar2 : float = intVar2 // IntToFloat

        var stringVar = @stringToSql("5555")

        var intArray: [2]uint32
        intArray[0] = 1
        intArray[1] = 2

        return res
      })";

  std::vector<std::string> constants = {
      "res",
  };

  CheckDump(src, constants);
}

}  // namespace noisepage::execution::ast::test
