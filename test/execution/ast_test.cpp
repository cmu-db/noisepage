#include "execution/tpl_test.h"  // NOLINT

#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"

namespace tpl::ast::test {

class AstTest : public TplTest {
 public:
  AstTest() : region_("ast_test"), pos_() {}

  util::Region *region() { return &region_; }

  const SourcePosition &empty_pos() const { return pos_; }

 private:
  util::Region region_;

  SourcePosition pos_;
};

// NOLINTNEXTLINE
TEST_F(AstTest, HierechyTest) {
  AstNodeFactory factory(region());

#define CHECK_NODE_IS_NOT_KIND(n) EXPECT_FALSE(node->Is<n>()) << "Node " << node->kind_name() << " is not " << #n;

#define IS_MATCH(n) node->Is<n>() +
#define COUNT_MATCHES(NODE_LIST) NODE_LIST(IS_MATCH) 0
#define CSL(n) #n << ", "

  /// Test declarations
  {
    AstNode *all_decls[] = {
        factory.NewFieldDecl(empty_pos(), Identifier(nullptr), nullptr),
        factory.NewFunctionDecl(
            empty_pos(), Identifier(nullptr),
            factory.NewFunctionLitExpr(
                factory.NewFunctionType(empty_pos(), util::RegionVector<FieldDecl *>(region()), nullptr), nullptr)),
        factory.NewStructDecl(empty_pos(), Identifier(nullptr), nullptr),
        factory.NewVariableDecl(empty_pos(), Identifier(nullptr), nullptr, nullptr),
    };

    for (const auto *node : all_decls) {
      // Ensure declarations aren't expressions, types or statements
      STATEMENT_NODES(CHECK_NODE_IS_NOT_KIND)
      EXPRESSION_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete declarations are also a base declaration type
      EXPECT_TRUE(node->Is<Decl>()) << "Node " << node->kind_name()
                                    << " isn't an Decl? Ensure Decl::classof() handles all "
                                       "cases if you've added a new Decl node.";

      // Each declaration must match only one other declaration type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(DECLARATION_NODES))
          << node->kind_name() << " matches more than one of " << DECLARATION_NODES(CSL);
    }
  }

  /// Test expressions
  {
    AstNode *all_exprs[] = {
        factory.NewBinaryOpExpr(empty_pos(), parsing::Token::Type::PLUS, nullptr, nullptr),
        factory.NewCallExpr(factory.NewNilLiteral(empty_pos()), util::RegionVector<Expr *>(region())),
        factory.NewFunctionLitExpr(
            factory.NewFunctionType(empty_pos(), util::RegionVector<FieldDecl *>(region()), nullptr), nullptr),
        factory.NewNilLiteral(empty_pos()),
        factory.NewUnaryOpExpr(empty_pos(), parsing::Token::Type::MINUS, nullptr),
        factory.NewIdentifierExpr(empty_pos(), Identifier(nullptr)),
        factory.NewArrayType(empty_pos(), nullptr, nullptr),
        factory.NewFunctionType(empty_pos(), util::RegionVector<FieldDecl *>(region()), nullptr),
        factory.NewPointerType(empty_pos(), nullptr),
        factory.NewStructType(empty_pos(), util::RegionVector<FieldDecl *>(region())),
    };

    for (const auto *node : all_exprs) {
      // Ensure expressions aren't declarations, types or statements
      DECLARATION_NODES(CHECK_NODE_IS_NOT_KIND)
      STATEMENT_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete expressions are also a base expression type
      EXPECT_TRUE(node->Is<Expr>()) << "Node " << node->kind_name()
                                    << " isn't an Expr? Ensure Expr::classof() handles all "
                                       "cases if you've added a new Expr node.";

      // Each expression must match only one other expression type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(EXPRESSION_NODES))
          << node->kind_name() << " matches more than one of " << EXPRESSION_NODES(CSL);
    }
  }

  /// Test statements
  {
    AstNode *all_stmts[] = {
        factory.NewBlockStmt(empty_pos(), empty_pos(), util::RegionVector<Stmt *>(region())),
        factory.NewDeclStmt(factory.NewVariableDecl(empty_pos(), Identifier(nullptr), nullptr, nullptr)),
        factory.NewExpressionStmt(factory.NewNilLiteral(empty_pos())),
        factory.NewForStmt(empty_pos(), nullptr, nullptr, nullptr, nullptr),
        factory.NewIfStmt(empty_pos(), nullptr, nullptr, nullptr),
        factory.NewReturnStmt(empty_pos(), nullptr),
    };

    for (const auto *node : all_stmts) {
      // Ensure statements aren't declarations, types or expressions
      DECLARATION_NODES(CHECK_NODE_IS_NOT_KIND)
      EXPRESSION_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete expressions are also a base expression type
      EXPECT_TRUE(node->Is<Stmt>()) << "Node " << node->kind_name()
                                    << " isn't an Statement? Ensure Statement::classof() handles all "
                                       "cases if you've added a new Statement node.";

      // Each expression must match only one other expression type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(STATEMENT_NODES))
          << node->kind_name() << " matches more than one of " << STATEMENT_NODES(CSL);
    }
  }
}

}  // namespace tpl::ast::test
