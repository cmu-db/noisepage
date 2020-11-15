#include "execution/ast/ast.h"

#include "execution/ast/ast_node_factory.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::ast::test {

class AstTest : public TplTest {
 public:
  AstTest() : region_("ast_test"), pos_() {}

  util::Region *Region() { return &region_; }

  const SourcePosition &EmptyPos() const { return pos_; }

 private:
  util::Region region_;

  SourcePosition pos_;
};

// NOLINTNEXTLINE
TEST_F(AstTest, HierarchyTest) {
  AstNodeFactory factory(Region());

#define CHECK_NODE_IS_NOT_KIND(n) EXPECT_FALSE(node->Is<n>()) << "Node " << node->KindName() << " is not " << #n;

#define IS_MATCH(n) node->Is<n>() +
#define COUNT_MATCHES(NODE_LIST) NODE_LIST(IS_MATCH) 0
#define CSL(n) #n << ", "

  /// Test declarations
  {
    AstNode *all_decls[] = {
        factory.NewFieldDecl(EmptyPos(), Identifier(), nullptr),
        factory.NewFunctionDecl(
            EmptyPos(), Identifier(),
            factory.NewFunctionLitExpr(
                factory.NewFunctionType(EmptyPos(), util::RegionVector<FieldDecl *>(Region()), nullptr), nullptr)),
        factory.NewStructDecl(EmptyPos(), Identifier(), nullptr),
        factory.NewVariableDecl(EmptyPos(), Identifier(), nullptr, nullptr),
    };

    for (const auto *node : all_decls) {
      // Ensure declarations aren't expressions, types or statements
      STATEMENT_NODES(CHECK_NODE_IS_NOT_KIND)
      EXPRESSION_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete declarations are also a base declaration type
      EXPECT_TRUE(node->Is<Decl>()) << "Node " << node->KindName()
                                    << " isn't an Decl? Ensure Decl::classof() handles all "
                                       "cases if you've added a new Decl node.";

      // Each declaration must match only one other declaration type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(DECLARATION_NODES))
          << node->KindName() << " matches more than one of " << DECLARATION_NODES(CSL);
    }
  }

  /// Test expressions
  {
    AstNode *all_exprs[] = {
        factory.NewBinaryOpExpr(EmptyPos(), parsing::Token::Type::PLUS, nullptr, nullptr),
        factory.NewCallExpr(factory.NewNilLiteral(EmptyPos()), util::RegionVector<Expr *>(Region())),
        factory.NewFunctionLitExpr(
            factory.NewFunctionType(EmptyPos(), util::RegionVector<FieldDecl *>(Region()), nullptr), nullptr),
        factory.NewNilLiteral(EmptyPos()),
        factory.NewUnaryOpExpr(EmptyPos(), parsing::Token::Type::MINUS, nullptr),
        factory.NewIdentifierExpr(EmptyPos(), Identifier()),
        factory.NewArrayType(EmptyPos(), nullptr, nullptr),
        factory.NewFunctionType(EmptyPos(), util::RegionVector<FieldDecl *>(Region()), nullptr),
        factory.NewPointerType(EmptyPos(), nullptr),
        factory.NewStructType(EmptyPos(), util::RegionVector<FieldDecl *>(Region())),
    };

    for (const auto *node : all_exprs) {
      // Ensure expressions aren't declarations, types or statements
      DECLARATION_NODES(CHECK_NODE_IS_NOT_KIND)
      STATEMENT_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete expressions are also a base expression type
      EXPECT_TRUE(node->Is<Expr>()) << "Node " << node->KindName()
                                    << " isn't an Expr? Ensure Expr::classof() handles all "
                                       "cases if you've added a new Expr node.";

      // Each expression must match only one other expression type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(EXPRESSION_NODES))
          << node->KindName() << " matches more than one of " << EXPRESSION_NODES(CSL);
    }
  }

  /// Test statements
  {
    AstNode *all_stmts[] = {
        factory.NewBlockStmt(EmptyPos(), EmptyPos(), util::RegionVector<Stmt *>(Region())),
        factory.NewDeclStmt(factory.NewVariableDecl(EmptyPos(), Identifier(), nullptr, nullptr)),
        factory.NewExpressionStmt(factory.NewNilLiteral(EmptyPos())),
        factory.NewForStmt(EmptyPos(), nullptr, nullptr, nullptr, nullptr),
        factory.NewIfStmt(EmptyPos(), nullptr, nullptr, nullptr),
        factory.NewReturnStmt(EmptyPos(), nullptr),
    };

    for (const auto *node : all_stmts) {
      // Ensure statements aren't declarations, types or expressions
      DECLARATION_NODES(CHECK_NODE_IS_NOT_KIND)
      EXPRESSION_NODES(CHECK_NODE_IS_NOT_KIND)

      // Ensure concrete expressions are also a base expression type
      EXPECT_TRUE(node->Is<Stmt>()) << "Node " << node->KindName()
                                    << " isn't an Statement? Ensure Statement::classof() handles all "
                                       "cases if you've added a new Statement node.";

      // Each expression must match only one other expression type (itself)
      EXPECT_EQ(1, COUNT_MATCHES(STATEMENT_NODES))
          << node->KindName() << " matches more than one of " << STATEMENT_NODES(CSL);
    }
  }
}

}  // namespace noisepage::execution::ast::test
