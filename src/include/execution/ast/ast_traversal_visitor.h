#pragma once

#include "common/macros.h"
#include "execution/ast/ast_visitor.h"
#include "execution/util/execution_common.h"

namespace noisepage::execution::ast {

/**
 * A visitor that fully and recursively traverses an entire AST tree. Clients
 * can control which AST nodes by implementing only the Visit() methods on the
 * node types they're interested. Moreover, clients can cull visitations to
 * whole classes of nodes by implementing VisitNode() and returning true
 * only for those node types they're interested.
 *
 * Usage:
 * @code
 * // The ForStmtVisitor class will find ALL for-statement nodes in the input AST
 * class ForStmtVisitor : public AstTraversalVisitor<ForStmtVisitor> {
 *  public:
 *   ForStmtVisitor(ast::AstNode *root) : AstTraversalVisitor<ForStmtVisitor>(root) {}
 *
 *   void VisitForStmt(ast::ForStmt *stmt) {
 *     // Process stmt
 *   }
 * }
 * @endcode
 *
 * @tparam Subclass visitor subclass
 */
template <typename Subclass>
class AstTraversalVisitor : public AstVisitor<Subclass> {
 public:
  /**
   * Construct a visitor over the AST rooted at @em root.
   * @param root The root of the AST tree to begin visiting.
   */
  explicit AstTraversalVisitor(AstNode *root) : root_(root) {}

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(AstTraversalVisitor);

  /**
   * Run the traversal.
   */
  void Run() {
    NOISEPAGE_ASSERT(root_ != nullptr, "Cannot run traversal on NULL tree");
    AstVisitor<Subclass>::Visit(root_);
  }

  /**
   * Declare all node visit methods here
   */
#define DECLARE_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

 protected:
  /**
   * Should this iterator visit the given node? This method can be implemented
   * in subclasses to skip some visiting some nodes. By default, we visit all
   * nodes.
   * @param node to visit
   * @return whether the node should be visited
   */
  bool VisitNode(AstNode *node) { return true; }

 private:
  AstNode *root_;
};

// ---------------------------------------------------------
//
// Implementation below
//
// ---------------------------------------------------------

#define PROCESS_NODE(node)                \
  do {                                    \
    if (!this->Impl()->VisitNode(node)) { \
      return;                             \
    }                                     \
  } while (false)

#define RECURSE(call) this->Impl()->call

// TODO(Amadou): Doxygen complains that these function do not exist for some reason.
// Figure out why
// \cond DO_NOT_DOCUMENT
template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitBadExpr(BadExpr *node) {
  PROCESS_NODE(node);
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitFieldDecl(FieldDecl *node) {
  PROCESS_NODE(node);
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitFunctionDecl(FunctionDecl *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Function()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitIdentifierExpr(IdentifierExpr *node) {
  PROCESS_NODE(node);
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->ElementType()));
  if (node->HasLength()) {
    RECURSE(Visit(node->Length()));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitBlockStmt(BlockStmt *node) {
  PROCESS_NODE(node);
  for (auto *stmt : node->Statements()) {
    RECURSE(Visit(stmt));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitStructDecl(StructDecl *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->TypeRepr()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitVariableDecl(VariableDecl *node) {
  PROCESS_NODE(node);
  if (node->HasTypeDecl()) {
    RECURSE(Visit(node->TypeRepr()));
  }
  if (node->HasInitialValue()) {
    RECURSE(Visit(node->Initial()));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitUnaryOpExpr(UnaryOpExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Input()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitReturnStmt(ReturnStmt *node) {
  PROCESS_NODE(node);
  if (node->Ret() != nullptr) {
    RECURSE(Visit(node->Ret()));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitCallExpr(CallExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Function()));
  for (auto *arg : node->Arguments()) {
    RECURSE(Visit(arg));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitImplicitCastExpr(ImplicitCastExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Input()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitAssignmentStmt(AssignmentStmt *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Destination()));
  RECURSE(Visit(node->Source()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitFile(File *node) {
  PROCESS_NODE(node);
  for (auto *decl : node->Declarations()) {
    RECURSE(Visit(decl));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitFunctionLitExpr(FunctionLitExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->TypeRepr()));
  RECURSE(Visit(node->Body()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitForStmt(ForStmt *node) {
  PROCESS_NODE(node);
  if (node->Init() != nullptr) {
    RECURSE(Visit(node->Init()));
  }
  if (node->Condition() != nullptr) {
    RECURSE(Visit(node->Condition()));
  }
  if (node->Next() != nullptr) {
    RECURSE(Visit(node->Next()));
  }
  RECURSE(Visit(node->Body()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitForInStmt(ForInStmt *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Target()));
  RECURSE(Visit(node->Iterable()));
  RECURSE(Visit(node->Body()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitBinaryOpExpr(BinaryOpExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Left()));
  RECURSE(Visit(node->Right()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitMapTypeRepr(MapTypeRepr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->KeyType()));
  RECURSE(Visit(node->ValType()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitLitExpr(LitExpr *node) {
  PROCESS_NODE(node);
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitStructTypeRepr(StructTypeRepr *node) {
  PROCESS_NODE(node);
  for (auto *field : node->Fields()) {
    RECURSE(Visit(field));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitDeclStmt(DeclStmt *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Declaration()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitMemberExpr(MemberExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Object()));
  RECURSE(Visit(node->Member()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitPointerTypeRepr(PointerTypeRepr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Base()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitComparisonOpExpr(ComparisonOpExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Left()));
  RECURSE(Visit(node->Right()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitIfStmt(IfStmt *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Condition()));
  RECURSE(Visit(node->ThenStmt()));
  if (node->HasElseStmt()) {
    RECURSE(Visit(node->ElseStmt()));
  }
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitExpressionStmt(ExpressionStmt *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Expression()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitIndexExpr(IndexExpr *node) {
  PROCESS_NODE(node);
  RECURSE(Visit(node->Object()));
  RECURSE(Visit(node->Index()));
}

template <typename Subclass>
inline void AstTraversalVisitor<Subclass>::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  PROCESS_NODE(node);
  for (auto *param : node->Parameters()) {
    RECURSE(Visit(param));
  }
  RECURSE(Visit(node->ReturnType()));
}
// \endcond

}  // namespace noisepage::execution::ast
