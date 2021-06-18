#include <string>
#include <utility>

#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallString.h"
#include "llvm/Support/raw_os_ostream.h"
#include "llvm/Support/raw_ostream.h"

#include "execution/ast/ast.h"
#include "execution/ast/ast_clone.h"
#include "execution/ast/ast_visitor.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"

namespace noisepage::execution::ast {

class AstCloneImpl : public AstVisitor<AstCloneImpl, AstNode *> {
 public:
  explicit AstCloneImpl(AstNode *root, AstNodeFactory *factory, Context *old_context, Context *new_context)
      : root_(root), factory_{factory}, old_context_{old_context}, new_context_{new_context} {}

  AstNode *Run() { return Visit(root_); }

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) AstNode *Visit##type(type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  Identifier CloneIdentifier(const Identifier &ident) { return new_context_->GetIdentifier(ident.GetData()); }

  Identifier CloneIdentifier(const Identifier &&ident) {
    (void)old_context_;
    return new_context_->GetIdentifier(ident.GetData());
  }

 private:
  /** The root of the AST to clone. */
  AstNode *root_;

  /** The AST node factory used to allocate new nodes. */
  AstNodeFactory *factory_;

  /** The AST context of the source AST. */
  Context *old_context_;

  /** The AST context of the destination AST. */
  Context *new_context_;
};

AstNode *AstCloneImpl::VisitFile(File *node) {
  util::RegionVector<Decl *> decls(new_context_->GetRegion());
  for (auto *decl : node->Declarations()) {
    decls.push_back(reinterpret_cast<Decl *>(Visit(decl)));
  }
  return factory_->NewFile(node->Position(), std::move(decls));
}

AstNode *AstCloneImpl::VisitFieldDecl(FieldDecl *node) {
  return factory_->NewFieldDecl(node->Position(), CloneIdentifier(node->Name()),
                                reinterpret_cast<Expr *>(Visit(node->TypeRepr())));
}

AstNode *AstCloneImpl::VisitFunctionDecl(FunctionDecl *node) {
  return factory_->NewFunctionDecl(node->Position(), CloneIdentifier(node->Name()),
                                   reinterpret_cast<FunctionLitExpr *>(VisitFunctionLitExpr(node->Function())));
}

AstNode *AstCloneImpl::VisitVariableDecl(VariableDecl *node) {
  return factory_->NewVariableDecl(
      node->Position(), CloneIdentifier(node->Name()),
      node->TypeRepr() == nullptr ? nullptr : reinterpret_cast<Expr *>(Visit(node->TypeRepr())),
      node->Initial() == nullptr ? nullptr : reinterpret_cast<Expr *>(Visit(node->Initial())));
}

AstNode *AstCloneImpl::VisitStructDecl(StructDecl *node) {
  return factory_->NewStructDecl(
      node->Position(), CloneIdentifier(node->Name()),
      reinterpret_cast<StructTypeRepr *>(VisitStructTypeRepr(reinterpret_cast<StructTypeRepr *>(node->TypeRepr()))));
}

AstNode *AstCloneImpl::VisitAssignmentStmt(AssignmentStmt *node) {
  return factory_->NewAssignmentStmt(node->Position(), reinterpret_cast<Expr *>(Visit(node->Destination())),
                                     reinterpret_cast<Expr *>(Visit(node->Source())));
}

AstNode *AstCloneImpl::VisitBlockStmt(BlockStmt *node) {
  util::RegionVector<ast::Stmt *> stmts(new_context_->GetRegion());
  for (auto *stmt : node->Statements()) {
    stmts.push_back(reinterpret_cast<ast::Stmt *>(Visit(stmt)));
  }
  return factory_->NewBlockStmt(node->Position(), node->RightBracePosition(), std::move(stmts));
}

AstNode *AstCloneImpl::VisitDeclStmt(DeclStmt *node) {
  return factory_->NewDeclStmt(reinterpret_cast<Decl *>(Visit(node->Declaration())));
}

AstNode *AstCloneImpl::VisitExpressionStmt(ExpressionStmt *node) {
  return factory_->NewExpressionStmt(reinterpret_cast<Expr *>(Visit(node->Expression())));
}

AstNode *AstCloneImpl::VisitForStmt(ForStmt *node) {
  auto init = node->Init() == nullptr ? nullptr : reinterpret_cast<Stmt *>(Visit(node->Init()));
  auto next = node->Next() == nullptr ? nullptr : reinterpret_cast<Stmt *>(Visit(node->Next()));
  return factory_->NewForStmt(node->Position(), init, reinterpret_cast<Expr *>(Visit(node->Condition())), next,
                              reinterpret_cast<BlockStmt *>(VisitBlockStmt(node->Body())));
}

AstNode *AstCloneImpl::VisitForInStmt(ForInStmt *node) {
  return factory_->NewForInStmt(node->Position(), reinterpret_cast<Expr *>(Visit(node->Target())),
                                reinterpret_cast<Expr *>(Visit(node->Iterable())),
                                reinterpret_cast<BlockStmt *>(VisitBlockStmt(node->Body())));
}

AstNode *AstCloneImpl::VisitIfStmt(IfStmt *node) {
  auto *else_stmt = node->ElseStmt() == nullptr ? nullptr : reinterpret_cast<Stmt *>(Visit((node->ElseStmt())));
  return factory_->NewIfStmt(node->Position(), reinterpret_cast<Expr *>(Visit(node->Condition())),
                             reinterpret_cast<BlockStmt *>(VisitBlockStmt(node->ThenStmt())), else_stmt);
}

AstNode *AstCloneImpl::VisitReturnStmt(ReturnStmt *node) {
  if (node->Ret() == nullptr) {
    return factory_->NewReturnStmt(node->Position(), nullptr);
  } else {
    return factory_->NewReturnStmt(node->Position(), reinterpret_cast<Expr *>(Visit(node->Ret())));
  }
}

AstNode *AstCloneImpl::VisitCallExpr(CallExpr *node) {
  util::RegionVector<Expr *> args(new_context_->GetRegion());

  for (auto *arg : node->Arguments()) {
    args.push_back(reinterpret_cast<Expr *>(Visit(arg)));
  }
  if (node->GetCallKind() == CallExpr::CallKind::Builtin) {
    return factory_->NewBuiltinCallExpr(reinterpret_cast<Expr *>(Visit(node->Function())), std::move(args));
  }
  return factory_->NewCallExpr(reinterpret_cast<Expr *>(Visit(node->Function())), std::move(args));
}

AstNode *AstCloneImpl::VisitBinaryOpExpr(BinaryOpExpr *node) {
  return factory_->NewBinaryOpExpr(node->Position(), node->Op(), reinterpret_cast<Expr *>(Visit(node->Left())),
                                   reinterpret_cast<Expr *>(Visit(node->Right())));
}

AstNode *AstCloneImpl::VisitComparisonOpExpr(ComparisonOpExpr *node) {
  return factory_->NewComparisonOpExpr(node->Position(), node->Op(), reinterpret_cast<Expr *>(Visit(node->Left())),
                                       reinterpret_cast<Expr *>(Visit(node->Right())));
}

AstNode *AstCloneImpl::VisitFunctionLitExpr(FunctionLitExpr *node) {
  return factory_->NewFunctionLitExpr(reinterpret_cast<FunctionTypeRepr *>(VisitFunctionTypeRepr(node->TypeRepr())),
                                      reinterpret_cast<BlockStmt *>(VisitBlockStmt(node->Body())));
}

AstNode *AstCloneImpl::VisitIdentifierExpr(IdentifierExpr *node) {
  return factory_->NewIdentifierExpr(node->Position(), CloneIdentifier(node->Name()));
}

AstNode *AstCloneImpl::VisitImplicitCastExpr(ImplicitCastExpr *node) { return Visit(node->Input()); }

AstNode *AstCloneImpl::VisitIndexExpr(IndexExpr *node) {
  return factory_->NewIndexExpr(node->Position(), reinterpret_cast<Expr *>(Visit(node->Object())),
                                reinterpret_cast<Expr *>(Visit(node->Index())));
}

AstNode *AstCloneImpl::VisitLambdaExpr(LambdaExpr *node) {
  util::RegionVector<ast::Expr *> capture_idents(new_context_->GetRegion());
  for (auto ident : node->GetCaptureIdents()) {
    capture_idents.push_back(reinterpret_cast<ast::Expr *>(Visit(ident)));
  }
  return factory_->NewLambdaExpr(node->Position(),
                                 reinterpret_cast<FunctionLitExpr *>(Visit(node->GetFunctionLitExpr())),
                                 std::move(capture_idents));
}

AstNode *AstCloneImpl::VisitLitExpr(LitExpr *node) {
  AstNode *literal = nullptr;
  switch (node->GetLiteralKind()) {
    case LitExpr::LitKind::Nil: {
      literal = factory_->NewNilLiteral(node->Position());
      break;
    }
    case LitExpr::LitKind::Boolean: {
      literal = factory_->NewBoolLiteral(node->Position(), node->BoolVal());
      break;
    }
    case LitExpr::LitKind::Int: {
      literal = factory_->NewIntLiteral(node->Position(), node->Int64Val());
      break;
    }
    case LitExpr::LitKind::Float: {
      literal = factory_->NewFloatLiteral(node->Position(), node->Float64Val());
      break;
    }
    case LitExpr::LitKind::String: {
      literal = factory_->NewStringLiteral(node->Position(), CloneIdentifier(node->StringVal()));
      break;
    }
  }
  NOISEPAGE_ASSERT(literal != nullptr, "Unknown literal kind");
  return literal;
}

AstNode *AstCloneImpl::VisitBreakStmt(BreakStmt *node) { return factory_->NewBreakStmt(node->Position()); }

AstNode *AstCloneImpl::VisitMemberExpr(MemberExpr *node) {
  return factory_->NewMemberExpr(node->Position(), reinterpret_cast<Expr *>(Visit(node->Object())),
                                 reinterpret_cast<Expr *>(Visit(node->Member())));
}

AstNode *AstCloneImpl::VisitUnaryOpExpr(UnaryOpExpr *node) {
  return factory_->NewUnaryOpExpr(node->Position(), node->Op(), reinterpret_cast<Expr *>(Visit(node->Input())));
}

AstNode *AstCloneImpl::VisitBadExpr(BadExpr *node) { return factory_->NewBadExpr(node->Position()); }

AstNode *AstCloneImpl::VisitStructTypeRepr(StructTypeRepr *node) {
  util::RegionVector<FieldDecl *> field_decls(new_context_->GetRegion());
  field_decls.reserve(node->Fields().size());
  for (auto field : node->Fields()) {
    field_decls.push_back(reinterpret_cast<FieldDecl *>((VisitFieldDecl(field))));
  }
  return factory_->NewStructType(node->Position(), std::move(field_decls));
}

AstNode *AstCloneImpl::VisitPointerTypeRepr(PointerTypeRepr *node) {
  return factory_->NewPointerType(node->Position(), reinterpret_cast<Expr *>(Visit(node->Base())));
}

AstNode *AstCloneImpl::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  util::RegionVector<FieldDecl *> params(new_context_->GetRegion());
  for (auto *param : node->Parameters()) {
    params.push_back(reinterpret_cast<FieldDecl *>(VisitFieldDecl(param)));
  }

  return factory_->NewFunctionType(node->Position(), std::move(params),
                                   reinterpret_cast<Expr *>(Visit(node->ReturnType())));
}

AstNode *AstCloneImpl::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  return factory_->NewArrayType(node->Position(), reinterpret_cast<Expr *>(Visit(node->Length())),
                                reinterpret_cast<Expr *>(Visit(node->ElementType())));
}

AstNode *AstCloneImpl::VisitMapTypeRepr(MapTypeRepr *node) {
  return factory_->NewMapType(node->Position(), reinterpret_cast<Expr *>(Visit(node->KeyType())),
                              reinterpret_cast<Expr *>(Visit(node->ValType())));
}

AstNode *AstCloneImpl::VisitLambdaTypeRepr(LambdaTypeRepr *node) {
  return factory_->NewLambdaType(node->Position(), reinterpret_cast<Expr *>(Visit(node->FunctionType())));
}

AstNode *AstClone::Clone(AstNode *node, AstNodeFactory *factory, Context *old_context, Context *new_context) {
  AstCloneImpl cloner{node, factory, old_context, new_context};
  return cloner.Run();
}

}  // namespace noisepage::execution::ast
