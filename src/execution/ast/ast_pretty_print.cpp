#include "execution/ast/ast_pretty_print.h"

#include <iostream>

#include "execution/ast/ast.h"
#include "execution/ast/ast_visitor.h"

namespace noisepage::execution::ast {

namespace {

class AstPrettyPrintImpl : public AstVisitor<AstPrettyPrintImpl> {
 public:
  explicit AstPrettyPrintImpl(std::ostream &os, AstNode *root) : os_(os), root_(root), indent_(0) {}

  void Run() { Visit(root_); }

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

 private:
  void IncreaseIndent() { indent_ += 4; }

  void DecreaseIndent() { indent_ -= 4; }

  void NewLine() {
    os_ << std::endl;
    for (int32_t i = 0; i < indent_; i++) {
      os_ << " ";
    }
  }

 private:
  // The output stream
  std::ostream &os_;
  // The AST
  AstNode *root_;
  // Current indent level
  int32_t indent_;
};

void AstPrettyPrintImpl::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  os_ << "[";
  if (node->HasLength()) {
    os_ << node->Length()->As<ast::LitExpr>()->Int64Val();
  } else {
    os_ << "*";
  }
  os_ << "]";
  Visit(node->ElementType());
}

void AstPrettyPrintImpl::VisitBadExpr(BadExpr *node) { NOISEPAGE_ASSERT(false, "Invalid"); }

void AstPrettyPrintImpl::VisitBlockStmt(BlockStmt *node) {
  if (node->IsEmpty()) {
    os_ << "{ }";
    return;
  }

  os_ << "{";
  IncreaseIndent();
  NewLine();

  bool first = true;
  for (auto *stmt : node->Statements()) {
    if (!first) NewLine();
    first = false;
    Visit(stmt);
  }

  DecreaseIndent();
  NewLine();
  os_ << "}";
}

void AstPrettyPrintImpl::VisitCallExpr(CallExpr *node) {
  if (node->IsBuiltinCall()) os_ << "@";
  Visit(node->Function());
  os_ << "(";
  bool first = true;
  for (auto *arg : node->Arguments()) {
    if (!first) os_ << ", ";
    first = false;
    Visit(arg);
  }
  os_ << ")";
}

void AstPrettyPrintImpl::VisitFieldDecl(FieldDecl *node) {
  os_ << node->Name().GetView() << ": ";
  Visit(node->TypeRepr());
}

void AstPrettyPrintImpl::VisitFunctionDecl(FunctionDecl *node) {
  os_ << "fun " << node->Name().GetView();
  Visit(node->Function());
  NewLine();
}

void AstPrettyPrintImpl::VisitIdentifierExpr(IdentifierExpr *node) { os_ << node->Name().GetView(); }

void AstPrettyPrintImpl::VisitImplicitCastExpr(ImplicitCastExpr *node) {
  os_ << CastKindToString(node->GetCastKind()) << "(";
  Visit(node->Input());
  os_ << ")";
}

void AstPrettyPrintImpl::VisitReturnStmt(ReturnStmt *node) {
  os_ << "return";
  if (node->Ret() != nullptr) {
    os_ << " ";
    Visit(node->Ret());
  }
}

void AstPrettyPrintImpl::VisitStructDecl(StructDecl *node) {
  os_ << "struct " << node->Name().GetView() << " {";
  IncreaseIndent();
  NewLine();
  Visit(node->TypeRepr());
  DecreaseIndent();
  NewLine();
  os_ << "}";
  NewLine();
}

void AstPrettyPrintImpl::VisitUnaryOpExpr(UnaryOpExpr *node) {
  os_ << parsing::Token::GetString(node->Op());
  Visit(node->Input());
}

void AstPrettyPrintImpl::VisitVariableDecl(VariableDecl *node) {
  os_ << "var " << node->Name().GetView();
  if (node->TypeRepr() != nullptr) {
    os_ << ": ";
    Visit(node->TypeRepr());
  }
  if (node->Initial() != nullptr) {
    os_ << " = ";
    Visit(node->Initial());
  }
}

void AstPrettyPrintImpl::VisitAssignmentStmt(AssignmentStmt *node) {
  Visit(node->Destination());
  os_ << " = ";
  Visit(node->Source());
}

void AstPrettyPrintImpl::VisitFile(File *node) {
  for (auto *decl : node->Declarations()) {
    Visit(decl);
  }
}

void AstPrettyPrintImpl::VisitFunctionLitExpr(FunctionLitExpr *node) {
  Visit(node->TypeRepr());
  os_ << " ";
  Visit(node->Body());
  NewLine();
}

void AstPrettyPrintImpl::VisitForStmt(ForStmt *node) {
  os_ << "for (";
  if (node->Init() != nullptr || node->Next() != nullptr) {
    // Standard
    if (node->Init() != nullptr) Visit(node->Init());
    os_ << "; ";
    Visit(node->Condition());
    os_ << "; ";
    if (node->Next() != nullptr) Visit(node->Next());
  } else if (node->Condition() != nullptr) {
    // While
    Visit(node->Condition());
  } else {
    // Unconditional loop
  }
  os_ << ") ";
  Visit(node->Body());
}

void AstPrettyPrintImpl::VisitForInStmt(ForInStmt *node) {
  os_ << "for (";
  Visit(node->Target());
  os_ << " in ";
  Visit(node->Iterable());
  os_ << ")";
  Visit(node->Body());
}

void AstPrettyPrintImpl::VisitBinaryOpExpr(BinaryOpExpr *node) {
  Visit(node->Left());
  os_ << " " << parsing::Token::GetString(node->Op()) << " ";
  Visit(node->Right());
}

void AstPrettyPrintImpl::VisitMapTypeRepr(MapTypeRepr *node) {
  os_ << "map[";
  Visit(node->KeyType());
  os_ << "]";
  Visit(node->ValType());
}

void AstPrettyPrintImpl::VisitLitExpr(LitExpr *node) {
  switch (node->GetLiteralKind()) {
    case LitExpr::LitKind::Nil:
      os_ << "nil";
      break;
    case LitExpr::LitKind::Boolean:
      os_ << (node->BoolVal() ? "true" : "false");
      break;
    case LitExpr::LitKind::Int:
      os_ << node->Int64Val();
      break;
    case LitExpr::LitKind::Float:
      os_ << node->Float64Val();
      break;
    case LitExpr::LitKind::String:
      os_ << "\"" << node->StringVal().GetView() << "\"";
      break;
  }
}

void AstPrettyPrintImpl::VisitStructTypeRepr(StructTypeRepr *node) {
  // We want to ensure all types are aligned. Pre-process the fields to
  // find longest field names, then align as appropriate.

  std::size_t longest_field_len = 0;
  for (const auto *field : node->Fields()) {
    longest_field_len = std::max(longest_field_len, field->Name().GetLength());
  }

  bool first = true;
  for (const auto *field : node->Fields()) {
    if (!first) NewLine();
    first = false;
    os_ << field->Name().GetView();
    const std::size_t padding = longest_field_len - field->Name().GetLength();
    os_ << std::string(padding, ' ') << ": ";
    Visit(field->TypeRepr());
  }
}

void AstPrettyPrintImpl::VisitDeclStmt(DeclStmt *node) { Visit(node->Declaration()); }

void AstPrettyPrintImpl::VisitMemberExpr(MemberExpr *node) {
  Visit(node->Object());
  os_ << ".";
  Visit(node->Member());
}

void AstPrettyPrintImpl::VisitPointerTypeRepr(PointerTypeRepr *node) {
  os_ << "*";
  Visit(node->Base());
}

void AstPrettyPrintImpl::VisitComparisonOpExpr(ComparisonOpExpr *node) {
  Visit(node->Left());
  os_ << " " << parsing::Token::GetString(node->Op()) << " ";
  Visit(node->Right());
}

void AstPrettyPrintImpl::VisitIfStmt(IfStmt *node) {
  os_ << "if (";
  Visit(node->Condition());
  os_ << ") ";
  Visit(node->ThenStmt());
  if (node->ElseStmt() != nullptr) {
    os_ << " else ";
    Visit(node->ElseStmt());
  }
}

void AstPrettyPrintImpl::VisitExpressionStmt(ExpressionStmt *node) { Visit(node->Expression()); }

void AstPrettyPrintImpl::VisitIndexExpr(IndexExpr *node) {
  Visit(node->Object());
  os_ << "[";
  Visit(node->Index());
  os_ << "]";
}

void AstPrettyPrintImpl::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  os_ << "(";
  bool first = true;
  for (const auto &param : node->Parameters()) {
    if (!first) os_ << ", ";
    first = false;
    os_ << param->Name().GetView() << ": ";
    Visit(param->TypeRepr());
  }
  os_ << ") -> ";
  Visit(node->ReturnType());
}

}  // namespace

void AstPrettyPrint::Dump(std::ostream &os, AstNode *node) {
  AstPrettyPrintImpl printer(os, node);
  printer.Run();
  os << std::endl;
}

}  // namespace noisepage::execution::ast
