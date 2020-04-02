#include "execution/ast/ast_pretty_print.h"

#include <iostream>

#include "execution/ast/ast_visitor.h"

namespace terrier::execution::ast {

std::string CastKindToInt(CastKind x) {
  switch (x) {
    // Conversion of a 32-bit integer into a non-nullable SQL Integer value
    case CastKind::IntToSqlInt:
      return "IntToSqlInt";

      // Conversion of a 32-bit integer into a non-nullable SQL Decimal value
    case CastKind::IntToSqlDecimal:
      return "IntToSqlDecimal";

      // Conversion of a SQL boolean value (potentially nullable) into a primitive
      // boolean value
    case CastKind::SqlBoolToBool:
      return "SqlBoolToBool";

      // Conversion of a primitive boolean into a SQL boolean
    case CastKind::BoolToSqlBool:
      return "BoolToSqlBool";

      // A cast between integral types (i.e., 8-bit, 16-bit, 32-bit, or 64-bit
      // numbers), excluding to boolean! Boils down to a bitcast, a truncation:
      // a sign-extension, or a zero-extension. The same as in C/C++.
    case CastKind::IntegralCast:
      return "IntegralCast";

      // An integer to float cast. Only allows widening.
    case CastKind::IntToFloat:
      return "IntToFloat";

      // A float to integer cast. Only allows widening.
    case CastKind::FloatToInt:
      return "FloatToInt";

      // A simple bit cast reinterpretation
    case CastKind::BitCast:
      return "BitCast";

      // 64 bit float To Sql Real
    case CastKind::FloatToSqlReal:
      return "FloatToSqlReal";

      // Conversion of a SQL timestamp value (potentially nullable) into a primitive timestamp value
    case CastKind::SqlTimestampToTimestamp:
      return "SqlTimestampToTimestamp";

      // Conversion of a primitive timestamp valueinto a SQL timestamp
    case CastKind::TimestampToSqlTimestamp:
      return "TimestampToSqlTimestamp";

      // Convert a SQL integer into a SQL real
    case CastKind::SqlIntToSqlReal:
      return "SqlIntToSqlReal";
    default :
      return "default";
  }
}

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
    os_ << node->Length();
  } else {
    os_ << "*";
  }
  os_ << "]";
  Visit(node->ElementType());
}

void AstPrettyPrintImpl::VisitBadExpr(BadExpr *node) { TERRIER_ASSERT(false, "Invalid"); }

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
  if (node->GetCallKind() == terrier::execution::ast::CallExpr::CallKind::Builtin) os_ << "@";
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
  os_ << std::string(node->Name().Data()) << ": ";
  Visit(node->TypeRepr());
}

void AstPrettyPrintImpl::VisitFunctionDecl(FunctionDecl *node) {
  os_ << "fun " << std::string(node->Name().Data());
  Visit(node->Function());
  NewLine();
}

void AstPrettyPrintImpl::VisitIdentifierExpr(IdentifierExpr *node) {
  os_ << std::string(node->Name().Data());
}

void AstPrettyPrintImpl::VisitImplicitCastExpr(ImplicitCastExpr *node) {
  os_ << "(Do not know what comes here)"<< (terrier::execution::ast::CastKindToInt(node->GetCastKind())) << "(";
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
  os_ << "struct " << std::string(node->Name().Data()) << " {";
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
  Visit(node->Expression());
}

void AstPrettyPrintImpl::VisitVariableDecl(VariableDecl *node) {
  os_ << "var " << std::string(node->Name().Data());
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
  Visit(node->Iter());
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
  Visit(node->Key());
  os_ << "]";
  Visit(node->Val());
}

void AstPrettyPrintImpl::VisitLitExpr(LitExpr *node) {
  switch (node->LiteralKind()) {
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
      os_ << "\"" << std::string(node->RawStringVal().Data()) << "\"";
      break;
  }
}

void AstPrettyPrintImpl::VisitStructTypeRepr(StructTypeRepr *node) {
  // We want to ensure all types are aligned. Pre-process the fields to
  // find longest field names, then align as appropriate.

  std::size_t longest_field_len = 0;
  for (const auto *field : node->Fields()) {
    longest_field_len = std::max(longest_field_len, field->Name().Length());
  }

  bool first = true;
  for (const auto *field : node->Fields()) {
    if (!first) NewLine();
    first = false;

    os_ << std::string(field->Name().Data());
    const std::size_t padding = longest_field_len - field->Name().Length();
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
  if (node->ElseStmt()) {
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
  for (const auto &param : node->Paramaters()) {
    if (!first) os_ << ", ";
    first = false;
    os_ << std::string(param->Name().Data()) << ": ";
    Visit(param->TypeRepr());
  }
  os_ << ") -> ";
  Visit(node->ReturnType());
}

void AstPrettyPrint::Dump(std::ostream &os, AstNode *node) {
  AstPrettyPrintImpl printer(os, node);
  printer.Run();
  os << std::endl;
}

}  // namespace tpl::ast
