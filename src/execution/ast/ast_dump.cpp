#include "execution/ast/ast_dump.h"

#include <llvm/ADT/SmallString.h>
#include <llvm/Support/raw_os_ostream.h>
#include <llvm/Support/raw_ostream.h>

#include <string>
#include <utility>

#include "execution/ast/ast.h"
#include "execution/ast/ast_visitor.h"
#include "execution/ast/type.h"

namespace noisepage::execution::ast {

class AstDumperImpl : public AstVisitor<AstDumperImpl> {
 public:
  explicit AstDumperImpl(AstNode *root, llvm::raw_ostream &out)
      : root_(root), top_level_(true), first_child_(true), out_(out) {}

  void Run() { Visit(root_); }

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

 private:
  class WithColor {
   public:
    WithColor(AstDumperImpl *impl, llvm::raw_ostream::Colors color) : impl_(impl) { impl->out_.changeColor(color); }
    ~WithColor() { impl_->out_.resetColor(); }

   private:
    AstDumperImpl *impl_;
  };

  void DumpKind(AstNode *node) {
    WithColor color(this, llvm::raw_ostream::CYAN);
    out_ << " " << node->KindName();
  }

  void DumpPointer(const void *p) {
    WithColor color(this, llvm::raw_ostream::YELLOW);
    out_ << " (" << p << ")";
  }

  void DumpType(Type *type) {
    WithColor color(this, llvm::raw_ostream::GREEN);
    out_ << " '" << Type::ToString(type) << "'";
  }

  void DumpPosition(const SourcePosition &pos) {
    out_ << " <";
    {
      WithColor color(this, llvm::raw_ostream::YELLOW);
      out_ << "line:" << pos.line_ << ":" << pos.column_;
    }
    out_ << ">";
  }

  void DumpNodeCommon(AstNode *node) {
    DumpKind(node);
    DumpPointer(node);
    DumpPosition(node->Position());
    out_ << " ";
  }

  void DumpExpressionCommon(Expr *expr) {
    DumpNodeCommon(expr);
    if (expr->GetType() != nullptr) {
      DumpType(expr->GetType());
      out_ << " ";
    }
  }

  void DumpToken(parsing::Token::Type type) { out_ << "'" << parsing::Token::GetString(type) << "'"; }

  template <typename T>
  void DumpPrimitive(const T &val) {
    out_ << val;
  }

  void DumpIdentifier(Identifier str) { out_.write(str.GetData(), str.GetLength()); }

  template <typename Fn>
  void DumpChild(Fn dump_fn) {
    if (top_level_) {
      top_level_ = false;
      dump_fn();
      while (!pending_.empty()) {
        pending_.pop_back_val()(true);
      }
      prefix_.clear();
      out_ << "\n";
      top_level_ = true;
    }

    auto dump_with_prefix = [this, dump_fn](bool last_child) {
      {
        WithColor color(this, llvm::raw_ostream::BLUE);
        out_ << "\n";
        out_ << prefix_ << " " << (last_child ? "└" : "├") << "—";
        prefix_.append(last_child ? "  " : " |").append(" ");
      }

      first_child_ = true;
      auto depth = pending_.size();

      dump_fn();

      while (depth < pending_.size()) {
        pending_.pop_back_val()(true);
      }

      prefix_.resize(prefix_.size() - 3);
    };

    if (first_child_) {
      pending_.emplace_back(dump_with_prefix);
    } else {
      pending_.back()(false);
      pending_.back() = std::move(dump_with_prefix);
    }
    first_child_ = false;
  }

  void DumpDecl(Decl *decl) {
    DumpChild([=] { AstVisitor<AstDumperImpl>::Visit(decl); });
  }

  void DumpExpr(Expr *expr) {
    DumpChild([=] { AstVisitor<AstDumperImpl>::Visit(expr); });
  }

  void DumpStmt(Stmt *stmt) {
    DumpChild([=] { AstVisitor<AstDumperImpl>::Visit(stmt); });
  }

 private:
  AstNode *root_;

  std::string prefix_;

  bool top_level_;
  bool first_child_;

  llvm::SmallVector<std::function<void(bool)>, 32> pending_;

  llvm::raw_ostream &out_;
};

void AstDumperImpl::VisitFile(File *node) {
  DumpNodeCommon(node);
  DumpChild([=] {
    for (auto *decl : node->Declarations()) {
      DumpDecl(decl);
    }
  });
}

void AstDumperImpl::VisitFieldDecl(FieldDecl *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->Name());
  DumpExpr(node->TypeRepr());
}

void AstDumperImpl::VisitFunctionDecl(FunctionDecl *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->Name());
  DumpExpr(node->Function());
}

void AstDumperImpl::VisitVariableDecl(VariableDecl *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->Name());
  if (node->HasTypeDecl() && node->TypeRepr()->GetType() != nullptr) {
    DumpType(node->TypeRepr()->GetType());
  }
  if (node->HasInitialValue()) {
    DumpExpr(node->Initial());
  }
}

void AstDumperImpl::VisitStructDecl(StructDecl *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->Name());
  for (auto *field : node->TypeRepr()->As<StructTypeRepr>()->Fields()) {
    DumpDecl(field);
  }
}

void AstDumperImpl::VisitAssignmentStmt(AssignmentStmt *node) {
  DumpNodeCommon(node);
  DumpExpr(node->Destination());
  DumpExpr(node->Source());
}

void AstDumperImpl::VisitBlockStmt(BlockStmt *node) {
  DumpNodeCommon(node);
  for (auto *stmt : node->Statements()) {
    DumpStmt(stmt);
  }
}

void AstDumperImpl::VisitDeclStmt(DeclStmt *node) { AstVisitor<AstDumperImpl>::Visit(node->Declaration()); }

void AstDumperImpl::VisitExpressionStmt(ExpressionStmt *node) { AstVisitor<AstDumperImpl>::Visit(node->Expression()); }

void AstDumperImpl::VisitForStmt(ForStmt *node) {
  DumpNodeCommon(node);
  if (node->Init() != nullptr) {
    DumpStmt(node->Init());
  }
  if (node->Condition() != nullptr) {
    DumpExpr(node->Condition());
  }
  if (node->Next() != nullptr) {
    DumpStmt(node->Next());
  }
  DumpStmt(node->Body());
}

void AstDumperImpl::VisitForInStmt(ForInStmt *node) {
  DumpNodeCommon(node);
  DumpExpr(node->Target());
  DumpExpr(node->Iterable());
  DumpStmt(node->Body());
}

void AstDumperImpl::VisitIfStmt(IfStmt *node) {
  DumpNodeCommon(node);
  DumpExpr(node->Condition());
  DumpStmt(node->ThenStmt());
  if (node->HasElseStmt()) {
    DumpStmt(node->ElseStmt());
  }
}

void AstDumperImpl::VisitReturnStmt(ReturnStmt *node) {
  DumpNodeCommon(node);
  if (node->Ret() != nullptr) {
    DumpExpr(node->Ret());
  }
}

void AstDumperImpl::VisitCallExpr(CallExpr *node) {
  DumpExpressionCommon(node);

  DumpPrimitive("<");
  {
    WithColor color(this, llvm::raw_ostream::Colors::RED);
    switch (node->GetCallKind()) {
      case CallExpr::CallKind::Builtin: {
        out_ << "Builtin";
        break;
      }
      case CallExpr::CallKind::Regular: {
        out_ << "Regular";
      }
    }
  }
  DumpPrimitive("> ");

  DumpExpr(node->Function());
  for (auto *expr : node->Arguments()) {
    DumpExpr(expr);
  }
}

void AstDumperImpl::VisitBinaryOpExpr(BinaryOpExpr *node) {
  DumpExpressionCommon(node);
  DumpToken(node->Op());
  DumpExpr(node->Left());
  DumpExpr(node->Right());
}

void AstDumperImpl::VisitComparisonOpExpr(ComparisonOpExpr *node) {
  DumpExpressionCommon(node);
  DumpToken(node->Op());
  DumpExpr(node->Left());
  DumpExpr(node->Right());
}

void AstDumperImpl::VisitFunctionLitExpr(FunctionLitExpr *node) {
  DumpExpressionCommon(node);
  DumpStmt(node->Body());
}

void AstDumperImpl::VisitIdentifierExpr(IdentifierExpr *node) {
  DumpExpressionCommon(node);
  DumpIdentifier(node->Name());
}

void AstDumperImpl::VisitImplicitCastExpr(ImplicitCastExpr *node) {
  DumpExpressionCommon(node);
  DumpPrimitive("<");
  {
    WithColor color(this, llvm::raw_ostream::Colors::RED);
    DumpPrimitive(CastKindToString(node->GetCastKind()));
  }
  DumpPrimitive(">");
  DumpExpr(node->Input());
}

void AstDumperImpl::VisitIndexExpr(IndexExpr *node) {
  DumpExpressionCommon(node);
  DumpExpr(node->Object());
  DumpExpr(node->Index());
}

void AstDumperImpl::VisitLitExpr(LitExpr *node) {
  DumpExpressionCommon(node);
  switch (node->GetLiteralKind()) {
    case LitExpr::LitKind::Nil:
      DumpPrimitive("nil");
      break;
    case LitExpr::LitKind::Boolean:
      DumpPrimitive(node->BoolVal() ? "'true'" : "'false'");
      break;
    case LitExpr::LitKind::Int:
      DumpPrimitive(node->Int64Val());
      break;
    case LitExpr::LitKind::Float:
      DumpPrimitive(node->Float64Val());
      break;
    case LitExpr::LitKind::String:
      DumpIdentifier(node->StringVal());
      break;
  }
}

void AstDumperImpl::VisitMemberExpr(MemberExpr *node) {
  DumpExpressionCommon(node);
  DumpExpr(node->Object());
  DumpExpr(node->Member());
}

void AstDumperImpl::VisitUnaryOpExpr(UnaryOpExpr *node) {
  DumpExpressionCommon(node);
  DumpToken(node->Op());
  DumpExpr(node->Input());
}

void AstDumperImpl::VisitBadExpr(BadExpr *node) {
  DumpNodeCommon(node);
  DumpPrimitive("BAD EXPRESSION @ ");
  DumpPosition(node->Position());
}

void AstDumperImpl::VisitStructTypeRepr(StructTypeRepr *node) {
  DumpNodeCommon(node);
  DumpType(node->GetType());
}

void AstDumperImpl::VisitPointerTypeRepr(PointerTypeRepr *node) {
  DumpNodeCommon(node);
  DumpExpr(node->Base());
}

void AstDumperImpl::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  DumpNodeCommon(node);
  DumpType(node->GetType());
}

void AstDumperImpl::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  DumpNodeCommon(node);
  DumpExpr(node->Length());
  DumpExpr(node->ElementType());
}

void AstDumperImpl::VisitMapTypeRepr(MapTypeRepr *node) {
  DumpNodeCommon(node);
  DumpExpr(node->KeyType());
  DumpExpr(node->ValType());
}

std::string AstDump::Dump(AstNode *node) {
  llvm::SmallString<256> buffer;
  llvm::raw_svector_ostream stream(buffer);
  AstDumperImpl print(node, stream);
  print.Run();
  return buffer.str();
}

}  // namespace noisepage::execution::ast
