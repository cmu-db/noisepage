#include "execution/ast/ast_dump.h"

#include <iostream>
#include <string>
#include <utility>

#include "llvm/Support/raw_os_ostream.h"

#include "execution/ast/ast.h"
#include "execution/ast/ast_visitor.h"
#include "execution/ast/type.h"

namespace terrier::ast {

class AstDumperImpl : public AstVisitor<AstDumperImpl> {
 public:
  explicit AstDumperImpl(AstNode *root, int out_fd)
      : root_(root), top_level_(true), first_child_(true), out_(out_fd, false) {}

  void Run() { Visit(root_); }

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

 private:
  class WithColor {
   public:
    WithColor(AstDumperImpl *impl, llvm::raw_ostream::Colors color) : impl(impl) { impl->out_.changeColor(color); }
    ~WithColor() { impl->out_.resetColor(); }

   private:
    AstDumperImpl *impl;
  };

  void DumpKind(AstNode *node) {
    WithColor color(this, llvm::raw_ostream::CYAN);
    out_ << " " << node->kind_name();
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
      out_ << "line:" << pos.line << ":" << pos.column;
    }
    out_ << ">";
  }

  void DumpNodeCommon(AstNode *node) {
    DumpKind(node);
    DumpPointer(node);
    DumpPosition(node->position());
    out_ << " ";
  }

  void DumpExpressionCommon(Expr *expr) {
    DumpNodeCommon(expr);
    if (expr->type() != nullptr) {
      DumpType(expr->type());
      out_ << " ";
    }
  }

  void DumpToken(parsing::Token::Type type) { out_ << "'" << parsing::Token::GetString(type) << "'"; }

  template <typename T>
  void DumpPrimitive(const T &val) {
    out_ << val;
  }

  void DumpIdentifier(Identifier str) { out_.write(str.data(), str.length()); }

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
        out_ << prefix_ << (last_child ? "`" : "|") << "-";
        prefix_.append(last_child ? " " : "|").append(" ");
      }

      first_child_ = true;
      auto depth = pending_.size();

      dump_fn();

      while (depth < pending_.size()) {
        pending_.pop_back_val()(true);
      }

      prefix_.resize(prefix_.size() - 2);
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

  llvm::raw_fd_ostream out_;
};

void AstDumperImpl::VisitFile(File *node) {
  DumpNodeCommon(node);
  DumpChild([=] {
    for (auto *decl : node->declarations()) {
      DumpDecl(decl);
    }
  });
}

void AstDumperImpl::VisitFieldDecl(FieldDecl *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->name());
  DumpExpr(node->type_repr());
}

void AstDumperImpl::VisitFunctionDecl(FunctionDecl *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->name());
  DumpExpr(node->function());
}

void AstDumperImpl::VisitVariableDecl(VariableDecl *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->name());
  if (node->HasTypeDecl()) {
    DumpType(node->type_repr()->type());
  }
  if (node->HasInitialValue()) {
    DumpExpr(node->initial());
  }
}

void AstDumperImpl::VisitStructDecl(StructDecl *node) {
  DumpNodeCommon(node);
  DumpIdentifier(node->name());
  for (auto *field : node->type_repr()->As<StructTypeRepr>()->fields()) {
    DumpDecl(field);
  }
}

void AstDumperImpl::VisitAssignmentStmt(AssignmentStmt *node) {
  DumpNodeCommon(node);
  DumpExpr(node->destination());
  DumpExpr(node->source());
}

void AstDumperImpl::VisitBlockStmt(BlockStmt *node) {
  DumpNodeCommon(node);
  for (auto *stmt : node->statements()) {
    DumpStmt(stmt);
  }
}

void AstDumperImpl::VisitDeclStmt(DeclStmt *node) { AstVisitor<AstDumperImpl>::Visit(node->declaration()); }

void AstDumperImpl::VisitExpressionStmt(ExpressionStmt *node) { AstVisitor<AstDumperImpl>::Visit(node->expression()); }

void AstDumperImpl::VisitForStmt(ForStmt *node) {
  DumpNodeCommon(node);
  if (node->init() != nullptr) {
    DumpStmt(node->init());
  }
  if (node->condition() != nullptr) {
    DumpExpr(node->condition());
  }
  if (node->next() != nullptr) {
    DumpStmt(node->next());
  }
  DumpStmt(node->body());
}

void AstDumperImpl::VisitForInStmt(ForInStmt *node) {
  DumpNodeCommon(node);
  DumpExpr(node->target());
  DumpExpr(node->iter());
  DumpStmt(node->body());
}

void AstDumperImpl::VisitIfStmt(IfStmt *node) {
  DumpNodeCommon(node);
  DumpExpr(node->condition());
  DumpStmt(node->then_stmt());
  if (node->HasElseStmt()) {
    DumpStmt(node->else_stmt());
  }
}

void AstDumperImpl::VisitReturnStmt(ReturnStmt *node) {
  DumpNodeCommon(node);
  if (node->ret() != nullptr) {
    DumpExpr(node->ret());
  }
}

void AstDumperImpl::VisitCallExpr(CallExpr *node) {
  DumpExpressionCommon(node);

  DumpPrimitive("<");
  {
    WithColor color(this, llvm::raw_ostream::Colors::RED);
    switch (node->call_kind()) {
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

  DumpExpr(node->function());
  for (auto *expr : node->arguments()) {
    DumpExpr(expr);
  }
}

void AstDumperImpl::VisitBinaryOpExpr(BinaryOpExpr *node) {
  DumpExpressionCommon(node);
  DumpToken(node->op());
  DumpExpr(node->left());
  DumpExpr(node->right());
}

void AstDumperImpl::VisitComparisonOpExpr(ComparisonOpExpr *node) {
  DumpExpressionCommon(node);
  DumpToken(node->op());
  DumpExpr(node->left());
  DumpExpr(node->right());
}

void AstDumperImpl::VisitFunctionLitExpr(FunctionLitExpr *node) {
  DumpExpressionCommon(node);
  DumpStmt(node->body());
}

void AstDumperImpl::VisitIdentifierExpr(IdentifierExpr *node) {
  DumpExpressionCommon(node);
  DumpIdentifier(node->name());
}

void AstDumperImpl::VisitImplicitCastExpr(ImplicitCastExpr *node) {
  DumpExpressionCommon(node);
  DumpPrimitive("<");
  {
    WithColor color(this, llvm::raw_ostream::Colors::RED);
    switch (node->cast_kind()) {
      case CastKind::IntToSqlInt: {
        DumpPrimitive("IntToSqlInt");
        break;
      }
      case CastKind::IntToSqlDecimal: {
        DumpPrimitive("IntToSqlDecimal");
        break;
      }
      case CastKind::SqlBoolToBool: {
        DumpPrimitive("SqlBoolToBool");
        break;
      }
      case CastKind::IntegralCast: {
        DumpPrimitive("IntegralCast");
        break;
      }
      case CastKind::IntToFloat: {
        DumpPrimitive("IntToFloat");
        break;
      }
      case CastKind::FloatToInt: {
        DumpPrimitive("FloatToInt");
        break;
      }
      case CastKind::BitCast: {
        DumpPrimitive("BitCast");
        break;
      }
      case CastKind::FloatToSqlReal: {
        DumpPrimitive("FloatToSqlReal");
        break;
      }
    }
  }
  DumpPrimitive(">");
  DumpExpr(node->input());
}

void AstDumperImpl::VisitIndexExpr(IndexExpr *node) {
  DumpExpressionCommon(node);
  DumpExpr(node->object());
  DumpExpr(node->index());
}

void AstDumperImpl::VisitLitExpr(LitExpr *node) {
  DumpExpressionCommon(node);
  switch (node->literal_kind()) {
    case LitExpr::LitKind::Nil: {
      DumpPrimitive("nil");
      break;
    }
    case LitExpr::LitKind::Boolean: {
      DumpPrimitive(node->bool_val() ? "'true'" : "'false'");
      break;
    }
    case LitExpr::LitKind::Int: {
      DumpPrimitive(node->int64_val());
      break;
    }
    case LitExpr::LitKind::Float: {
      DumpPrimitive(node->float64_val());
      break;
    }
    case LitExpr::LitKind::String: {
      DumpIdentifier(node->raw_string_val());
      break;
    }
  }
}

void AstDumperImpl::VisitMemberExpr(MemberExpr *node) {
  DumpExpressionCommon(node);
  DumpExpr(node->object());
  DumpExpr(node->member());
}

void AstDumperImpl::VisitUnaryOpExpr(UnaryOpExpr *node) {
  DumpExpressionCommon(node);
  DumpToken(node->op());
  DumpExpr(node->expr());
}

void AstDumperImpl::VisitBadExpr(BadExpr *node) {
  DumpNodeCommon(node);
  DumpPrimitive("BAD EXPRESSION @ ");
  DumpPosition(node->position());
}

void AstDumperImpl::VisitStructTypeRepr(StructTypeRepr *node) {
  DumpNodeCommon(node);
  DumpType(node->type());
}

void AstDumperImpl::VisitPointerTypeRepr(PointerTypeRepr *node) {
  DumpNodeCommon(node);
  DumpExpr(node->base());
}

void AstDumperImpl::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  DumpNodeCommon(node);
  DumpType(node->type());
}

void AstDumperImpl::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  DumpNodeCommon(node);
  DumpExpr(node->length());
  DumpExpr(node->element_type());
}

void AstDumperImpl::VisitMapTypeRepr(MapTypeRepr *node) {
  DumpNodeCommon(node);
  DumpExpr(node->key());
  DumpExpr(node->val());
}

void AstDump::Dump(AstNode *node) {
  AstDumperImpl print(node, fileno(stderr));
  print.Run();
}

}  // namespace terrier::ast
