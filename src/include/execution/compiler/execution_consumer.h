#pragma once

#include <memory>
#include <string>
#include <utility>
#include "execution/ast/context.h"
#include "execution/compiler/code_context.h"
#include "execution/compiler/consumer_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/row_batch.h"
#include "execution/exec/output.h"

namespace tpl::compiler {
class CompilationContext;
class ConsumerContext;

class ExecutionConsumer {
 public:
  explicit ExecutionConsumer(std::shared_ptr<exec::FinalSchema> schema) : schema_(std::move(schema)) {}

  void Prepare(CompilationContext *ctx) {
    auto &codegen = *ctx->GetCodeGen();
    auto *ast_ctx = ctx->GetCodeGen()->GetCodeContext()->GetAstContext();

    const auto &os_cols = schema_->GetCols();
    util::RegionVector<ast::FieldDecl *> fields(ctx->GetRegion());
    fields.reserve(os_cols.size());
    for (const auto &col : os_cols) {
      auto field_name = ast_ctx->GetIdentifier(col.GetName());
      ast::Expr *type;
      switch (col.GetType()) {
        case terrier::type::TypeId::TINYINT:
        case terrier::type::TypeId::SMALLINT:
        case terrier::type::TypeId::INTEGER:
        case terrier::type::TypeId::BIGINT:
          type = codegen.TyInteger();
          break;
        case terrier::type::TypeId::BOOLEAN:
          type = codegen.TyBool();
          break;
        default:
          // TODO(WAN): throw some kinda error
          type = codegen.TyNil();
      }
      fields.emplace_back(codegen->NewFieldDecl(DUMMY_POS, field_name, type));
    }
    output_ty_ = codegen->NewStructType(DUMMY_POS, std::move(fields));
    auto output_decl = codegen->NewStructDecl(DUMMY_POS, ast_ctx->GetIdentifier(output_struct_name), output_ty_);
    ctx->GetCodeGen()->GetCodeContext()->AddTopDecl(output_decl);
  }

  void InitializeQueryState(CompilationContext *ctx) {}
  void TeardownQueryState(CompilationContext *ctx) {
    auto &codegen = *(ctx->GetCodeGen());

    // @outputFinalize()
    util::RegionVector<ast::Expr *> args(ctx->GetRegion());
    auto call_ex = codegen->NewCallExpr(codegen.BoutputFinalize(), std::move(args));
    ctx->GetCodeGen()->GetCurrentFunction()->Append(codegen->NewExpressionStmt(call_ex));
  }

  void ConsumeResult(ConsumerContext *context, RowBatch *batch) const {
    auto &codegen = *(context->GetCompilationContext()->GetCodeGen());
    auto *ast_ctx = context->GetCompilationContext()->GetCodeGen()->GetCodeContext()->GetAstContext();

    auto output_id = ast_ctx->GetIdentifier(output_name);
    auto output_ex = codegen->NewIdentifierExpr(DUMMY_POS, output_id);
    auto output_struct_ex = codegen->NewIdentifierExpr(DUMMY_POS, ast_ctx->GetIdentifier(output_struct_name));

    // var out = @ptrCast(*output_struct, @outputAlloc())
    {
      util::RegionVector<ast::Expr *> args(context->GetCompilationContext()->GetRegion());
      args.emplace_back(codegen->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::STAR, output_struct_ex));
      util::RegionVector<ast::Expr *> args2(context->GetCompilationContext()->GetRegion());
      args.emplace_back(codegen->NewCallExpr(codegen.BoutputAlloc(), std::move(args2)));
      auto output_init = codegen->NewCallExpr(codegen.BptrCast(), std::move(args));
      auto output_decl = codegen->NewVariableDecl(DUMMY_POS, output_id, nullptr, output_init);
      context->GetCompilationContext()->GetCodeGen()->GetCurrentFunction()->Append(codegen->NewDeclStmt(output_decl));
    }

    // out.x = identifier.x
    {
      for (const auto &col : schema_->GetCols()) {
        auto col_ex = codegen->NewIdentifierExpr(DUMMY_POS, ast_ctx->GetIdentifier(col.GetName()));
        auto dst = codegen->NewMemberExpr(DUMMY_POS, output_ex, col_ex);
        auto src = codegen->NewMemberExpr(DUMMY_POS, batch->GetIdentifierExpr(), col_ex);
        auto asgn_stmt = codegen->NewAssignmentStmt(DUMMY_POS, dst, src);
        context->GetCompilationContext()->GetCodeGen()->GetCurrentFunction()->Append(asgn_stmt);
      }
    }

    // @outputAdvance()
    {
      util::RegionVector<ast::Expr *> args(context->GetCompilationContext()->GetRegion());
      auto call_ex = codegen->NewCallExpr(codegen.BoutputAdvance(), std::move(args));
      context->GetCompilationContext()->GetCodeGen()->GetCurrentFunction()->Append(codegen->NewExpressionStmt(call_ex));
    }
  }

 private:
  std::string output_struct_name = "output_struct";
  std::string output_name = "output";
  std::shared_ptr<exec::FinalSchema> schema_;
  ast::StructTypeRepr *output_ty_;
};

}  // namespace tpl::compiler
