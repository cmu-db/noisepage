#include <execution/ast/ast_dump.h>
#include "execution/compiler/compilation_context.h"

#include "execution/compiler/code_context.h"

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/compiler/execution_consumer.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/query.h"
#include "execution/compiler/query_state.h"
#include "execution/util/region_containers.h"
#include "execution/parsing/token.h"
#include "execution/sema/sema.h"

namespace tpl::compiler {

CompilationContext::CompilationContext(Query *query, ExecutionConsumer *consumer)
    : query_(query), consumer_(consumer), codegen_(query_->GetCodeContext()) {}

void CompilationContext::GeneratePlan(Query *query) {
  Pipeline main_pipeline(this);
  consumer_->Prepare(this);
  // This doesn't recursively call on the rest of the nodes?
  Prepare(query->GetPlan(), &main_pipeline);
  query->GetQueryState()->FinalizeType(&codegen_);
  auto qs_type = query->GetQueryState()->GetType();
  auto qs_type_ptr = codegen_->NewPointerType(DUMMY_POS, qs_type);
  ast::Identifier qs_id(query->GetQueryStateName().c_str());

  // List of top level declarations
  util::RegionVector<ast::Decl *> decls(query->GetRegion());

  {
    // 1. Declare the query state struct.
    ast::Identifier qs_struct_id(query->GetQueryStateStructName().c_str());
    decls.emplace_back(codegen_->NewStructDecl(DUMMY_POS, qs_struct_id, qs_type));
  }

  {
    // 2. Declare init function
    util::RegionVector<ast::FieldDecl *> params(query->GetRegion());
    params.emplace_back(codegen_->NewFieldDecl(DUMMY_POS, qs_id, qs_type_ptr));
    FunctionBuilder
        init_fn(codegen_, ast::Identifier(query->GetQueryInitName().c_str()), std::move(params), codegen_.Ty_Nil());
    consumer_->InitializeQueryState(this);
    for (const auto &it : op_translators_) {
      it.second->InitializeQueryState();
    }
    decls.emplace_back(init_fn.Finish());
  }

  {
    // 3. Declare produce function
    util::RegionVector<ast::FieldDecl *> params(query->GetRegion());
    params.emplace_back(codegen_->NewFieldDecl(DUMMY_POS, qs_id, qs_type_ptr));
    FunctionBuilder
        produce_fn(codegen_, ast::Identifier(query->GetQueryProduceName().c_str()), std::move(params), codegen_.Ty_Nil());
    GetTranslator(query->GetPlan())->Produce();
    decls.emplace_back(produce_fn.Finish());
  }

  {
    // 4. Declare teardown function
    util::RegionVector<ast::FieldDecl *> params(query->GetRegion());
    params.emplace_back(codegen_->NewFieldDecl(DUMMY_POS, qs_id, qs_type_ptr));
    FunctionBuilder
        teardown_fn(codegen_, ast::Identifier(query->GetQueryTeardownName().c_str()), std::move(params), codegen_.Ty_Nil());
    consumer_->TeardownQueryState(this);
    for (const auto &it : op_translators_) {
      it.second->TeardownQueryState();
    }
    decls.emplace_back(teardown_fn.Finish());
  }

  {
    // 5. Define main function
    util::RegionVector<ast::FieldDecl *> main_params(query->GetRegion());
    FunctionBuilder main_fn(codegen_, ast::Identifier("main"), std::move(main_params), codegen_.Ty_Int32());
    // 5.1 Declare the qs variable
    auto qs_decl = codegen_->NewDeclStmt(codegen_->NewVariableDecl(DUMMY_POS, qs_id, qs_type, nullptr));
    main_fn.Append(qs_decl);
    // 5.2 Call init_fn(&qs)
    util::RegionVector<ast::Expr *> init_params(query->GetRegion());
    auto init_name = codegen_->NewIdentifierExpr(DUMMY_POS, ast::Identifier(query->GetQueryInitName().c_str()));
    auto init_qs_expr = codegen_->NewIdentifierExpr(DUMMY_POS, qs_id);
    init_params.emplace_back(codegen_->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::AMPERSAND, init_qs_expr));
    main_fn.Append(codegen_->NewExpressionStmt(codegen_->NewCallExpr(init_name, std::move(init_params))));

    // 5.3 Call produce_fn(&qs).
    // TODO(Amadou): See if the paramaters can be reused without creating memory issues.
    util::RegionVector<ast::Expr *> prod_params(query->GetRegion());
    auto prod_name = codegen_->NewIdentifierExpr(DUMMY_POS, ast::Identifier(query->GetQueryProduceName().c_str()));
    auto prod_qs_expr = codegen_->NewIdentifierExpr(DUMMY_POS, qs_id);
    prod_params.emplace_back(codegen_->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::AMPERSAND, prod_qs_expr));
    main_fn.Append(codegen_->NewExpressionStmt(codegen_->NewCallExpr(prod_name, std::move(prod_params))));

    // 5.4 Call teardown_fn(&qs)
    // TODO(Amadou): See if the paramaters can be reused without creating memory issues.
    util::RegionVector<ast::Expr *> teardown_params(query->GetRegion());
    auto teardown_name = codegen_->NewIdentifierExpr(DUMMY_POS, ast::Identifier(query->GetQueryTeardownName().c_str()));
    auto teardown_qs_expr = codegen_->NewIdentifierExpr(DUMMY_POS, qs_id);
    teardown_params.emplace_back(codegen_->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::AMPERSAND, teardown_qs_expr));
    main_fn.Append(codegen_->NewExpressionStmt(codegen_->NewCallExpr(teardown_name, std::move(teardown_params))));

    // 5.5 return 0
    auto return_stmt = codegen_->NewReturnStmt(DUMMY_POS, codegen_->NewIntLiteral(DUMMY_POS, 0));
    main_fn.Append(return_stmt);
    decls.emplace_back(main_fn.Finish());
  }

  tpl::sema::Sema type_check(codegen_.GetCodeContext()->GetAstContext());

  const auto compiled_fn = codegen_->NewFile(DUMMY_POS, std::move(decls));
  query->SetCompiledFunction(compiled_fn);
  type_check.Run(query_->GetCompiledFunction());
}

u32 CompilationContext::RegisterPipeline(Pipeline *pipeline) {
  auto pos = pipelines_.size();
  pipelines_.emplace_back(pipeline);
  return static_cast<u32>(pos);
}

ExecutionConsumer *CompilationContext::GetExecutionConsumer() { return consumer_; }
CodeGen *CompilationContext::GetCodeGen() { return &codegen_; }

util::Region *CompilationContext::GetRegion() {
  return query_->GetRegion();
}

void CompilationContext::Prepare(const terrier::planner::AbstractPlanNode &op, tpl::compiler::Pipeline *pipeline) {
  auto translator = translator_factory_.CreateTranslator(op, pipeline);
  op_translators_.emplace(std::make_pair(&op, translator));
}

// Prepare the translator for the given expression
void CompilationContext::Prepare(const terrier::parser::AbstractExpression &exp) {
  auto translator = translator_factory_.CreateTranslator(exp, *this);
  ex_translators_.insert(std::make_pair(&exp, translator));
}

// Get the registered translator for the given operator
OperatorTranslator *CompilationContext::GetTranslator(const terrier::planner::AbstractPlanNode &op) const {
  auto iter = op_translators_.find(&op);
  return iter == op_translators_.end() ? nullptr : iter->second;
}

ExpressionTranslator *CompilationContext::GetTranslator(const terrier::parser::AbstractExpression &ex) const {
  auto iter = ex_translators_.find(&ex);
  return iter == ex_translators_.end() ? nullptr : iter->second;
}

}