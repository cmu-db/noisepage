#include "execution/compiler/compiler.h"

#include <memory>
#include <utility>

#include "brain/operating_unit_recorder.h"
#include "execution/ast/ast_dump.h"
#include "execution/compiler/translator_factory.h"
#include "execution/sema/sema.h"
#include "loggers/execution_logger.h"

namespace terrier::execution::compiler {

Compiler::Compiler(query_id_t query_id, CodeGen *codegen, const planner::AbstractPlanNode *plan)
    : query_identifier_(query_id), codegen_(codegen), plan_(plan) {
  // Make the pipelines
  auto main_pipeline = std::make_unique<Pipeline>(codegen_);
  MakePipelines(*plan, main_pipeline.get());
  // If the query has an ouput, make an output translator
  if (plan_->GetOutputSchema() != nullptr) {
    auto output_translator = std::make_unique<OutputTranslator>(codegen_);
    main_pipeline->Add(std::move(output_translator));
  }
  // Finally add the main pipeline
  pipelines_.push_back(std::move(main_pipeline));
  EXECUTION_LOG_DEBUG("Made {} pipelines", pipelines_.size());
}

/*
 * The generated tpl will have the following top level declarations:
 * 1. Global state struct: struct State {...}
 * 2. Helper structs & functions specific to each operation (e.g. join build struct or comparison function for sorting).
 * 3. The setup and teardown function to initialize and free global state objects.
 * 4. The functions that execute each pipeline.
 * 5. The main function.
 */
ast::File *Compiler::Compile() {
  // Step 1: Generate state, structs, helper functions
  util::RegionVector<ast::Decl *> decls(codegen_->Region());
  util::RegionVector<ast::FieldDecl *> state_fields(codegen_->Region());
  util::RegionVector<ast::Stmt *> setup_stmts(codegen_->Region());
  util::RegionVector<ast::Stmt *> teardow_stmts(codegen_->Region());
  // 1.1 Let every pipeline initialize itself
  for (auto &pipeline : pipelines_) {
    pipeline->Initialize(&decls, &state_fields, &setup_stmts, &teardow_stmts);
  }

  // 1.2 Make the top level declarations
  util::RegionVector<ast::Decl *> top_level(codegen_->Region());
  GenStateStruct(&top_level, std::move(state_fields));
  top_level.insert(top_level.end(), decls.begin(), decls.end());
  GenFunction(&top_level, codegen_->GetSetupFn(), std::move(setup_stmts));
  GenFunction(&top_level, codegen_->GetTeardownFn(), std::move(teardow_stmts));

  // Step 2: For each pipeline: Generate a pipeline function that performs the produce, consume logic
  // TODO(Amadou): This can actually be combined with the previous step to avoid the additional pass
  // over the list of pipelines. However, I find this easier to debug for now.
  pipeline_id_t pipeline_cnt = pipeline_id_t{0};
  for (auto &pipeline : pipelines_) {
    auto pipeline_idx = pipeline_cnt++;

    // Record features
    brain::OperatingUnitRecorder recorder(common::ManagedPointer(codegen_->Accessor()),
                                          common::ManagedPointer(codegen_->Context()));
    auto &translators = pipeline->GetTranslators();
    auto features = recorder.RecordTranslators(translators);
    codegen_->GetPipelineOperatingUnits()->RecordOperatingUnit(pipeline_idx, std::move(features));

    // Produce the actual pipeline
    top_level.emplace_back(pipeline->Produce(query_identifier_, pipeline_idx));
  }

  // Step 3: Make the main function
  top_level.emplace_back(GenMainFunction());

  // Compile
  ast::File *compiled_file = codegen_->Compile(std::move(top_level));
  EXECUTION_LOG_DEBUG("Generated File");
  sema::Sema type_checker{codegen_->Context()};
  type_checker.Run(compiled_file);
  return compiled_file;
}

void Compiler::MakePipelines(const terrier::planner::AbstractPlanNode &op, Pipeline *curr_pipeline) {
  switch (op.GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::AGGREGATE:
    case terrier::planner::PlanNodeType::ORDERBY: {
      // These nodes split in two parts: A "build" side (called bottom) and an "iterate" side (called top).
      auto bottom_translator = TranslatorFactory::CreateBottomTranslator(&op, codegen_);
      auto top_translator = TranslatorFactory::CreateTopTranslator(&op, bottom_translator.get(), codegen_);
      // The "build" side is a pipeline breaker. It belongs to a new pipeline.
      auto next_pipeline = std::make_unique<Pipeline>(codegen_);
      MakePipelines(*op.GetChild(0), next_pipeline.get());
      next_pipeline->Add(std::move(bottom_translator));
      pipelines_.emplace_back(std::move(next_pipeline));
      // The "iterate" side terminates the current pipeline.
      curr_pipeline->Add(std::move(top_translator));
      return;
    }
    case terrier::planner::PlanNodeType::HASHJOIN: {
      // The hash join splits also splits in a "build" side (called left) and an "iterate" side (called right).
      auto left_translator = TranslatorFactory::CreateLeftTranslator(&op, codegen_);
      auto right_translator = TranslatorFactory::CreateRightTranslator(&op, left_translator.get(), codegen_);

      // The "build" side is a pipeline breaker. It belongs to a new pipeline.
      auto next_pipeline = std::make_unique<Pipeline>(codegen_);
      MakePipelines(*op.GetChild(0), next_pipeline.get());
      next_pipeline->Add(std::move(left_translator));
      pipelines_.emplace_back(std::move(next_pipeline));
      // The "iterate" side belongs the current pipeline.
      MakePipelines(*op.GetChild(1), curr_pipeline);
      curr_pipeline->Add(std::move(right_translator));
      return;
    }
    case terrier::planner::PlanNodeType::NESTLOOP: {
      // The two sides of the nested loop join belong to the same pipeline. They are just concatenated together.
      // These two translator glue the two sides together and ensure that expression evaluation is correctly done.
      auto left_translator = TranslatorFactory::CreateLeftTranslator(&op, codegen_);
      auto right_translator = TranslatorFactory::CreateRightTranslator(&op, left_translator.get(), codegen_);
      // Outer loop
      MakePipelines(*op.GetChild(1), curr_pipeline);
      curr_pipeline->Add(std::move(left_translator));
      // Inner loop
      MakePipelines(*op.GetChild(0), curr_pipeline);
      curr_pipeline->Add(std::move(right_translator));
      return;
    }
    default: {
      // Every other operation just adds itself to the current pipeline.
      auto translator = TranslatorFactory::CreateRegularTranslator(&op, codegen_);
      if (op.GetChildrenSize() != 0) {
        TERRIER_ASSERT(op.GetChildrenSize() == 1, "We only look at the first child.");
        MakePipelines(*op.GetChild(0), curr_pipeline);
      }
      curr_pipeline->Add(std::move(translator));
      return;
    }
  }
}

void Compiler::GenStateStruct(execution::util::RegionVector<execution::ast::Decl *> *top_level,
                              execution::util::RegionVector<execution::ast::FieldDecl *> &&fields) {
  // TODO(Amadou): Make a dummy fields in case no operator has a state.
  //  This can be removed once the empty struct bug is fixed.
  ast::Identifier dummy_name = codegen_->Context()->GetIdentifier("DUMMY");
  ast::Expr *dummy_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Int32);
  fields.emplace_back(codegen_->MakeField(dummy_name, dummy_type));
  top_level->emplace_back(codegen_->MakeStruct(codegen_->GetStateType(), std::move(fields)));
}

void Compiler::GenFunction(execution::util::RegionVector<execution::ast::Decl *> *top_level, ast::Identifier fn_name,
                           execution::util::RegionVector<execution::ast::Stmt *> &&stmts) {
  // Function parameter
  util::RegionVector<ast::FieldDecl *> params = codegen_->ExecParams();

  // Function return type (nil)
  ast::Expr *ret_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Nil);

  // Make the function
  FunctionBuilder builder{codegen_, fn_name, std::move(params), ret_type};
  for (const auto &stmt : stmts) {
    builder.Append(stmt);
  }
  top_level->emplace_back(builder.Finish());
}

ast::Decl *Compiler::GenMainFunction() {
  // Function name
  ast::Identifier fn_name = codegen_->GetMainFn();

  // Function parameter
  util::RegionVector<ast::FieldDecl *> params = codegen_->MainParams();

  // Function return type (int32)
  ast::Expr *ret_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Int32);

  // Make the function
  FunctionBuilder builder{codegen_, fn_name, std::move(params), ret_type};

  // Step 0: Define the state variable.
  ast::Identifier state = codegen_->GetStateVar();
  ast::Expr *state_type = codegen_->MakeExpr(codegen_->GetStateType());
  builder.Append(codegen_->DeclareVariable(state, state_type, nullptr));

  // Step 1: Call setupFn(state, execCtx)
  builder.Append(codegen_->ExecCall(codegen_->GetSetupFn()));
  // Step 2: For each pipeline, call its function
  for (const auto &pipeline : pipelines_) {
    builder.Append(codegen_->ExecCall(pipeline->GetPipelineName()));
  }
  // Step 3: Call the teardown function
  builder.Append(codegen_->ExecCall(codegen_->GetTeardownFn()));
  // Step 4: return a value of 0
  builder.Append(codegen_->ReturnStmt(codegen_->IntLiteral(0)));
  return builder.Finish();
}
}  // namespace terrier::execution::compiler
