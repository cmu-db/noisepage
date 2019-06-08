#pragma once

#include <vector>
#include "execution/util/region.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/operator/operator_translator.h"

namespace tpl::compiler {

class CompilationContext;

/// A single pipeline
class Pipeline {
 public:
  /**
   * Constructor
   * @param ctx compilation context to use
   */
  explicit Pipeline(CodeGen * codegen) : codegen_(codegen) {}

  /**
   * Parallism level to use
   */
  enum class Parallelism : uint32_t { Serial = 0, Flexible = 1, Parallel = 2 };

  /**
   * Add an operator translator to the pipeline
   * @param translator translator to add
   * @param parallelism parallelism level
   */
  void Add(OperatorTranslator *translator, Parallelism parallelism = Parallelism::Serial);


  void Initialize(std::vector<ast::Decl> *decls, std::vector<ast::Stmt> *stmts, std::vector<ast::FieldDecl> *fields) {
    // Last operator to have materialized a struct.
    // This is needed because one operator may need to access the members of the struct created by a previous operator.
    // Only a few operators should set this (SeqScan and Pipeline Breakers)
    OperatorState * curr_state = nullptr;
    for (const auto & translator: pipeline_) {
      translator->GetOperatorState()->Initialize(decls, stmts, fields, curr_state);
      if (translator->GetOperatorState()->IsMaterializer()) {
        curr_state = translator->GetOperatorState();
      }
    }
  }

  void Produce(std::vector<ast::Stmt> *stmts, OperatorState ** last_state) {
    ProduceHelper(stmts, 0, last_state);
  }
 private:
  void ProduceHelper(std::vector<ast::Stmt> *stmts, uint64_t pipeline_idx, OperatorState ** curr_state) {
    OperatorTranslator * translator = pipeline_idx[pipeline_idx];
    translator->BeginProduce(stmts, curr_state);
    if (translator->GetOperatorState()->IsMaterializer()) {
      *curr_state = translator->GetOperatorState();
    }
    if (pipeline_idx < pipeline_.size()) ProduceHelper(stmts, pipeline_idx + 1, curr_state);
    translator->FinishProduce(stmts);
  }

  CodeGen * codegen_;
  std::vector<OperatorTranslator *> pipeline_{};
};

}  // namespace tpl::compiler
