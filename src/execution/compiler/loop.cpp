#include "execution/compiler/loop.h"

#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"

namespace noisepage::execution::compiler {

Loop::Loop(FunctionBuilder *function, ast::Stmt *init, ast::Expr *condition, ast::Stmt *next)
    : function_(function),
      position_(function_->GetCodeGen()->GetPosition()),
      prev_statements_(nullptr),
      init_(init),
      condition_(condition),
      next_(next),
      loop_body_(function_->GetCodeGen()->MakeEmptyBlock()),
      completed_(false) {
  // Stash the previous statement list so we can restore it upon completion.
  prev_statements_ = function_->statements_;
  // Swap in our loop-body statement list as the active statement list.
  function_->statements_ = loop_body_;
}

// Static cast to disambiguate constructor.
Loop::Loop(FunctionBuilder *function, ast::Expr *condition)
    : Loop(function, static_cast<ast::Stmt *>(nullptr), condition, nullptr) {}

// Static cast to disambiguate constructor.
Loop::Loop(FunctionBuilder *function) : Loop(function, static_cast<ast::Stmt *>(nullptr), nullptr, nullptr) {}

Loop::~Loop() { EndLoop(); }

void Loop::EndLoop() {
  if (completed_) {
    return;
  }

  // Restore the previous statement list, now that we're done.
  function_->statements_ = prev_statements_;

  // Create and append the if statement.
  auto codegen = function_->GetCodeGen();
  auto loop = codegen->GetFactory()->NewForStmt(position_, init_, condition_, next_, loop_body_);
  function_->Append(loop);

  // Done.
  completed_ = true;
}

}  // namespace noisepage::execution::compiler
