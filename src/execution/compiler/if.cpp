#include "execution/compiler/if.h"

#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"

namespace noisepage::execution::compiler {

If::If(FunctionBuilder *function, ast::Expr *condition)
    : function_(function),
      position_(function_->GetCodeGen()->GetPosition()),
      prev_func_stmt_list_(nullptr),
      condition_(condition),
      then_stmts_(function_->GetCodeGen()->MakeEmptyBlock()),
      else_stmts_(nullptr),
      completed_(false) {
  // Stash the previous statement list so we can restore it upon completion.
  prev_func_stmt_list_ = function_->statements_;
  // Swap in our 'then' statement list as the active statement list.
  function_->statements_ = then_stmts_;
}

If::~If() { EndIf(); }

void If::Else() {
  // Create a new statement list for the 'else' block and activate it in the
  // current function.
  else_stmts_ = function_->GetCodeGen()->MakeEmptyBlock();
  function_->statements_ = else_stmts_;
}

void If::EndIf() {
  if (completed_) {
    return;
  }

  // Restore the previous statement list, now that we're done.
  function_->statements_ = prev_func_stmt_list_;

  // Create and append the if statement.
  auto codegen = function_->GetCodeGen();
  auto if_stmt = codegen->GetFactory()->NewIfStmt(position_, condition_, then_stmts_, else_stmts_);
  function_->Append(if_stmt);

  // Done.
  completed_ = true;
}

}  // namespace noisepage::execution::compiler
