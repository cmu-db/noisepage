#include "execution/compiler/operator/sort_translator.h"
#include <memory>
#include <utility>
#include <vector>
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"
#include "planner/plannodes/order_by_plan_node.h"

namespace terrier::execution::compiler {
SortBottomTranslator::SortBottomTranslator(const terrier::planner::OrderByPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::SORT_BUILD),
      op_(op),
      sorter_(codegen_->NewIdentifier("sorter")),
      sorter_row_(codegen_->NewIdentifier("sorter_row")),
      sorter_struct_(codegen_->NewIdentifier("SorterRow")),
      comp_fn_(codegen_->NewIdentifier("sorterCompFn")),
      comp_lhs_(codegen_->NewIdentifier("lhs")),
      comp_rhs_(codegen_->NewIdentifier("rhs")) {}

void SortBottomTranslator::Produce(FunctionBuilder *builder) {
  child_translator_->Produce(builder);
  // At the end of the pipeline, call sorterSort.
  GenSorterSort(builder);
}

void SortBottomTranslator::Abort(FunctionBuilder *builder) { child_translator_->Abort(builder); }

void SortBottomTranslator::Consume(FunctionBuilder *builder) {
  // First call sorterInsert
  GenSorterInsert(builder);
  // Then fill in the values
  FillSorterRow(builder);
  // If this has a limit, call finish topK.
  if (op_->HasLimit()) {
    GenFinishTopK(builder);
  }
}

void SortBottomTranslator::GenSorterInsert(FunctionBuilder *builder) {
  // var sorter_row = @ptrCast(*SorterStruct, @sorterInsert(&state.sorter))
  ast::Expr *insert_call;
  if (op_->HasLimit()) {
    ast::Expr *sorter = codegen_->GetStateMemberPtr(sorter_);
    ast::Expr *k = codegen_->IntLiteral(op_->GetLimit() + op_->GetOffset());
    insert_call = codegen_->BuiltinCall(ast::Builtin::SorterInsertTopK, {sorter, k});
  } else {
    insert_call = codegen_->OneArgStateCall(ast::Builtin::SorterInsert, sorter_);
  }

  // Gen create @ptrcast(*SorterStruct, ...)
  ast::Expr *cast_call = codegen_->PtrCast(sorter_struct_, insert_call);

  // Declare var sorter_row
  builder->Append(codegen_->DeclareVariable(sorter_row_, nullptr, cast_call));
}

void SortBottomTranslator::GenFinishTopK(FunctionBuilder *builder) {
  ast::Expr *sorter = codegen_->GetStateMemberPtr(sorter_);
  ast::Expr *k = codegen_->IntLiteral(op_->GetLimit() + op_->GetOffset());
  auto finish_call = codegen_->BuiltinCall(ast::Builtin::SorterInsertTopKFinish, {sorter, k});
  builder->Append(codegen_->MakeStmt(finish_call));
}

void SortBottomTranslator::FillSorterRow(FunctionBuilder *builder) {
  // For each child output, set the sorter attribute
  for (uint32_t attr_idx = 0; attr_idx < op_->GetChild(0)->GetOutputSchema()->GetColumns().size(); attr_idx++) {
    ast::Expr *lhs = GetAttribute(sorter_row_, attr_idx);
    ast::Expr *rhs = child_translator_->GetOutput(attr_idx);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

void SortBottomTranslator::GenSorterSort(FunctionBuilder *builder) {
  ast::Expr *sort_call = codegen_->OneArgStateCall(ast::Builtin::SorterSort, sorter_);
  builder->Append(codegen_->MakeStmt(sort_call));
}

void SortBottomTranslator::InitializeStateFields(
    execution::util::RegionVector<execution::ast::FieldDecl *> *state_fields) {
  // sorter: Sorter
  ast::Expr *sorter_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Sorter);
  state_fields->emplace_back(codegen_->MakeField(sorter_, sorter_type));
}

void SortBottomTranslator::InitializeStructs(execution::util::RegionVector<execution::ast::Decl *> *decls) {
  util::RegionVector<execution::ast::FieldDecl *> fields{codegen_->Region()};
  GetChildOutputFields(&fields, SORTER_ATTR_PREFIX);

  auto *decl = codegen_->MakeStruct(sorter_struct_, std::move(fields));
  TERRIER_ASSERT(ast::StructDecl::classof(decl), "Expected StructDecl");

  struct_decl_ = reinterpret_cast<ast::StructDecl *>(decl);
  decls->emplace_back(struct_decl_);
}

void SortBottomTranslator::InitializeHelperFunctions(execution::util::RegionVector<execution::ast::Decl *> *decls) {
  // Make a function (lhs *SorterStruct, rhs *SorterStruct) -> int32
  ast::FieldDecl *lhs = codegen_->MakeField(comp_lhs_, codegen_->PointerType(sorter_struct_));
  ast::FieldDecl *rhs = codegen_->MakeField(comp_rhs_, codegen_->PointerType(sorter_struct_));
  ast::Expr *ret_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Int32);
  util::RegionVector<ast::FieldDecl *> params{{lhs, rhs}, codegen_->Region()};
  FunctionBuilder builder{codegen_, comp_fn_, std::move(params), ret_type};
  GenComparisons(&builder);
  decls->push_back(builder.Finish());
}

void SortBottomTranslator::InitializeSetup(execution::util::RegionVector<execution::ast::Stmt *> *setup_stmts) {
  // @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterStruct))
  ast::Expr *sizeof_call = codegen_->SizeOf(sorter_struct_);
  std::vector<ast::Expr *> init_args{codegen_->GetStateMemberPtr(sorter_), codegen_->ExecCtxGetMem(),
                                     codegen_->MakeExpr(comp_fn_), sizeof_call};
  ast::Expr *init_call = codegen_->BuiltinCall(ast::Builtin::SorterInit, std::move(init_args));

  // Add it the setup statements
  setup_stmts->emplace_back(codegen_->MakeStmt(init_call));
}

void SortBottomTranslator::InitializeTeardown(execution::util::RegionVector<execution::ast::Stmt *> *teardown_stmts) {
  // @sorterFree(&state.sorter)
  ast::Expr *free_call = codegen_->OneArgStateCall(ast::Builtin::SorterFree, sorter_);
  teardown_stmts->emplace_back(codegen_->MakeStmt(free_call));
}

ast::Expr *SortBottomTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  // Pass through to child node
  if (current_row_ == CurrentRow::Child) {
    return child_translator_->GetOutput(attr_idx);
  }
  // Use the lhs or lhs
  return GetAttribute(current_row_ == CurrentRow::Lhs ? comp_lhs_ : comp_rhs_, attr_idx);
}

ast::Expr *SortBottomTranslator::GetOutput(uint32_t attr_idx) { return GetAttribute(sorter_row_, attr_idx); }

ast::Expr *SortBottomTranslator::GetAttribute(execution::ast::Identifier object, uint32_t attr_idx) {
  ast::Identifier member = codegen_->Context()->GetIdentifier(SORTER_ATTR_PREFIX + std::to_string(attr_idx));
  return codegen_->MemberExpr(object, member);
}

void SortBottomTranslator::GenComparisons(FunctionBuilder *builder) {
  // For each order by expr generate this (or its inverse depending on the ordering type):
  // if (lhs.col_i < rhs.col_i) {return -1}
  // if (lhs.col_i > rhs.col_i) {return 1}
  // ...
  // return 0
  // This will be 1 or -1 depending on the order type.
  int32_t ret_value;
  uint32_t attr_idx = 0;
  for (const auto &order : op_->GetSortKeys()) {
    if (order.second == optimizer::OrderByOrderingType::ASC) {
      ret_value = -1;
    } else {
      ret_value = 1;
    }
    std::unique_ptr<ExpressionTranslator> key_translator =
        TranslatorFactory::CreateExpressionTranslator(order.first.Get(), codegen_);
    for (const auto tok : {parsing::Token::Type::LESS, parsing::Token::Type::GREATER}) {
      // Get lhs.col_i
      current_row_ = CurrentRow::Lhs;
      ast::Expr *lhs_cond = key_translator->DeriveExpr(this);
      // Get rhs.col_i
      current_row_ = CurrentRow::Rhs;
      ast::Expr *rhs_cond = key_translator->DeriveExpr(this);
      // Generate if (lhs.col_i TOK rhs.col_i) {return ret_value;}
      ast::Expr *if_cond = codegen_->Compare(tok, lhs_cond, rhs_cond);
      builder->StartIfStmt(if_cond);
      builder->Append(codegen_->ReturnStmt(codegen_->IntLiteral(ret_value)));
      builder->FinishBlockStmt();
      // Next if statement should return the opposite value
      ret_value = -ret_value;
    }
    attr_idx++;
  }
  current_row_ = CurrentRow::Child;
  // return 0 at the end
  builder->Append(codegen_->ReturnStmt(codegen_->IntLiteral(0)));
}

SortTopTranslator::SortTopTranslator(const terrier::planner::OrderByPlanNode *op, CodeGen *codegen,
                                     OperatorTranslator *bottom)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::SORT_ITERATE),
      op_(op),
      bottom_(dynamic_cast<SortBottomTranslator *>(bottom)),
      sort_iter_(codegen_->NewIdentifier("sort_iter")),
      num_tuples_(codegen->NewIdentifier("limit")) {}

void SortTopTranslator::Produce(FunctionBuilder *builder) {
  // Declare the iterator
  DeclareIterator(builder);
  // In case of nested loop joins, let the child produce
  if (child_translator_ != nullptr) {
    child_translator_->Produce(builder);
  } else {
    // Otherwise directly consume the bottom's output
    Consume(builder);
  }
}

void SortTopTranslator::Abort(FunctionBuilder *builder) {
  CloseIterator(builder);
  if (child_translator_ != nullptr) child_translator_->Abort(builder);
}

void SortTopTranslator::Consume(FunctionBuilder *builder) {
  // Generate the for loop
  GenForLoop(builder);
  // Declare the resulting iterator.
  DeclareResult(builder);
  // Let parent consume
  parent_translator_->Consume(builder);
  // Close the iterator after the loop ends.
  builder->FinishBlockStmt();
  if (op_->HasLimit() && op_->GetOffset() != 0) {
    // Close the if statement for the offset
    builder->FinishBlockStmt();
  }
  CloseIterator(builder);
}

void SortTopTranslator::DeclareIterator(FunctionBuilder *builder) {
  // var sort_iter : SorterIterator
  ast::Expr *iter_type = codegen_->BuiltinType(ast::BuiltinType::SorterIterator);
  builder->Append(codegen_->DeclareVariable(sort_iter_, iter_type, nullptr));
  // @sorterIterInit(&sort_iter, &state.sorter)
  std::vector<ast::Expr *> init_args{codegen_->PointerTo(sort_iter_), codegen_->GetStateMemberPtr(bottom_->sorter_)};
  ast::Expr *init_call = codegen_->BuiltinCall(ast::Builtin::SorterIterInit, std::move(init_args));
  builder->Append(codegen_->MakeStmt(init_call));
}

void SortTopTranslator::GenForLoop(FunctionBuilder *builder) {
  if (op_->GetOffset() != 0) {
    // Declare Limit variable.
    builder->Append(codegen_->DeclareVariable(num_tuples_, nullptr, codegen_->IntLiteral(0)));
  }
  // for (; @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter))
  // Loop condition
  ast::Expr *has_next_call = codegen_->OneArgCall(ast::Builtin::SorterIterHasNext, sort_iter_, true);
  ast::Expr *next_call = codegen_->OneArgCall(ast::Builtin::SorterIterNext, sort_iter_, true);
  ast::Stmt *loop_update = codegen_->MakeStmt(next_call);
  builder->StartForStmt(nullptr, has_next_call, loop_update);
  if (op_->HasLimit() && op_->GetOffset() != 0) {
    auto lhs = codegen_->MakeExpr(num_tuples_);
    auto rhs = codegen_->BinaryOp(parsing::Token::Type::PLUS, codegen_->MakeExpr(num_tuples_), codegen_->IntLiteral(1));
    builder->Append(codegen_->Assign(lhs, rhs));
    ast::Expr *offset_comp = codegen_->Compare(parsing::Token::Type::GREATER, codegen_->MakeExpr(num_tuples_),
                                               codegen_->IntLiteral(op_->GetOffset()));
    builder->StartIfStmt(offset_comp);
  }
}

void SortTopTranslator::CloseIterator(FunctionBuilder *builder) {
  // Call @sorterIterClose(&sort_iter)
  ast::Expr *close_call = codegen_->OneArgCall(ast::Builtin::SorterIterClose, sort_iter_, true);
  builder->Append(codegen_->MakeStmt(close_call));
}

void SortTopTranslator::DeclareResult(FunctionBuilder *builder) {
  // var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
  // @sorterIterGetRow(&sort_iter)
  ast::Expr *get_row_call = codegen_->OneArgCall(ast::Builtin::SorterIterGetRow, sort_iter_, true);

  // @ptrcast(*SorterRow, ...)
  ast::Expr *cast_call = codegen_->PtrCast(bottom_->sorter_struct_, get_row_call);

  // Declare var sorter_row
  builder->Append(codegen_->DeclareVariable(bottom_->sorter_row_, nullptr, cast_call));
}

ast::Expr *SortTopTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  return bottom_->GetOutput(attr_idx);
}

ast::Expr *SortTopTranslator::GetOutput(uint32_t attr_idx) {
  auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
  std::unique_ptr<ExpressionTranslator> translator =
      TranslatorFactory::CreateExpressionTranslator(output_expr.Get(), codegen_);
  return translator->DeriveExpr(this);
}
}  // namespace terrier::execution::compiler
