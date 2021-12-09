#include "execution/exec/execution_context_builder.h"

#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "common/macros.h"
#include "execution/exec/execution_context.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::execution::exec {

std::unique_ptr<ExecutionContext> ExecutionContextBuilder::Build() {
  if (db_oid_ == catalog::INVALID_DATABASE_OID) {
    throw EXECUTION_EXCEPTION("Must specify database OID.", common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (!exec_settings_.has_value()) {
    throw EXECUTION_EXCEPTION("Must specify exection settings.", common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (!txn_.has_value()) {
    throw EXECUTION_EXCEPTION("Must specify a transaction context.", common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (!output_schema_.has_value()) {
    throw EXECUTION_EXCEPTION("Must specify output schema.", common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (!output_callback_.has_value()) {
    throw EXECUTION_EXCEPTION("Must specify output callback.", common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (!catalog_accessor_.has_value()) {
    throw EXECUTION_EXCEPTION("Must specify catalog accessor.", common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (!metrics_manager_.has_value()) {
    throw EXECUTION_EXCEPTION("Must specify metrics manager.", common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (!replication_manager_.has_value()) {
    throw EXECUTION_EXCEPTION("Must specify replication manager.", common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (!recovery_manager_.has_value()) {
    throw EXECUTION_EXCEPTION("Must specify recovery manager.", common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }

  // Query parameters (parameters_) is not validated because default is empty collection
  // ExecutionSettings exec_settings = exec_settings_.value();
  return std::unique_ptr<ExecutionContext>{
      new ExecutionContext{db_oid_, std::move(parameters_), exec_settings_.value(), txn_.value(),
                           output_schema_.value(), std::move(output_callback_.value()), catalog_accessor_.value(),
                           metrics_manager_.value(), replication_manager_.value(), recovery_manager_.value()}};
}

ExecutionContextBuilder &ExecutionContextBuilder::WithQueryParametersFrom(
    const std::vector<parser::ConstantValueExpression> &parameter_exprs) {
  NOISEPAGE_ASSERT(parameters_.empty(), "Attempt to initialize query parameters more than once.");
  parameters_.reserve(parameter_exprs.size());
  std::transform(parameter_exprs.cbegin(), parameter_exprs.cend(), std::back_inserter(parameters_),
                 [](const parser::ConstantValueExpression &expr) -> common::ManagedPointer<const sql::Val> {
                   return common::ManagedPointer{expr.SqlValue()};
                 });
  return *this;
}
}  // namespace noisepage::execution::exec
