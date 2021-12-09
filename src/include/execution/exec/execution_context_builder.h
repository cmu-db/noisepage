#pragma once

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec/output.h"

namespace noisepage::parser {
class ConstantValueExpression;
}  // namespace noisepage::parser

namespace noisepage::planner {
class OutputSchema;
}  // namespace noisepage::planner

namespace noisepage::catalog {
class CatalogAccessor;
}  // namespace noisepage::catalog

namespace noisepage::metrics {
class MetricsManager;
}  // namespace noisepage::metrics

namespace noisepage::replication {
class ReplicationManager;
}  // namespace noisepage::replication

namespace noisepage::storage {
class RecoveryManager;
}  // namespace noisepage::storage

namespace noisepage::execution::sql {
struct Val;
}  // namespace noisepage::execution::sql

namespace noisepage::transaction {
class TransactionContext;
}  // namespace noisepage::transaction

namespace noisepage::execution::exec {

class ExecutionContext;
class ExecutionSettings;

/**
 * The ExecutionContextBuilder class implements a builder for ExecutionContext.
 */
class ExecutionContextBuilder {
 public:
  /**
   * Construct a new ExecutionContextBuilder.
   */
  ExecutionContextBuilder() = default;

  /** @return The completed ExecutionContext instance */
  std::unique_ptr<ExecutionContext> Build();

  /**
   * Set the query parameters for the execution context.
   * @param parameters The query parameters
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithQueryParameters(std::vector<common::ManagedPointer<const sql::Val>> &&parameters) {
    parameters_ = std::move(parameters);
    return *this;
  }

  /**
   * Set the query parameters for the execution context.
   * @param parameter_exprs The collection of expressions from which the query parameters are derived
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithQueryParametersFrom(const std::vector<parser::ConstantValueExpression> &parameter_exprs);

  /**
   * Set the database OID for the execution context.
   * @param db_oid The database OID
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithDatabaseOID(const catalog::db_oid_t db_oid) {
    db_oid_ = db_oid;
    return *this;
  }

  /**
   * Set the transaction context for the execution context.
   * @param txn The transaction context
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithTxnContext(common::ManagedPointer<transaction::TransactionContext> txn) {
    txn_ = txn;
    return *this;
  }

  /**
   * Set the output schema for the execution context.
   * @param output_schema The output schema
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithOutputSchema(common::ManagedPointer<const planner::OutputSchema> output_schema) {
    output_schema_ = output_schema;
    return *this;
  }

  /**
   * Set the output callback for the execution context.
   * @param output_callback The output callback
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithOutputCallback(OutputCallback output_callback) {
    output_callback_.emplace(std::move(output_callback));
    return *this;
  }

  /**
   * Set the catalog accessor for the execution context.
   * @param accessor The catalog accessor
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithCatalogAccessor(common::ManagedPointer<catalog::CatalogAccessor> accessor) {
    catalog_accessor_ = accessor;
    return *this;
  }

  /**
   * Set the execution settings for the execution context.
   * @param exec_settings The execution settings
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithExecutionSettings(exec::ExecutionSettings exec_settings) {
    exec_settings_.emplace(exec_settings);
    return *this;
  }

  /**
   * Set the metrics manager for the execution context.
   * @param metrics_manager The metrics manager
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithMetricsManager(common::ManagedPointer<metrics::MetricsManager> metrics_manager) {
    metrics_manager_ = metrics_manager;
    return *this;
  }

  /**
   * Set the replication manager for the execution context.
   * @param replication_manager The replication manager
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithReplicationManager(
      common::ManagedPointer<replication::ReplicationManager> replication_manager) {
    replication_manager_ = replication_manager;
    return *this;
  }

  /**
   * Set the recovery manager for the execution context.
   * @param recovery_manager The recovery manager
   * @return Builder reference for chaining
   */
  ExecutionContextBuilder &WithRecoveryManager(common::ManagedPointer<storage::RecoveryManager> recovery_manager) {
    recovery_manager_ = recovery_manager;
    return *this;
  }

 private:
  /** The query execution settings */
  std::optional<exec::ExecutionSettings> exec_settings_;
  /** The query parmeters */
  std::vector<common::ManagedPointer<const sql::Val>> parameters_;
  /** The database OID */
  catalog::db_oid_t db_oid_{catalog::INVALID_DATABASE_OID};
  /** The associated transaction */
  std::optional<common::ManagedPointer<transaction::TransactionContext>> txn_;
  /** The output callback */
  std::optional<OutputCallback> output_callback_;
  /** The output schema */
  std::optional<common::ManagedPointer<const planner::OutputSchema>> output_schema_;
  /** The catalog accessor */
  std::optional<common::ManagedPointer<catalog::CatalogAccessor>> catalog_accessor_;
  /** The metrics manager */
  std::optional<common::ManagedPointer<metrics::MetricsManager>> metrics_manager_;
  /** The replication manager */
  std::optional<common::ManagedPointer<replication::ReplicationManager>> replication_manager_;
  /** The recovery manager */
  std::optional<common::ManagedPointer<storage::RecoveryManager>> recovery_manager_;
};

}  // namespace noisepage::execution::exec
