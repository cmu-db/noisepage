#pragma once

#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/error/exception.h"
#include "common/macros.h"
#include "common/managed_pointer.h"

namespace noisepage::parser {
class TableRef;
class UpdateStatement;
class SelectStatement;
class InsertStatement;
class DeleteStatement;
}  // namespace noisepage::parser

namespace noisepage::binder::cte {
/**
 * A static class for Binder utilities that are specific to CTEs.
 */
class BinderUtil {
 public:
  /**
   * Compute the order in which temporary CTE tables associated with the SELECT
   * statement should be visited during binding, respecting table dependencies.
   *
   * During this process we perform an analysis of table reference dependencies.
   * If a violation of intra-statement dependency constraints is found, we throw.
   *
   * @param select_statement The SELECT statement at which to root the analysis
   * @return An ordered collection of the WITH tables for this statement
   */
  static std::vector<common::ManagedPointer<parser::TableRef>> GetSelectWithOrder(
      common::ManagedPointer<parser::SelectStatement> select_statement);

  /**
   * Compute the order in which temporary CTE tables associated with the INSERT
   * statement should be visited during binding, respecting table dependencies.
   *
   * During this process we perform an analysis of table reference dependencies.
   * If a violation of intra-statement dependency constraints is found, we throw.
   *
   * @param insert_statement The INSERT statement at which to root the analysis
   * @return An ordered collection of the WITH tables for this statement
   */
  static std::vector<common::ManagedPointer<parser::TableRef>> GetInsertWithOrder(
      common::ManagedPointer<parser::InsertStatement> insert_statement);

  /**
   * Compute the order in which temporary CTE tables associated with the UPDATE
   * statement should be visited during binding, respecting table dependencies.
   *
   * During this process we perform an analysis of table reference dependencies.
   * If a violation of intra-statement dependency constraints is found, we throw.
   *
   * @param update_statement The UPDATE statement at which to root the analysis
   * @return An ordered collection of the WITH tables for this statement
   */
  static std::vector<common::ManagedPointer<parser::TableRef>> GetUpdateWithOrder(
      common::ManagedPointer<parser::UpdateStatement> update_statement);

  /**
   * Compute the order in which temporary CTE tables associated with the DELETE
   * statement should be visited during binding, respecting table dependencies.
   *
   * During this process we perform an analysis of table reference dependencies.
   * If a violation of intra-statement dependency constraints is found, we throw.
   *
   * @param delete_statement The DELETE statement at which to root the analysis
   * @return An ordered collection of the WITH tables for this statement
   */
  static std::vector<common::ManagedPointer<parser::TableRef>> GetDeleteWithOrder(
      common::ManagedPointer<parser::DeleteStatement> delete_statement);
};
}  // namespace noisepage::binder::cte
