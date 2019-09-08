#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"

namespace terrier {

namespace parser {
class TableRef;
}

namespace transaction {
class TransactionContext;
}

namespace catalog {
class Schema;
}

namespace binder {

/**
 * @brief Store the visible table alias and the corresponding <db_id, table_id>
 *  tuple. Also record the upper level context when traversing into the nested
 *  query. This context keep track of all the table alias that the column in the
 *  current level can bind to.
 */
class BinderContext {
 public:
  /**
   * Initializes the BinderContext object which has an empty regular table map and an empty nested table map.
   * It also takes in a pointer to the binder context's upper context, and the constructor determines the depth of the
   * current context based on the upper context. These two fields are used in nested queries.
   * @param upper_context Pointer to the upper level binder context of the current binder context.
   *
   */
  explicit BinderContext(BinderContext *upper_context) : upper_context_(upper_context) {
    if (upper_context != nullptr) depth_ = upper_context->depth_ + 1;
  }

  /**
   * Update the table alias map given a table reference (in the from clause)
   * @param accessor Pointer to the catalog accessor object
   * @param table_ref Pointer to the table ref object
   */
  void AddRegularTable(const std::unique_ptr<catalog::CatalogAccessor> &accessor, parser::TableRef *table_ref);

  /**
   * Update the table alias map given a table reference (in the from clause)
   * @param accessor Pointer to the catalog accessor object
   * @param db_name Name of the database
   * @param table_name Name of the table
   * @param table_alias Alias of the table
   */
  void AddRegularTable(const std::unique_ptr<catalog::CatalogAccessor> &accessor, const std::string &db_name,
                       const std::string &table_name, const std::string &table_alias);

  /**
   * Update the nested table alias map
   * @param table_alias Alias of the table
   * @param select_list List of select columns
   */
  void AddNestedTable(const std::string &table_alias,
                      const std::vector<common::ManagedPointer<parser::AbstractExpression>> &select_list);

  /**
   * Check if the current context has any table
   * @param current_context Pointer to the current binder context object
   */
  bool HasTables() { return (!regular_table_alias_map_.empty() || !nested_table_alias_map_.empty()); }

  /**
   * Check if the column name is in the schema
   * @param schema Schema object
   * @param col_name Name of the column
   * @return true if the column is in the schema, false otherwise
   */
  static bool ColumnInSchema(const catalog::Schema &schema, const std::string &col_name);

  /**
   * Construct the column position tuple given column name and the corresponding column value expression.
   * Note that this is just a helper function and it is independent of the context.
   * @param col_name Name of the column
   * @param tuple Tuple of database oid, table oid, and schema object
   * @param expr Column value expression
   */
  static void SetColumnPosTuple(const std::string &col_name,
                                std::tuple<catalog::db_oid_t, catalog::table_oid_t, catalog::Schema> tuple,
                                parser::ColumnValueExpression *expr);

  /**
   * Construct the column position tuple given only the column value expression and the context.
   * Also internally update the column value expression according to the values in the context
   * @param current_context Pointer to the current binder context
   * @param expr Column value expression
   * @return Returns true if the column is found in the alias maps of the current context; false otherwise
   */
  bool SetColumnPosTuple(parser::ColumnValueExpression *expr);

  /**
   * Check if the table alias can be found in the alias maps of the current context or the upper contexts.
   * This function internally updates the depth of the expression if the alias is successfully found
   * @param current_context Current binder context
   * @param alias Table alias
   * @param expr Column value expression
   * @param tuple Tuple of database oid, table oid, and schema object
   * @return Return true if the alias is found, false otherwise
   */
  bool GetRegularTableObj(const std::string &alias, parser::ColumnValueExpression *expr,
                          std::tuple<catalog::db_oid_t, catalog::table_oid_t, catalog::Schema> *tuple);

  /**
   * Check if the table, represented by the table alias, has the column indicated by the column name.
   * This function internally updates the information of the expression if the column is successfully found
   * @param current_context Current binder context
   * @param alias Table alias
   * @param col_name Name of the column
   * @param expr Column value expression
   * @return Return true if the column is found, false otherwise
   */
  bool CheckNestedTableColumn(const std::string &alias, const std::string &col_name,
                              parser::ColumnValueExpression *expr);

  /**
   * Get the pointer to the upper context of the current context
   * @return Pointer to the upper binder context
   */
  BinderContext *GetUpperContext() { return upper_context_; }

  // TODO(Ling): not sure if we should do assign or move... will the calling method still need it?
  //  we don't have function calling this two functions in peloton even
  /**
   * Set the upper context of the current context
   * @param upper_context Pointer to the upper binder context
   */
  void SetUpperContext(BinderContext *upper_context) { upper_context_ = upper_context; }

  /**
   * Set the depth of the current context
   * @param depth Depth of the context
   */
  void SetDepth(int depth) { depth_ = depth; }

  /**
   * Get the depth of the current context
   * @return depth of the current binder context
   */
  const int GetDepth() { return depth_; }

  /**
   * Generate list of column value expression that covers all columns in the alias maps of the current context
   * @param exprs Pointer to the list of column value expression.
   * The generated column value expressions will be placed in this list.
   */
  void GenerateAllColumnExpressions(std::vector<common::ManagedPointer<parser::AbstractExpression>> *exprs);

 private:
  /**
   * Map table alias to a tuple of database oid, table oid, and schema of the table
   */
  std::unordered_map<std::string, std::tuple<catalog::db_oid_t, catalog::table_oid_t, catalog::Schema>>
      regular_table_alias_map_;

  /**
   * Map the table alias to maps which is from table alias to the value type
   */
  std::unordered_map<std::string, std::unordered_map<std::string, type::TypeId>> nested_table_alias_map_;

  /**
   * Upper binder context of the current binder context
   */
  BinderContext *upper_context_;

  /**
   * depth of the current binder context
   */
  int depth_ = 0;
};

}  // namespace binder
}  // namespace terrier
