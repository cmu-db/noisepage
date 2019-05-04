#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "execution/ast/identifier.h"
#include "execution/util/common.h"

namespace tpl::sql {

class Table;

#define TABLES(V)              \
  V(EmptyTable, "empty_table") \
  V(Test1, "test_1")

enum class TableId : u16 {
#define ENTRY(Id, ...) Id,
  TABLES(ENTRY)
#undef ENTRY
};

/// A catalog of all the databases and tables in the system. Only one exists
/// per TPL process, and its instance can be acquired through
/// \ref Catalog::instance(). At this time, the catalog is read-only after
/// startup. If you want to add a new table, modify the \ref TABLES macro which
/// lists the IDs of all tables, and configure your table in the Catalog
/// initialization function.
class Catalog {
 public:
  /// Access singleton Catalog object. Singletons are bad blah blah blah ...
  /// \return The singleton Catalog object
  static Catalog *Instance();

  /// Lookup a table in this catalog by name
  /// \param name The name of the target table
  /// \return A pointer to the table, or NULL if the table doesn't exist.
  Table *LookupTableByName(const std::string &name) const;

  /// Lookup a table in this catalog by name, using an identifier
  /// \param name The name of the target table
  /// \return A pointer to the table, or NULL if the table doesn't exist.
  Table *LookupTableByName(ast::Identifier name) const;

  /// Lookup a table in this catalog by ID
  /// \param table_id The ID of the target table
  /// \return A pointer to the table, or NULL if the table doesn't exist.
  Table *LookupTableById(TableId table_id) const;

 private:
  Catalog();

  ~Catalog();

 private:
  std::unordered_map<TableId, std::unique_ptr<Table>> table_catalog_;
};

}  // namespace tpl::sql
