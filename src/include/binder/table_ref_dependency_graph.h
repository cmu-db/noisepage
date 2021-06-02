#pragma once

#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/error/exception.h"
#include "common/macros.h"
#include "common/managed_pointer.h"

namespace noisepage::parser {
class TableRef;
class SelectStatement;
}  // namespace noisepage::parser

namespace noisepage::catalog {
class CatalogAccessor;
}  // namespace noisepage::catalog

namespace noisepage::common {
class Graph;
}  // namespace noisepage::common

namespace noisepage::binder {

class TableRefDependencyGraphVisitor;

/**
 * The TableRefDependencyGraph class encapsulates the logic necessary
 * to compute a graph of the dependencies among table references within
 * a query. Currently, this is utilized in the context of the table
 * references produced by common table expressions.
 */
class TableRefDependencyGraph {
  /**
   * The key type in the underlying map that implements the graph.
   */
  using KeyType = common::ManagedPointer<parser::TableRef>;

  /**
   * The value type in the underlying map that implements the graph.
   */
  using ValueType = std::unordered_set<common::ManagedPointer<parser::TableRef>>;

  /**
   * The type that implements the underlying graph.
   */
  using GraphType = std::unordered_map<KeyType, ValueType>;

 public:
  /**
   * Construct a new dependency graph instance.
   * @param root_select The root SELECT statement
   * @param catalog_accessor The catalog accessor
   */
  TableRefDependencyGraph(common::ManagedPointer<parser::SelectStatement> root_select,
                          common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor);

  ~TableRefDependencyGraph() = default;

  // Non-copyable
  TableRefDependencyGraph(const TableRefDependencyGraph &) = delete;
  TableRefDependencyGraph &operator=(const TableRefDependencyGraph &) = delete;

  // Non-movable
  TableRefDependencyGraph(TableRefDependencyGraph &&) = delete;
  TableRefDependencyGraph &operator=(TableRefDependencyGraph &&) = delete;

  /**
   * Compute the Graph representation of the dependency graph.
   * @param metadata The metadata structure that supports subsequent
   * translation of graph algorithm results back to dependency graph domain
   * @return The Graph representation
   */
  common::Graph ToGraph(std::unordered_map<std::size_t, common::ManagedPointer<parser::TableRef>> *metadata) const;

 private:
  friend class TableRefDependencyGraphVisitor;

 private:
  /**
   * The catalog accessor for the graph instance.
   */
  common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor_;

  /**
   * The map that implements the graph.
   */
  GraphType graph_;
};

/**
 * Implements the visitor pattern (or something approaching it)
 * for constructing the table reference dependency graph.
 */
class TableRefDependencyGraphVisitor {
  /**
   * The type that implements the underlying graph.
   */
  using GraphType = TableRefDependencyGraph::GraphType;

 public:
  /**
   * Construct a new dependency graph builder
   */
  explicit TableRefDependencyGraphVisitor(GraphType *graph);

  /**
   * Visit a table reference.
   * @param table_ref The table reference
   */
  void Visit(common::ManagedPointer<parser::TableRef> table_ref);

  /**
   * Visit a SELECT statement.
   * @param select The SELECT statement
   */
  void Visit(common::ManagedPointer<parser::SelectStatement> select);

 private:
  /**
   * The current table reference under consideration.
   */
  common::ManagedPointer<parser::TableRef> current_ref_{nullptr};

  /**
   * A mutable pointer to the graph that is populated by this visitor.
   */
  GraphType *graph_;
};

}  // namespace noisepage::binder
