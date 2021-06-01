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

namespace noisepage::binder {

class TableRefDependencyGraphVisitor;

/**
 * The AliasAdjacencyList class is a simplified representation
 * of the dependenc graph for table references; it is used as
 * an intermediary data structure to simplify testing.
 */
class AliasAdjacencyList {
 public:
  AliasAdjacencyList() = default;

  /**
   * Add an edge to the AdjacencyList instance.
   * @param src The source node identifier
   * @param dst The destination node identifier
   */
  void AddEdge(const std::string &src, const std::string &dst);

  /**
   * Get the vertex set for the AdjacencyList instance.
   * @return A collection of all node identifiers
   */
  std::vector<std::string> Nodes() const;

  /**
   * Get the adjacencies for the specified alias.
   * @pre The specified alias is present in the AdjacencyList
   * @param alias The alias for which to query
   * @return An immutable reference to the adjacencies for `alias`
   */
  const std::set<std::string> &AdjacenciesFor(const std::string &alias) const;

  /**
   * Determine if AdjacencyList `lhs` is equivalent to AdjacencyList `rhs`.
   * @param lhs Input adjacency list
   * @param rhs Input adjacency list
   * @return `true` if `lhs` is equivalent to `rhs`, `false` otherwise
   */
  static bool Equals(const AliasAdjacencyList &lhs, const AliasAdjacencyList &rhs);

 private:
  /**
   * The underlying adjacency list.
   *
   * NOTE: We use std::set rather than std::set here because we
   * rely on the container being sorted for equality comparison.
   */
  std::unordered_map<std::string, std::set<std::string>> adjacency_list_;
};

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
   * @param table_refs The collection of top-level table references
   * @param catalog_accessor The catalog accessor
   */
  TableRefDependencyGraph(const std::vector<common::ManagedPointer<parser::TableRef>> &table_refs,
                          common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor);

  ~TableRefDependencyGraph() = default;

  // Non-copyable
  TableRefDependencyGraph(const TableRefDependencyGraph &) = delete;
  TableRefDependencyGraph &operator=(const TableRefDependencyGraph &) = delete;

  // Non-movable
  TableRefDependencyGraph(TableRefDependencyGraph &&) = delete;
  TableRefDependencyGraph &operator=(TableRefDependencyGraph &&) = delete;

  /**
   * Get the AdjacencyList representation of the underlying graph.
   * @return An adjacency list representation
   */
  AliasAdjacencyList AdjacencyList() const;

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
