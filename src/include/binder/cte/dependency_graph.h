#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"

namespace noisepage::parser {
class TableRef;
class UpdateStatement;
class SelectStatement;
class InsertStatement;
class DeleteStatement;
}  // namespace noisepage::parser

namespace noisepage::binder::cte {

class LexicalScope;
class StructuredStatement;

/**
 * The Vertex class provides a convenient way to query the graph.
 */
class Vertex {
 public:
  /**
   * Construct a new Vertex instance.
   * @param alias The vertex alias
   * @param depth The vertex depth
   * @param position The vertex position
   */
  Vertex(std::string alias, const std::size_t depth, const std::size_t position)
      : alias_{std::move(alias)}, depth_{depth}, position_{position} {}

  /** @return The vertex alias */
  const std::string &Alias() const { return alias_; }

  /** @return The vertex depth */
  std::size_t Depth() const { return depth_; }

  /** @return The vertex position */
  std::size_t Position() const { return position_; }

 private:
  // The vertex alias
  const std::string alias_;

  // The vertex depth
  const std::size_t depth_;

  // The vertex position
  const std::size_t position_;
};

/**
 * The Edge class provides a convenient way to query the graph.
 */
class Edge {
 public:
  /**
   * Construct a new Edge instance, assuming ownership of vertices.
   * @param src Source vertex
   * @param dst Destination vertex
   */
  Edge(Vertex &&src, Vertex &&dst) : src_{std::move(src)}, dst_{std::move(dst)} {}

  /** @return An immutable reference to the source vertex */
  const Vertex &Source() const { return src_; }

  /** @return An immutable reference to the destination vertex */
  const Vertex &Destination() const { return dst_; }

 private:
  // The source vertex
  const Vertex src_;

  // The destination vertex
  const Vertex dst_;
};

/**
 * The DependencyGraph class encapsulates the logic necessary
 * to compute a graph of the dependencies among table references within
 * a query statement. Currently, this is utilized in the context of the
 * table references produced by common table expressions.
 */
class DependencyGraph {
  /**
   * The TableReference class provides an internal representation
   * for table references within the dependency graph; it serves
   * as a graph vertex. Apologies in advance for the overloading
   * of the terms "ref" and "reference" that is going on here.
   */
  class TableReference {
   public:
    /**
     * Construct a new Vertex instance.
     * @param scope The scope in which the associated table reference appears
     * @param position The lateral position of the table reference within its scope
     * @param table The associated table reference
     */
    TableReference(common::ManagedPointer<parser::TableRef> table, const LexicalScope *scope);

    /** @return The scope for this table reference */
    const LexicalScope *Scope() const { return scope_; }

    /** @return The table reference for this table reference */
    common::ManagedPointer<parser::TableRef> Table() const { return table_; }

    /** @return The dependencies for this table reference */
    const std::vector<const TableReference *> &Dependencies() const { return dependencies_; }

    /**
     * Add a new dependency for this table reference.
     * @param ref A pointer to the the dependency
     */
    void AddDependency(const TableReference *ref) { dependencies_.push_back(ref); }

   private:
    // The associated table reference
    common::ManagedPointer<parser::TableRef> table_;

    // The scope in which the reference appears
    const LexicalScope *scope_;

    // A collection of the dependencies for this vertex
    std::vector<const TableReference *> dependencies_;
  };

 public:
  /**
   * Construct a dependency graph from a StructuredStatement.
   * @param statement The structured statement
   */
  explicit DependencyGraph(std::unique_ptr<StructuredStatement> &&statement);

  /**
   * Construct a new dependency graph instance from a root SELECT statement.
   * @param root The root statement
   */
  static std::unique_ptr<DependencyGraph> Build(common::ManagedPointer<parser::SelectStatement> root);

  /**
   * Construct a new dependency graph instance from a root SELECT statement.
   * @param root The root statement
   */
  static std::unique_ptr<DependencyGraph> Build(common::ManagedPointer<parser::InsertStatement> root);

  /**
   * Construct a new dependency graph instance from a root SELECT statement.
   * @param root The root statement
   */
  static std::unique_ptr<DependencyGraph> Build(common::ManagedPointer<parser::UpdateStatement> root);

  /**
   * Construct a new dependency graph instance from a root SELECT statement.
   * @param root The root statement
   */
  static std::unique_ptr<DependencyGraph> Build(common::ManagedPointer<parser::DeleteStatement> root);

  /** @return The order of the graph (cardinality of the vertex set) */
  std::size_t Order() const;

  /** @return The size of the graph (cardinality of the edge set) */
  std::size_t Size() const;

  /** @return `true` if the graph contains the vertex `vertex`, `false` otherwise */
  bool HasVertex(const Vertex &vertex) const;

  /** @return `true` if the graph contains the edge `edge`, `false` otherwise */
  bool HasEdge(const Edge &edge) const;

  /**
   * Check all consistency constraints for the underlying dependency graph.
   * @return `true` if the graph is valid, `false` otherwise
   */
  bool CheckAll() const;

  /**
   * Check forward reference constraints for the underlying dependency graph.
   *
   * A forward reference is a dependency from CTE A to CTE B (A depends on B)
   * where CTE B appears "to the right" of CTE A in the input query. For instance,
   * the following query is invalid because it contains a forward reference:
   *
   *  WITH x(i) AS (SELECT * FROM y), y(j) AS (SELECT 1) SELECT * FROM x;
   *
   * However, forward references are permitted in some special cases. First,
   * forward references are permitted for inductive CTEs, so the following
   * query is valid:
   *
   *  WITH RECURSIVE x(i) AS (SELECT * FROM y), y(j) AS (SELECT 1) SELECT * FROM x;
   *
   * Furthermore, forward references are also permitted for nested CTEs regardless
   * of their position in the statement relative to the target:
   *
   *  WITH
   *    x(i) AS (WITH a(m) AS (SELECT * FROM y) SELECT * FROM a),
   *    y(j) AS (SELECT 1)
   *  SELECT * FROM y;
   *
   * @return `true` if the graph is valid, `false` otherwise
   */
  bool CheckForwardReferences() const;

  /**
   * Check mutual recursion constraints for the underlying dependency graph.
   *
   * Mutual recursion occurs when CTE A reads CTE B and CTE B reads CTE A.
   * It is only present in inductive CTEs. For instance, the query below
   * fails because of normal "visibility" rules for non-inductive CTEs:
   *
   *  WITH x(i) AS (SELECT * FROM y), y(j) AS (SELECT * FROM x) SELECT * FROM y;
   *
   * However, the query should also fail in the event that the CTEs are recursive:
   *
   *  WITH RECURSIVE x(i) AS (SELECT * FROM y), y(j) AS (SELECT * FROM x) SELECT * FROM y;
   *
   * @return `true` if the graph is valid, `false` otherwise
   */
  bool CheckMutualRecursion() const;

  /**
   * Check nested scope dependency constraints for the underlying dependency graph.
   *
   * A dependency on a nested scope occurs when CTE A reads CTE C that appears within
   * the subsquery that defines CTE B. For instance, the following query is invalid
   * because it contains a dependency on a nested scope:
   *
   *  WITH x(i) AS (WITH a(m) AS (SELECT 1) SELECT * FROM a), y(j) AS (SELECT * FROM a) SELECT * FROM y;
   *
   * @return `true` if the graph is valid, `false` otherwise
   */
  bool CheckNestedScopes() const;

 private:
  /**
   * Recursively populate the graph from the given LexicalScope.
   * @param scope The root lexical scope from which to continue population
   */
  void PopulateGraphVisit(const LexicalScope &scope);

  /**
   *
   */
  void ResolveReference(TableReference *table_ref);

 private:
  /** The underlying representation for the graph */
  std::vector<TableReference> graph_;

  /** The corresponding structured statement */
  std::unique_ptr<StructuredStatement> statement_;
};
}  // namespace noisepage::binder::cte
