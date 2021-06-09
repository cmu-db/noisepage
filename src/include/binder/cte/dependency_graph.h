#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

enum class RefType;
class LexicalScope;
class StructuredStatement;
class ContextSensitiveTableRef;

/**
 * Defines the type of the reference represented by the vertex;
 * there is a one-to-one relationship between VertexType and RefType.
 */
enum class VertexType { READ, WRITE };

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
  Vertex(std::string alias, VertexType type, const std::size_t depth, const std::size_t position)
      : alias_{std::move(alias)}, type_{type}, depth_{depth}, position_{position} {}

  /** @return The vertex alias */
  const std::string &Alias() const { return alias_; }

  /** @return The vertex type */
  VertexType Type() const { return type_; }

  /** @return The vertex depth */
  std::size_t Depth() const { return depth_; }

  /** @return The vertex position */
  std::size_t Position() const { return position_; }

 private:
  // The vertex alias
  const std::string alias_;

  // The type of the vertex
  const VertexType type_;

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

 public:
  /**
   * Resolve the dependencies for the specified table reference.
   * @param table_ref The table reference for which dependencies should be resolved
   * @return A set of all resolved dependencies for the reference
   * @throws BINDER_EXCEPTION in the event that reference resolution fails
   */
  static std::unordered_set<const ContextSensitiveTableRef *> ResolveDependenciesFor(
      const ContextSensitiveTableRef &table_ref);

  /**
   * Resolve a READ reference the corresponding WRITE to which it refers.
   * @param table_ref The READ reference to be resolved
   * @return A pointer to the corresponding WRITE table reference
   * @throws BINDER_EXCEPTION in the event that reference resolution fails
   */
  static const ContextSensitiveTableRef *ResolveReference(const ContextSensitiveTableRef &table_ref);

  /**
   * Find a WRITE table reference idenfitied by `alias` within `scope`.
   * @param alias The alias of the table reference for which to search
   * @param scope The scope in which to perform the search
   * @return A pointer to the table reference if found, `NOT_FOUND` (nullptr) otherwise
   */
  static const ContextSensitiveTableRef *FindWriteReferenceInScope(std::string_view alias, const LexicalScope &scope);

  /**
   * Find a WRITE table reference idenfitied by `alias` within any enclosing scope of `scope`.
   * @param alias The alias of the table reference for which to search
   * @param scope The base scope; the search begins from the enclosing scope of `scope`
   * @return A pointer to the table reference if found, `NOT_FOUND` (nullptr) otherwise
   */
  static const ContextSensitiveTableRef *FindWriteReferenceInAnyEnclosingScope(std::string_view alias,
                                                                               const LexicalScope &scope);

  /**
   * Find a forward WRITE table reference identified by `alias` within `scope`.
   * @param alias The alias of the table reference for which to search
   * @param scope The scope in which to perform the search
   * @param partition_point The scope that serves as the partition point;
   * in the case of forward-reference resolution, the search only considers
   * those WRITE references that define a scope "to the right" of the partition
   * point. The partition point itself is NOT considered in the search
   * @return A pointer to the table reference if found, `NOT_FOUND` (nullptr) otherwise
   */
  static const ContextSensitiveTableRef *FindForwardWriteReferenceInScope(std::string_view alias,
                                                                          const LexicalScope &scope,
                                                                          const LexicalScope &partition_point);

  /**
   * Find a backward WRITE table reference identified by `alias` within `scope`.
   * @param alias The alias of the table reference for which to search
   * @param scope The scope in which to perform the search
   * @param partition_point The scope that serves as the partition point;
   * in the case of backward-reference resolution, the search only considers
   * those WRITE references that define a scope "to the left" of the partition
   * point. The partition point itself is NOT considered in the search
   * @return A pointer to the table reference if found, `NOT_FOUND` (nullptr) otherwise
   */
  static const ContextSensitiveTableRef *FindBackwardWriteReferenceInScope(std::string_view alias,
                                                                           const LexicalScope &scope,
                                                                           const LexicalScope &partition_point);

 private:
  /**
   * Determine if the given structured statement is valid.
   * @param statement The structured statement to validate
   * @throws BINDER_EXCEPTION on invalid statement
   */
  static void ValidateStructuredStatement(const StructuredStatement &statement);

  /**
   * Determine if the given scope contains any ambiguous references.
   * @param scope The scope from which to begin the validation
   */
  static bool ContainsAmbiguousReferences(const LexicalScope &scope);

  /**
   * Locate the reference identified by the given parameters in the graph.
   * @pre A reference satisfying the parameters is present in the graph
   * @param alias The alias that identifies the table reference
   * @param type The type of the table reference
   * @param depth The depth of the reference in the statement
   * @param position The position of the reference in its enclosing scope
   * @return A pointer to the corresponding table reference
   */
  ContextSensitiveTableRef *FindRef(std::string_view alias, RefType type, std::size_t depth,
                                    std::size_t position) const;

 public:
  /** Denotes the scope is not found when resolving references */
  static constexpr const ContextSensitiveTableRef *NOT_FOUND{nullptr};

 private:
  /** The key type in the underlying map */
  using KeyType = ContextSensitiveTableRef *;

  /** The value type in the underlying map */
  using ValueType = std::unordered_set<const ContextSensitiveTableRef *>;

  /** The entry type for (key, value) pairs in the map */
  using EntryType = std::pair<KeyType, ValueType>;

  /** The underlying representation for the graph */
  std::unordered_map<KeyType, ValueType> graph_;

  /** The corresponding structured statement */
  std::unique_ptr<StructuredStatement> statement_;
};
}  // namespace noisepage::binder::cte
