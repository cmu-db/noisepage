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

namespace noisepage::catalog {
class CatalogAccessor;
}  // namespace noisepage::catalog

namespace noisepage::common {
class Graph;
}  // namespace noisepage::common

namespace noisepage::binder {
namespace detail {

// ----------------------------------------------------------------------------
// ContextSensitiveTableRef
// ----------------------------------------------------------------------------

/**
 * A ContextSensitiveTableRef is just a wrapper around
 * a regular TableRef with some extra metadata attached -
 * namely, we need to keep track of the depth and position
 * of table references within the original parse tree in
 * order to perform some subsequent validations.
 */
class ContextSensitiveTableRef {
 public:
  /**
   * Construct a new ContextSensitiveTableRef instance.
   * @param id The unique identifier for this instance
   * @param depth The depth of this table reference, relative to the root statement
   * @param position The position of this table reference relative to
   * other table references in the same scope
   * @param table The associated table reference
   */
  ContextSensitiveTableRef(std::size_t id, std::size_t depth, std::size_t position,
                           common::ManagedPointer<parser::TableRef> table);

  /** @return The unique identifier for this reference */
  std::size_t Id() const noexcept { return id_; }

  /** @return The depth at which this reference appears */
  std::size_t Depth() const noexcept { return depth_; }

  /** @return The lateral position of this reference */
  std::size_t Position() const noexcept { return position_; }

  /** @return The table associated with this context-sensitive reference */
  common::ManagedPointer<parser::TableRef> Table() const { return table_; }

  /** @return The collection of identifier's for this reference's dependencies */
  const std::unordered_set<std::size_t> &Dependencies() const { return dependencies_; }

  /**
   * Add a dependency for this context-sensitive reference.
   * @param id The unique identifier for the dependency
   */
  void AddDependency(std::size_t id);

 private:
  // The unique identifier for the context-sensitive reference
  const std::size_t id_;

  // The depth of the reference, relative to the root of the statement
  const std::size_t depth_;

  // The lateral position of the reference, within its local scope
  const std::size_t position_;

  // The associated table
  const common::ManagedPointer<parser::TableRef> table_;

  // The identifiers for the context-sensitive references on which this depends
  std::unordered_set<std::size_t> dependencies_;
};
}  // namespace detail

// ----------------------------------------------------------------------------
// TableDependencyGraph
// ----------------------------------------------------------------------------

/**
 * The TableDependencyGraph class encapsulates the logic necessary
 * to compute a graph of the dependencies among table references within
 * a query statement. Currently, this is utilized in the context of the
 * table references produced by common table expressions.
 */
class TableDependencyGraph {
 public:
  /**
   * The BuildContext class encapsulates the context that is
   * passed through recursive calls while building the graph.
   */
  class BuildContext {
   public:
    /** @return The next unique identifier */
    std::size_t NextId() noexcept { return next_id_++; }

   private:
    // The unique ID counter
    std::size_t next_id_;
  };

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
    const std::string alias_;
    const std::size_t depth_;
    const std::size_t position_;
  };

  /**
   * The Edge class provides a convenient way to query the graph.
   */
  class Edge {
   public:
    /**
     * Construct a new Edge instance.
     * @param src Source vertex
     * @param dst Destination vertex
     */
    Edge(Vertex src, Vertex dst) : src_{std::move(src)}, dst_{std::move(dst)} {}

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
    const Vertex src_;
    const Vertex dst_;
  };

 public:
  /**
   * Construct a new dependency graph instance from a root SELECT statement.
   * @param root The root statement of the parse tree
   */
  explicit TableDependencyGraph(common::ManagedPointer<parser::SelectStatement> root);

  /**
   * Construct a new dependency graph instance a root INSERT statement.
   * @param root The root statement of the parse tree
   */
  explicit TableDependencyGraph(common::ManagedPointer<parser::InsertStatement> root);

  /**
   * Construct a new dependency graph instance from a root UPDATE statement.
   * @param root The root statement of the parse tree
   */
  explicit TableDependencyGraph(common::ManagedPointer<parser::UpdateStatement> root);

  /**
   * Construct a new dependency graph instance from a root DELETE statement.
   * @param root The root statement of the parse tree
   */
  explicit TableDependencyGraph(common::ManagedPointer<parser::DeleteStatement> root);

  /** @return The order of the graph (cardinality of the vertex set) */
  std::size_t Order() const;

  /** @return The size of the graph (cardinality of the edge set) */
  std::size_t Size() const;

  /** @return `true` if the graph contains the vertex `vertex`, `false` otherwise */
  bool HasVertex(const Vertex &vertex) const;

  /** @return `true` if the graph contains the edge `edge`, `false` otherwise */
  bool HasEdge(const Edge &edge) const;

 private:
  /**
   * Recursively construct the table dependency graph.
   * @param select The SELECT statement from which to continue graph construction
   * @param id The unique identifier for the context-sensitive table reference
   * that is defined (at least in part) by thie SELECT statement
   * @param depth The depth of this statement, relative to the root statement
   * @param position The lateral position of this statement within its scope
   * @param context The build context
   */
  void BuildFromVisit(common::ManagedPointer<parser::SelectStatement> select, std::size_t id, std::size_t depth,
                      std::size_t position, BuildContext *context);

  /**
   * Recursively construct the table dependency graph.
   * @param select The TableRef from which to continue graph construction
   * @param id The unique identifier for the context-sensitive table reference
   * that is considered in this recursive invocation
   * @param depth The depth of this statement, relative to the root statement
   * @param position The lateral position of this table reference within its scope
   * @param context The build context
   */
  void BuildFromVisit(common::ManagedPointer<parser::TableRef> table_ref, std::size_t id, std::size_t depth,
                      std::size_t position, BuildContext *context);

  /**
   * Get a pointer to the table reference identified by `vertex`.
   * @param vertex The vertex that represents the graph query
   * @return A pointer to the table reference, or `nullptr`
   */
  const detail::ContextSensitiveTableRef *GetVertex(const Vertex &vertex) const;

  /**
   * Get pointers to the table references identified by `edge`.
   * @param edge The edge that represents the graph query
   * @return A pair of pointers to the table references, or `nullptr`s
   */
  std::pair<const detail::ContextSensitiveTableRef *, const detail::ContextSensitiveTableRef *> GetEdge(
      const Edge &edge) const;

 private:
  // The underlying representation for the graph
  std::vector<detail::ContextSensitiveTableRef> graph_;
};

// ----------------------------------------------------------------------------
// BinderCteUtil
// ----------------------------------------------------------------------------

class BinderCteUtil {
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

}  // namespace noisepage::binder
