#pragma once

#include <string>
#include <tuple>
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

class LexicalScope;

/**
 * The StructuredStatement class encapsulates the logic for traversing
 * a raw parse tree from the parser and extracting the information related
 * to common table expressions that we can later use to construct a
 * dependency graph among temporary table references within the statement.
 */
class ParsedStatement {
 public:
  /**
   * A RefDescriptor provides a convenient way to uniquely
   * identify table references for testing purposes.
   */
  using RefDescriptor = std::tuple<std::string, std::size_t, std::size_t>;

  /**
   * The BuildContext class encapsulates the context that is
   * passed through recursive calls while building the graph.
   */
  class BuildContext {
   public:
    /** @return The next unique scope identifier */
    std::size_t NextScopeId() noexcept { return next_scope_id_++; }

   private:
    // The unique scope counter
    std::size_t next_scope_id_;
  };

 public:
  /**
   * Construct a new structure statement from a root SELECT statement.
   * @param root The root statement of the parse tree
   */
  explicit ParsedStatement(common::ManagedPointer<parser::SelectStatement> root);

  /**
   * Construct a new structured statement a root INSERT statement.
   * @param root The root statement of the parse tree
   */
  explicit ParsedStatement(common::ManagedPointer<parser::InsertStatement> root);

  /**
   * Construct a new structured statement from a root UPDATE statement.
   * @param root The root statement of the parse tree
   */
  explicit ParsedStatement(common::ManagedPointer<parser::UpdateStatement> root);

  /**
   * Construct a new structured statement from a root DELETE statement.
   * @param root The root statement of the parse tree
   */
  explicit ParsedStatement(common::ManagedPointer<parser::DeleteStatement> root);

  /** @return The number of table references in the structured statement */
  std::size_t RefCount() const;

  /** @return The number of read table references in the structured statement */
  std::size_t ReadRefCount() const;

  /** @return The number of write table references in the structured statement */
  std::size_t WriteRefCount() const;

  /** @return The number of reference dependencies in the structured statement */
  std::size_t DependencyCount() const;

  /** @return The number of scopes in the structured statement */
  std::size_t ScopeCount() const;

  /**
   * Determine if the structured statement contains the specified reference.
   * @param ref A descriptor for the reference of interest
   * @return `true` if the structured statement contains the reference, `false` otherwise
   */
  bool HasRef(const RefDescriptor &ref) const;

  /**
   * Determine if the structured statement contains the specified reference.
   * @param ref A descriptor for the reference of interest
   * @return `true` if the structured statement contains the reference, `false` otherwise
   */
  bool HasReadRef(const RefDescriptor &ref) const;

  /**
   * Determine if the structured statement contains the specified reference.
   * @param ref A descriptor for the reference of interest
   * @return `true` if the structured statement contains the reference, `false` otherwise
   */
  bool HasWriteRef(const RefDescriptor &ref) const;

  /**
   * Determine if the structured statement contains the specified dependency.
   * @param src A descriptor for the source reference
   * @param dst A descriptor for the destination reference
   * @return `true` if the structured statement contains the reference, `false` otherwise
   */
  bool HasDependency(const RefDescriptor &src, const RefDescriptor &dst) const;

  /** @return A collection of the unique identifiers for nodes in the graph */
  std::vector<std::size_t> Identifiers() const;

 private:
  friend class DependencyGraph;

  /** Dummy identifier we use during statement construction */
  static constexpr const std::size_t DONT_CARE_ID = 0UL;

  /**
   * Recursively construct the structured statement.
   * @param select The SELECT statement from which to continue graph construction
   * @param id The unique identifier for the context-sensitive table reference
   * that is defined (at least in part) by thie SELECT statement
   * @param scope The scope in which this statement appears
   * @param depth The depth of this statement, relative to the root statement
   * @param position The lateral position of this statement within its scope
   * @param context The build context
   */
  void BuildFromVisit(common::ManagedPointer<parser::SelectStatement> select, std::size_t id, std::size_t scope,
                      std::size_t depth, std::size_t position, BuildContext *context);

  /**
   * Recursively construct the structured statement.
   * @param select The TableRef from which to continue graph construction
   * @param id The unique identifier for the context-sensitive table reference
   * that is considered in this recursive invocation
   * @param scope The scope in which this table reference appears
   * @param depth The depth of this table reference, relative to the root statement
   * @param position The lateral position of this table reference within its scope
   * @param context The build context
   */
  void BuildFromVisit(common::ManagedPointer<parser::TableRef> table_ref, std::size_t id, std::size_t scope,
                      std::size_t depth, std::size_t position, BuildContext *context);

  /**
   * Add a context-sensitive table reference to the collection.
   * @param ref The new reference to add
   */
  void AddRef(const ContextSensitiveTableRef &ref);

  /**
   * Add a context-sensitive table reference to the collection.
   * @param ref The new reference to add
   */
  void AddRef(ContextSensitiveTableRef &&ref);

  /**
   * Add a new scope to the scope map.
   * @param scope The scope identifier
   * @param depth The depth of the scope
   */
  void AddScope(std::size_t scope, std::size_t depth);

  /**
   * Determine if the structured statement contains the specified reference.
   * @param ref A descriptor for the reference of interest
   * @param type The reference type
   * @return `true` if the structured statement contains the reference, `false` otherwise
   */
  bool HasRef(const RefDescriptor &ref, RefType type) const;

  /**
   * Get the identified reference.
   * @param ref A descriptor for the reference of interest
   * @return A mutable reference to the reference
   */
  ContextSensitiveTableRef &GetRef(std::size_t id);

  /**
   * Get the identified reference.
   * @param ref A descriptor for the reference of interest
   * @return An immutable reference to the reference
   */
  const ContextSensitiveTableRef &GetRef(std::size_t id) const;

  /**
   * Get the identified reference.
   * @param ref A descriptor for the reference of interest
   * @return A mutable reference to the reference
   */
  ContextSensitiveTableRef &GetRef(const RefDescriptor &ref);

  /**
   * Get the identified reference.
   * @param ref A descriptor for the reference of interest
   * @return An immutable reference to the reference
   */
  const ContextSensitiveTableRef &GetRef(const RefDescriptor &ref) const;

  /** @return An immutable reference to the underlying references collection */
  const std::vector<ContextSensitiveTableRef> &References() const;

  /** @return An immutable reference to the underlying scopes map */
  const std::unordered_map<std::size_t, std::size_t> &Scopes() const;

 private:
  // The root scope for the structured statement
  std::unique_ptr<LexicalScope> root_scope_;
};

}  // namespace noisepage::binder::cte
