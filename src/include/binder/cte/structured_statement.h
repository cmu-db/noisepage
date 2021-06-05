#pragma once

#include <memory>
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

enum class RefType;
class LexicalScope;
class TypedTableRef;

/**
 * The StructuredStatement class encapsulates the logic for traversing
 * a raw parse tree from the parser and extracting the information related
 * to common table expressions that we can later use to construct a
 * dependency graph among temporary table references within the statement.
 */
class StructuredStatement {
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
  explicit StructuredStatement(common::ManagedPointer<parser::SelectStatement> root);

  /**
   * Construct a new structured statement a root INSERT statement.
   * @param root The root statement of the parse tree
   */
  explicit StructuredStatement(common::ManagedPointer<parser::InsertStatement> root);

  /**
   * Construct a new structured statement from a root UPDATE statement.
   * @param root The root statement of the parse tree
   */
  explicit StructuredStatement(common::ManagedPointer<parser::UpdateStatement> root);

  /**
   * Construct a new structured statement from a root DELETE statement.
   * @param root The root statement of the parse tree
   */
  explicit StructuredStatement(common::ManagedPointer<parser::DeleteStatement> root);

  /**
   * Must declare the destructor here and define in the .cpp file in order
   * to avoid issues with type-incompleteness of LexicalScope.
   */
  ~StructuredStatement();

  /** @return The number of table references in the structured statement */
  std::size_t RefCount() const;

  /** @return The number of table references that match the specified description */
  std::size_t RefCount(const RefDescriptor &ref) const;

  /** @return The number of read table references in the structured statement */
  std::size_t ReadRefCount() const;

  /** @return The number of read table references that match the specified description */
  std::size_t ReadRefCount(const RefDescriptor &ref) const;

  /** @return The number of write table references in the structured statement */
  std::size_t WriteRefCount() const;

  /** @return The number of write table references that match the specified description */
  std::size_t WriteRefCount(const RefDescriptor &ref) const;

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

 private:
  friend class DependencyGraph;

  /**
   * Recursively construct the structured statement.
   * @param select The SELECT statement from which to continue graph construction
   * @param scope The scope in which the statement appears
   * @param context The build context
   */
  void BuildFromVisit(common::ManagedPointer<parser::SelectStatement> select, LexicalScope *scope,
                      BuildContext *context);

  /**
   * Recursively construct the structured statement.
   * @param select The TableRef from which to continue graph construction
   * @param scope The scope in the table reference appears
   * @param context The build context
   */
  void BuildFromVisit(common::ManagedPointer<parser::TableRef> table_ref, LexicalScope *scope, BuildContext *context);

  /**
   * Count the number of references that match the given
   * reference descriptor, with the given type.
   * @param ref A descriptor for the reference of interest
   * @param type The reference type
   * @return The number of references that match the description
   */
  std::size_t RefCount(const RefDescriptor &ref, RefType type) const;

  /**
   * Compute the number of table references with the given type.
   * @param type The reference type of interest
   */
  std::size_t RefCountWithType(RefType type) const;

  /** @return A mutable reference to the root scope of the statement */
  LexicalScope &RootScope();

  /** @return An immutable reference to the root scope of the statement */
  const LexicalScope &RootScope() const;

  /** @return A flattened collection of all table references in the statement */
  std::vector<const TypedTableRef *> References() const;

  /**
   * Recursively determine if the invariants of the structured statement are satisfied.
   * @param scope The scope from which to begin the check
   * @return `true` if invariants are satisfied for the root scope
   * and all enclosed scopes, `false` otherwise
   */
  static bool InvariantsSatisfied(const LexicalScope &scope);

  /**
   * Flatten the scope hierarchy.
   * @param root The root scope
   * @param result The container to which the hierarchy is flattened
   */
  static void FlattenTo(const LexicalScope *root, std::vector<const LexicalScope *> *result);

 private:
  // The root scope for the structured statement
  std::unique_ptr<LexicalScope> root_scope_;

  // A flattened view of the scope hierarchy so we can easily apply std algorithms
  std::vector<const LexicalScope *> flat_scopes_;
};

}  // namespace noisepage::binder::cte
