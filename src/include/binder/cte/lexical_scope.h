#pragma once

#include <memory>
#include <vector>

namespace noisepage::binder::cte {

enum class RefType;
class ContextSensitiveTableRef;

/**
 * The LexicalScope class is the fundamental unit of organization
 * used when constructing a StructuredStatement. Each LexicalScope
 * instance represents (as the name implies) a unique lexical scope
 * in the statement from which it is derived.
 */
class LexicalScope {
 public:
  /**
   * Construct a new LexicalScope instance.
   * @param id The unique identifier for the scope
   * @param depth The depth of the scope in the statement
   * relative to the root scope
   * @param enclosing_scope A pointer to the enclosing scope
   */
  LexicalScope(std::size_t id, std::size_t depth, const LexicalScope *enclosing_scope);

  /** @return The unique identifier for this scope */
  std::size_t Id() const { return id_; }

  /** @return The depth of this scope in the statement */
  std::size_t Depth() const { return depth_; }

  /** @return The enclosing scope for this scope */
  const LexicalScope *EnclosingScope() const { return enclosing_scope_; }

  /** @return `true` if this scope has an enclosing scope, `false` otherwise */
  bool HasEnclosingScope() const { return enclosing_scope_ != nullptr; }

  /** @return The number of references contained in this scope */
  std::size_t RefCount() const;

  /** @return The number of read references contained in this scope */
  std::size_t ReadRefCount() const;

  /** @return The number of write references contained in this scope */
  std::size_t WriteRefCount() const;

  /** @return A mutable reference to the collection of enclosed scopes */
  std::vector<std::unique_ptr<LexicalScope>> &EnclosedScopes() { return enclosed_scopes_; }

  /** @return An immutable reference to the collection of enclosed scopes */
  const std::vector<std::unique_ptr<LexicalScope>> &EnclosedScopes() const { return enclosed_scopes_; }

  /** @return A mutable reference to the collection of table references in this scope */
  std::vector<std::unique_ptr<ContextSensitiveTableRef>> &References();

  /** @return An immutable reference to the collection of table references in this scope */
  const std::vector<std::unique_ptr<ContextSensitiveTableRef>> &References() const;

  /**
   * Push an enclosed scope at the back of the collection.
   * @param scope The enclosed scope
   */
  void AddEnclosedScope(std::unique_ptr<LexicalScope> &&scope);

  /**
   * Push a reference at the back of the collection.
   * @param ref The table reference
   */
  void AddReference(std::unique_ptr<ContextSensitiveTableRef> &&ref);

  /**
   * Determine the position of the table reference identified by `alias` in this scope.
   * @pre A table reference identified by `alias` is present in the scope
   * @param alias The alias of the table reference for which to search
   * @param type The type of the table reference for which to search
   * @return The position of the table reference within the scope
   */
  std::size_t PositionOf(std::string_view alias, RefType type) const;

  /** Equality comparison with another scope instance */
  bool operator==(const LexicalScope &rhs) const { return id_ == rhs.id_; }

  /** Inequality comparison with another scope instance */
  bool operator!=(const LexicalScope &rhs) const { return id_ != rhs.id_; }

 private:
  /**
   * Compute the number of references in this scope with the given type.
   * @param type The reference type
   * @return The number of references of the specified type
   */
  std::size_t RefCountWithType(RefType type) const;

 public:
  /** The enclosing scope for the root scope */
  static constexpr const LexicalScope *GLOBAL_SCOPE{nullptr};

 private:
  /** The unique identifier for the scope */
  std::size_t id_;

  /** The depth of the scope, relative to the root scope */
  std::size_t depth_;

  /** A pointer to the scope that encloses this scope */
  const LexicalScope *enclosing_scope_;

  /**
   * The ordered collection of enclosed scopes.
   *
   * NOTE: The ordering of scopes within an enclosing scope
   * is an important property that we must respect. Later,
   * when we are validating the correctness of references
   * within the statement, we rely on this ordering to perform
   * a depth-first traversal of the "scope tree" that allows
   * us to identify forward references.
   */
  std::vector<std::unique_ptr<LexicalScope>> enclosed_scopes_;

  /** The ordered collection of table references in this scope */
  std::vector<std::unique_ptr<ContextSensitiveTableRef>> references_;
};

}  // namespace noisepage::binder::cte
