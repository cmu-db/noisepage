#pragma once

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
  std::vector<LexicalScope> &EnclosedScopes() { return enclosed_scopes_; }

  /** @return An immutable reference to the collection of enclosed scopes */
  const std::vector<LexicalScope> &EnclosedScopes() const { return enclosed_scopes_; }

  /** @return A mutable reference to the collection of table references in this scope */
  std::vector<ContextSensitiveTableRef> &References();

  /** @return An immutable reference to the collection of table references in this scope */
  const std::vector<ContextSensitiveTableRef> &References() const;

  /**
   * Push an enclosed scope at the back of the collection.
   * @param scope The enclosed scope
   */
  void AddEnclosedScope(const LexicalScope &scope);

  /**
   * Push an enclosed scope at the back of the collection.
   * @param scope The enclosed scope
   */
  void AddEnclosedScope(LexicalScope &&scope);

  /**
   * Push a reference at the back of the collection.
   * @param ref The table reference
   */
  void AddReference(const ContextSensitiveTableRef &ref);

  /**
   * Push a reference at the back of the collection.
   * @param ref The table reference
   */
  void AddReference(ContextSensitiveTableRef &&ref);

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
  std::vector<LexicalScope> enclosed_scopes_;

  /** The ordered collection of table references in this scope */
  std::vector<ContextSensitiveTableRef> references_;
};

}  // namespace noisepage::binder::cte
