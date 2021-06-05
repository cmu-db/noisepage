#pragma once

#include <vector>

namespace noisepage::binder::cte {

class TypedTableRef;

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
   * @param depth The depth of the scope in the statement,
   * relative to the root scope
   */
  LexicalScope(std::size_t id, std::size_t depth);

  /** @return The depth of this scope in the statement */
  std::size_t Depth() const { return depth_; }

  /** @return A mutable reference to the collection of enclosed scopes */
  std::vector<LexicalScope> &EnclosedScopes() { return enclosed_scopes_; }

  /** @return An immutable reference to the collection of enclosed scopes */
  const std::vector<LexicalScope> &EnclosedScopes() const { return enclosed_scopes_; }

  /** @return A mutable reference to the collection of table references in this scope */
  std::vector<TypedTableRef> &References();

  /** @return An immutable reference to the collection of table references in this scope */
  const std::vector<TypedTableRef> &References() const;

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
  void AddReference(const TypedTableRef &ref);

  /**
   * Push a reference at the back of the collection.
   * @param ref The table reference
   */
  void AddReference(TypedTableRef &&ref);

  /** Equality comparison with another scope instance */
  bool operator==(const LexicalScope &rhs) const { return id_ == rhs.id_; }

  /** Inequality comparison with another scope instance */
  bool operator!=(const LexicalScope &rhs) const { return id_ != rhs.id_; }

 private:
  /** The unique identifier for the scope */
  std::size_t id_;

  /** The depth of the scope, relative to the root scope */
  std::size_t depth_;

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
  std::vector<TypedTableRef> references_;
};

}  // namespace noisepage::binder::cte
