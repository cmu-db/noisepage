#pragma once

#include <vector>

namespace noisepage::binder::cte {

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
  LexicalScope(std::size_t id, std::size_t depth) : id_{id}, depth_{depth} {}

  /** @return A mutable reference to the collection of enclosed scopes */
  std::vector<LexicalScope>& EnclosedScopes() {
    return enclosed_scopes_;
  }

  /** @return An immutable reference to the collection of enclosed scopes */
  const std::vector<LexicalScope>& EnclosedScopes() const {
    return enclosed_scopes_;
  }

  /**
   * Add an enclosed scope to this scope.
   * @param scope The enclosed scope
   */
  void AddEnclosedScope(const LexicalScope& scope) {
    enclosed_scopes_.push_back(scope);
  }

  /**
   * Add an enclosed scope to this scope.
   * @param scope The enclosed scope
   */
  void AddEnclosedScope(LexicalScope&& scope) {
    enclosed_scopes_.emplace_back(std::move(scope));
  }

  /** Equality comparison with another scope instance */
  bool operator==(const LexicalScope& rhs) const {
    return id_ == rhs.id_;
  }

  /** Inequality comparison with another scope instance */
  bool operator!=(const LexicalScope& rhs) const {
    return id_ != rhs.id_;
  }

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
};

}  // namespace noisepage::binder::cte