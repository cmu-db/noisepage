#pragma once

#include <functional>

#include "llvm/ADT/DenseMapInfo.h"

#include "common/macros.h"

namespace terrier::execution::ast {

/**
 * A uniqued string identifier in some AST context. This serves as a super-
 * lightweight string reference. Two identifiers are equal if they point to the
 * same string (i.e., we don't need to check contents).
 */
class Identifier {
 public:
  /**
   * Constructor
   * @param str string literal of the identifier
   */
  explicit Identifier(const char *str) noexcept : data_(str) {}

  /**
   * @return the string literal of the identifier
   */
  const char *Data() const { return data_; }

  /**
   * @return the length of the string
   */
  std::size_t Length() const {
    TERRIER_ASSERT(data_ != nullptr, "Trying to get the length of an invalid identifier");
    return std::strlen(Data());
  }

  /**
   * @return whether the string is empty
   */
  bool Empty() const { return Length() == 0; }

  /**
   * @param other other identifer
   * @return whether this == other according to pointer comparison.
   */
  bool operator==(const Identifier &other) const { return Data() == other.Data(); }

  /**
   * @param other other identifer
   * @return whether this != other according to pointer comparison.
   */
  bool operator!=(const Identifier &other) const { return !(*this == other); }

  /**
   * @return an empty key
   */
  static Identifier GetEmptyKey() {
    return Identifier(static_cast<const char *>(llvm::DenseMapInfo<const void *>::getEmptyKey()));
  }

  /**
   * @return a tombstone key
   */
  static Identifier GetTombstoneKey() {
    return Identifier(static_cast<const char *>(llvm::DenseMapInfo<const void *>::getTombstoneKey()));
  }

 private:
  const char *data_;
};

}  // namespace terrier::execution::ast

namespace llvm {

/**
 * Make Identifiers usable from LLVM DenseMaps
 */
template <>
struct DenseMapInfo<terrier::execution::ast::Identifier> {
  /**
   * @return An empty key
   */
  static inline terrier::execution::ast::Identifier getEmptyKey() {  // NOLINT
    return terrier::execution::ast::Identifier::GetEmptyKey();
  }

  /**
   * @return A tombstone key
   */
  static inline terrier::execution::ast::Identifier getTombstoneKey() {  // NOLINT
    return terrier::execution::ast::Identifier::GetTombstoneKey();
  }

  /**
   * @param identifier: Identifier to hash
   * @return the hash of the identifier
   */
  static unsigned getHashValue(const terrier::execution::ast::Identifier identifier) {  // NOLINT
    return DenseMapInfo<const void *>::getHashValue(static_cast<const void *>(identifier.Data()));
  }

  /**
   * @param lhs left hand side
   * @param rhs right hand side
   * @return whether lhs == rhs.
   */
  static bool isEqual(const terrier::execution::ast::Identifier lhs,  // NOLINT
                      const terrier::execution::ast::Identifier rhs) {
    return lhs == rhs;
  }
};

}  // namespace llvm

namespace std {

/**
 * Make Identifiers usable as keys in STL/TPL maps
 */
template <>
struct hash<terrier::execution::ast::Identifier> {
  /**
   * Hashing operator
   * @param ident identifier to hash
   * @return hash value
   */
  std::size_t operator()(const terrier::execution::ast::Identifier &ident) const noexcept {
    std::string_view s(ident.Data(), ident.Length());
    return std::hash<decltype(s)>()(s);
  }
};

}  // namespace std
