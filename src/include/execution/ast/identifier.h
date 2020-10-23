#pragma once

#include <llvm/ADT/DenseMapInfo.h>

#include <functional>
#include <string>
#include <string_view>

#include "common/macros.h"

namespace noisepage::execution::ast {

/**
 * A uniqued string identifier in some AST context. This serves as a super-lightweight string
 * reference. Two identifiers allocated from the same AST context are equal if they have the same
 * pointer value - we don't need to check contents.
 */
class Identifier {
 private:
  friend class Context;

  // Constructor accessible only to Context which ensures uniqueness.
  explicit Identifier(const char *str) noexcept : data_(str) {}

 public:
  /**
   * Create an empty identifier.
   */
  Identifier() noexcept = default;

  /**
   * @return A const pointer to this identifier's underlying string data.
   */
  const char *GetData() const noexcept { return data_; }

  /**
   * @return The length of this identifier in bytes.
   */
  std::size_t GetLength() const {
    NOISEPAGE_ASSERT(data_ != nullptr, "Trying to get the length of an invalid identifier");
    return std::strlen(GetData());
  }

  /**
   * @return True if this identifier is empty; false otherwise.
   */
  bool IsEmpty() const noexcept { return data_ == nullptr; }

  /**
   * @return A string view over this identifier.
   */
  std::string_view GetView() const noexcept { return std::string_view(data_, GetLength()); }

  /**
   * @return A copy of this identifier's contents as a string. We assume that the identifier was
   *         properly NULL terminated.
   */
  std::string GetString() const { return data_ == nullptr ? std::string() : std::string(data_, GetLength()); }

  /**
   * Is this identifier equal to another identifier @em other.
   * @param other The identifier to compare with.
   * @return True if equal; false otherwise.
   */
  bool operator==(const Identifier &other) const noexcept { return GetData() == other.GetData(); }

  /**
   * Is this identifier not equal to another identifier @em other.
   * @param other The identifier to compare with.
   * @return True if not equal; false otherwise.
   */
  bool operator!=(const Identifier &other) const noexcept { return !(*this == other); }

  /**
   * @return An identifier that can be used to indicate an empty idenfitier.
   */
  static Identifier GetEmptyKey() {
    return Identifier(static_cast<const char *>(llvm::DenseMapInfo<const void *>::getEmptyKey()));
  }

  /**
   * @return A tombstone key that can be used to determine deleted keys in LLVM's DenseMap. This
   *         Identifier cannot equal any valid identifier that can be stored in the map.
   */
  static Identifier GetTombstoneKey() {
    return Identifier(static_cast<const char *>(llvm::DenseMapInfo<const void *>::getTombstoneKey()));
  }

 private:
  const char *data_{nullptr};
};

}  // namespace noisepage::execution::ast

namespace llvm {

/**
 * Functor to make Identifiers usable from LLVM DenseMap.
 */
template <>
struct DenseMapInfo<noisepage::execution::ast::Identifier> {
  /**
   * @return An empty key
   */
  static noisepage::execution::ast::Identifier getEmptyKey() {  // NOLINT
    return noisepage::execution::ast::Identifier::GetEmptyKey();
  }

  /**
   * @return A tombstone key
   */
  static noisepage::execution::ast::Identifier getTombstoneKey() {  // NOLINT
    return noisepage::execution::ast::Identifier::GetTombstoneKey();
  }

  /**
   * @param identifier: Identifier to hash
   * @return the hash of the identifier
   */
  static unsigned getHashValue(const noisepage::execution::ast::Identifier identifier) {  // NOLINT
    return DenseMapInfo<const void *>::getHashValue(static_cast<const void *>(identifier.GetData()));
  }

  /**
   * @param lhs left hand side
   * @param rhs right hand side
   * @return whether lhs == rhs.
   */
  static bool isEqual(const noisepage::execution::ast::Identifier lhs,  // NOLINT
                      const noisepage::execution::ast::Identifier rhs) {
    return lhs == rhs;
  }
};

}  // namespace llvm

namespace std {

/**
 * Functor to make Identifiers usable as keys in STL/TPL maps.
 */
template <>
struct hash<noisepage::execution::ast::Identifier> {
  /**
   * Hashing operator
   * @param ident identifier to hash
   * @return hash value
   */
  std::size_t operator()(const noisepage::execution::ast::Identifier &ident) const noexcept {
    std::string_view s(ident.GetData(), ident.GetLength());
    return std::hash<decltype(s)>()(s);
  }
};

}  // namespace std
