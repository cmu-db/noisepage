#pragma once
#include <memory>

namespace noisepage::common {

/**
 * A ManagedPointer points to an object whose life cycle is managed by some external entity. (e.g.
 * Catalog, SqlTable, GC) This class serves as a wrapper around the pointer to denote that the holder
 * of this pointer has no control over the life cycle of the underlying object and should not
 * attempt to delete it.
 *
 * In practice, this wrapper looks and smells like a pointer and has zero overhead. It serves as a
 * compiler hint to disallow the use of delete keyword and avoid mixing of pointers to objects
 * with different life cycles.
 *
 * @tparam Underlying the type of the object ManagedPointer points to.
 */
template <class Underlying>
class ManagedPointer {
 public:
  ManagedPointer() = default;

  /**
   * Constructs a new ManagedPointer.
   * @param ptr the pointer value this ManagedPointer wraps
   */
  explicit ManagedPointer(Underlying *ptr) : underlying_(ptr) {}

  /**
   * Constructs a new ManagedPointer.
   * @param smart_ptr the pointer value this ManagedPointer wraps
   */
  explicit ManagedPointer(const std::unique_ptr<Underlying> &smart_ptr) : underlying_(smart_ptr.get()) {}

  /**
   * @param null_ptr null pointer
   */
  ManagedPointer(std::nullptr_t null_ptr) noexcept : underlying_(nullptr) {}  // NOLINT

  /**
   * @return the underlying pointer
   */
  Underlying &operator*() const { return *underlying_; }

  /**
   * @return the underlying pointer
   */
  Underlying *operator->() const { return underlying_; }

  /**
   * @return the underlying pointer
   */
  Underlying *Get() const { return underlying_; }

  /**
   * @return True if it is not a nullptr, false otherwise
   */
  explicit operator bool() const { return underlying_; }

  /**
   * Overloaded assignment operator for nullptr type
   * @return reference to this
   */
  ManagedPointer &operator=(std::nullptr_t) {
    underlying_ = nullptr;
    return *this;
  }

  /**
   * Overloaded assignment operator for unique_ptr type
   * @return reference to this
   */
  ManagedPointer &operator=(const std::unique_ptr<Underlying> &smart_ptr) {
    underlying_ = smart_ptr.get();
    return *this;
  }

  /**
   * Overloaded assignment operator for underlying ptr type
   * @return reference to this
   */
  ManagedPointer &operator=(Underlying *const ptr) {
    underlying_ = ptr;
    return *this;
  }

  /**
   * Equality operator
   * @param other the other ManagedPointer to be compared with
   * @return true if the two ManagedPointers are equal, false otherwise.
   */
  bool operator==(const ManagedPointer &other) const { return underlying_ == other.underlying_; }

  /**
   * Convenience operator that is semantically equal to *this == ManagedPointer<Underlying>(other)
   * @param other the other pointer to be compared with
   * @return true if the underlying pointer is equal to the given pointer, false otherwise.
   */
  bool operator==(Underlying *other) const { return underlying_ == other; }

  /**
   * Inequality operator
   * @param other the other ManagedPointer to be compared with
   * @return true if the two ManagedPointers are not equal, false otherwise.
   */
  bool operator!=(const ManagedPointer &other) const { return underlying_ != other.underlying_; }

  /**
   * Convenience operator that is semantically equal to *this != ManagedPointer<Underlying>(other)
   * @param other the other pointer to be compared with
   * @return true if the underlying pointer is not equal to the given pointer, false otherwise.
   */
  bool operator!=(Underlying *other) const { return underlying_ != other; }

  /**
   * Outputs the ManagedPointer to the output stream.
   * @param os output stream to be written to.
   * @param pointer The ManagedPointer to be output.
   * @return modified output stream.
   */
  friend std::ostream &operator<<(std::ostream &os, const ManagedPointer &pointer) { return os << pointer.underlying_; }

  /**
   * Performs a reinterpret cast on the underlying pointer of a ManagedPointer to a different type
   * @tparam NewType type to cast to. Underlying type must be reinterpretable to new type
   * @return ManagedPointer holding the new type
   */
  template <class NewType>
  ManagedPointer<NewType> CastManagedPointerTo() const {
    return ManagedPointer<NewType>(reinterpret_cast<NewType *>(underlying_));
  }

 private:
  Underlying *underlying_;
};
}  // namespace noisepage::common

namespace std {
/**
 * Implements std::hash for ManagedPointer.
 * @tparam Underlying the type of the object ManagedPointer points to.
 */
template <class Underlying>
struct hash<noisepage::common::ManagedPointer<Underlying>> {
  /**
   * @param ptr the ManagedPointer to be hashed.
   * @return the hash of the ManagedPointer.
   */
  size_t operator()(const noisepage::common::ManagedPointer<Underlying> &ptr) const {
    return hash<Underlying *>()(ptr.operator->());
  }
};
}  // namespace std
