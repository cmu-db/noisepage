#pragma once

#include <array>

#include "execution/sql/sql.h"
#include "execution/sql/vector.h"

namespace noisepage::execution::sql {

/**
 * A static data whose type is known at compile-time and whose vector data is directly inlined into
 * the class's memory, thus avoiding an allocation. This is appropriate for stack-allocations.
 * Upon construction, the capacity, size, and count of the vector is equal to default vector size
 * sourced from DEFAULT_VECTOR_SIZE, usually 2048 elements.
 * @tparam T The primitive type of the vector.
 */
template <typename T>
class StaticVector : public Vector {
 public:
  /**
   * Create a new empty static vector.
   * The capacity, size, and count of the vector is determined by the default vector size constant, usually 2048.
   */
  StaticVector() : Vector(execution::sql::GetTypeId<T>()) {
    // Arrange for the vector to reference the inlined data.
    Reference(reinterpret_cast<byte *>(inlined_data_.data()), nullptr, common::Constants::K_DEFAULT_VECTOR_SIZE);
  }

 private:
  // The underlying vector data.
  std::array<T, common::Constants::K_DEFAULT_VECTOR_SIZE> inlined_data_;
};

}  // namespace noisepage::execution::sql
