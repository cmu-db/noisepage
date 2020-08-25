#pragma once

#include <utility>

#include "execution/sql/vector.h"

namespace terrier::execution::sql {

/**
 * A constant vector is a vector with a single, constant value in it.
 */
class ConstantVector : public Vector {
 public:
  /** Create a constant vector containing the single constant value provided. */
  explicit ConstantVector(GenericValue value) : Vector(value.GetTypeId()), value_(std::move(value)) {
    Reference(&value_);
  }

 private:
  GenericValue value_;
};

}  // namespace terrier::execution::sql
