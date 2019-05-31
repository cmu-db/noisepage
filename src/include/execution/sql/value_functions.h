#pragma once

#include <cmath>

#include "execution/sql/value.h"

namespace tpl::sql {

/**
 * Generic unary function on reals
 * @tparam Function the unary function
 */
template <double(Function)(double)>
struct UnaryFunctionReal {
  /**
   * Generic implementation
   * @tparam UseBranchingNullCheck whether to use a branching or branch-free implementation
   * @param src input of the function
   * @param dest destination of the result
   */
  template <bool UseBranchingNullCheck>
  static void Execute(Real *src, Real *dest) {
    if constexpr (UseBranchingNullCheck) {
      // Branching implementation
      if (!src->is_null) {
        dest->is_null = false;
        dest->val = Function(src->val);
      } else {
        *dest = Real::Null();
      }
    } else {
      // Branch-free implementation
      dest->is_null = src->is_null;
      dest->val = Function(src->val);
    }
  }
};

/**
 * Generic binary function on reals
 * @tparam Function the binary function
 */
template <double(Function)(double, double)>
struct BinaryFunctionReal {
  /**
   * Generic implementation
   * @tparam UseBranchingNullCheck whether to use a branching or branch-free implementation
   * @param arg_1 first input of the function
   * @param arg_2 second input of the function
   * @param dest destination of the result
   */
  template <bool UseBranchingNullCheck>
  static void Execute(const Real *arg_1, const Real *arg_2, Real *dest) {
    if constexpr (UseBranchingNullCheck) {
      // Branching implementation
      if (!arg_1->is_null && !arg_2->is_null) {
        dest->is_null = false;
        dest->val = Function(arg_1->val, arg_2->val);
      } else {
        *dest = Real::Null();
      }
    } else {
      // Branch-free implementation
      dest->is_null = arg_1->is_null || arg_2->is_null;
      dest->val = Function(arg_1->val, arg_2->val);
    }
  }
};

// ---------------------------------------------------------
// Implementations
// ---------------------------------------------------------

/**
 * Cotangent
 */
inline double cotan(const double arg) { return (1.0 / std::tan(arg)); }

struct ACos : public UnaryFunctionReal<std::acos> {};
struct ASin : public UnaryFunctionReal<std::asin> {};
struct ATan : public UnaryFunctionReal<std::atan> {};
struct ATan2 : public BinaryFunctionReal<std::atan2> {};
struct Cos : public UnaryFunctionReal<std::cos> {};
struct Cot : public UnaryFunctionReal<cotan> {};
struct Sin : public UnaryFunctionReal<std::sin> {};
struct Tan : public UnaryFunctionReal<std::tan> {};

}  // namespace tpl::sql
