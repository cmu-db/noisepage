#include "execution/util/math_util.h"

#include <cmath>

namespace terrier::execution::util {

bool MathUtil::ApproxEqual(float left, float right) {
  const auto epsilon = static_cast<float>(std::fabs(right) * 0.01);
  return std::fabs(left - right) <= epsilon;
}

bool MathUtil::ApproxEqual(double left, double right) {
  double epsilon = std::fabs(right) * 0.01;
  return std::fabs(left - right) <= epsilon;
}

}  // namespace terrier::execution::util
