#include "common/math_util.h"

#include <cmath>

namespace terrier::common {

bool MathUtil::ApproxEqual(float left, float right) {
  const float epsilon = std::fabs(right) * 0.01;
  return std::fabs(left - right) <= epsilon;
}

bool MathUtil::ApproxEqual(double left, double right) {
  double epsilon = std::fabs(right) * 0.01;
  return std::fabs(left - right) <= epsilon;
}

}  // namespace terrier::common
