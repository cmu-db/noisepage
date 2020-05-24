#include "util/math_util.h"

#include <cmath>

namespace tpl::util {

bool MathUtil::ApproxEqual(float left, float right) {
  const float epsilon = std::fabs(right) * 0.01;
  return std::fabs(left - right) <= epsilon;
}

bool MathUtil::ApproxEqual(double left, double right) {
  double epsilon = std::fabs(right) * 0.01;
  return std::fabs(left - right) <= epsilon;
}

}  // namespace tpl::util
