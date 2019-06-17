// Copyright 2015 The libcount Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License. See the AUTHORS file for names of
// contributors.

#ifndef COUNT_UTILITY_H_
#define COUNT_UTILITY_H_

#include <math.h>
#include <stdint.h>

namespace libcount {

// Return the number of leading zero bits in the unsigned value.
static inline uint8_t CountLeadingZeroes(uint64_t x) {
  uint64_t y = 0;
  uint64_t n = 64;
  y = x >> 32;
  if (y != 0) {
    n = n - 32;
    x = y;
  }
  y = x >> 16;
  if (y != 0) {
    n = n - 16;
    x = y;
  }
  y = x >> 8;
  if (y != 0) {
    n = n - 8;
    x = y;
  }
  y = x >> 4;
  if (y != 0) {
    n = n - 4;
    x = y;
  }
  y = x >> 2;
  if (y != 0) {
    n = n - 2;
    x = y;
  }
  y = x >> 1;
  if (y != 0) {
    return static_cast<uint8_t>(n - 2);
  }
  return static_cast<uint8_t>(n - x);
}

// Equality test for doubles. Returns true if ((a - b) < epsilon).
static inline bool IsDoubleEqual(double a, double b, double epsilon) {
  return (fabs(a - b) < epsilon);
}

// If the destination pointer is valid, copy the source value to it.
// Returns true if a value was copied. Returns false otherwise.
template <typename Type>
bool MaybeAssign(Type* dest, const Type& src) {
  if (dest) {
    *dest = src;
    return true;
  }
  return false;
}

}  // namespace libcount

#endif  // COUNT_UTILITY_H_
