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

#include "count/empirical_data.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "count/hll_data.h"
#include "count/hll_limits.h"

using libcount::HLL_MIN_PRECISION;
using libcount::HLL_MAX_PRECISION;
using libcount::ValidTableEntries;

// Ensure that the empirical data set is sorted properly.

int main(int argc, char* argv[]) {
  // Sweep through all precision levels.
  for (int p = HLL_MIN_PRECISION; p <= HLL_MAX_PRECISION; ++p) {
    // The arrays are zero indexed; calculate the array index for the
    // associated precision level.
    const int precision_index = p - HLL_MIN_PRECISION;

    // How many valid entries will there be?
    const int kMax = 201;
    const int size = ValidTableEntries(ESTIMATE_DATA[precision_index], kMax);

    double last = 0.0;
    for (int i = 0; i < size; ++i) {
      const double curr = ESTIMATE_DATA[precision_index][i];
      // Iff we assert here, the empirical estimate data is not sorted. This
      // is a serious problem because the linear interpolation algorithm
      // assumes that the estimate values increase monotonically with their
      // array index.
      assert(curr >= last);
      if (curr < last) {
        return EXIT_FAILURE;
      }
      last = curr;
    }
  }
  return EXIT_SUCCESS;
}
