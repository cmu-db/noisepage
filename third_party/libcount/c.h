/*
   Copyright 2015 The libcount Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. See the AUTHORS file for names of
   contributors.
*/

#ifndef INCLUDE_COUNT_C_H_
#define INCLUDE_COUNT_C_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "hll_limits.h"

/* Exported types */

typedef struct hll_t hll_t;

/* HLL Operations */

/* Create a HyperLogLog context object to estimate the cardinality of a set. */
extern hll_t* HLL_create(int precision, int* opt_error);

/* Update a context to record the observation of an element in the set. */
extern void HLL_update(hll_t* ctx, uint64_t hash);

/* Merge 'src' context with 'dest', storing the resulting state in 'dest'. */
extern int HLL_merge(hll_t* dest, const hll_t* src);

/* Return an estimate of the cardinality of the set using HyperLogLog++ */
extern uint64_t HLL_estimate(hll_t* ctx);

/* Free resources associated with a context. */
extern void HLL_free(hll_t* ctx);

#ifdef __cplusplus
} /* end extern "C" */
#endif

#endif /* INCLUDE_COUNT_C_H_ */
