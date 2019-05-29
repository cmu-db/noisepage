/* Copyright 2015 The libcount Authors.

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

#include "count/c.h"
#include <assert.h>
#include <stdlib.h>
#include "count/hll.h"

#ifdef __cplusplus
extern "C" {
#endif

using libcount::HLL;

#include <stdint.h>

/* Exported types */

struct hll_t {
  HLL* rep;
};

/* HLL Operations */

hll_t* HLL_create(int precision, int* opt_error) {
  HLL* rep = HLL::Create(precision, opt_error);
  if (rep == NULL) {
    return NULL;
  }

  hll_t* obj = reinterpret_cast<hll_t*>(malloc(sizeof(hll_t)));
  if (obj == NULL) {
    delete rep;
    return NULL;
  }

  obj->rep = rep;
  return obj;
}

void HLL_update(hll_t* ctx, uint64_t hash) {
  assert(ctx != NULL);
  ctx->rep->Update(hash);
}

int HLL_merge(hll_t* dest, const hll_t* src) {
  assert(dest != NULL);
  assert(src != NULL);
  return dest->rep->Merge(src->rep);
}

uint64_t HLL_estimate(hll_t* ctx) {
  assert(ctx != NULL);
  return ctx->rep->Estimate();
}

void HLL_free(hll_t* ctx) {
  assert(ctx != NULL);
  assert(ctx->rep != NULL);
  delete ctx->rep;
  ctx->rep = NULL;
  free(ctx);
}

#ifdef __cplusplus
} /* end extern "C" */
#endif
