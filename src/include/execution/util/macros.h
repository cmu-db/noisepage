#pragma once

#include <cassert>

#include "common/macros.h"
#include "llvm/Support/ErrorHandling.h"

#define CACHELINE_SIZE 64

#define RESTRICT __restrict__
#define UNUSED __attribute__((unused))
#define NEVER_INLINE __attribute__((noinline))
#define FALLTHROUGH LLVM_FALLTHROUGH
#define PACKED __attribute__((packed))

#define TPL_LIKELY(x) LLVM_LIKELY(x)
#define TPL_UNLIKELY(x) LLVM_UNLIKELY(x)

#ifdef NDEBUG
#define TPL_ASSERT(expr, msg) ((void)0)
#else
#define TPL_ASSERT(expr, msg) assert((expr) && (msg))
#endif

#define UNREACHABLE(msg) llvm_unreachable(msg)
