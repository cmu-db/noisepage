#pragma once

#include <cassert>

#include "llvm/Support/ErrorHandling.h"

#define CACHELINE_SIZE 64

#define RESTRICT __restrict__
#define UNUSED __attribute__((unused))
#ifndef ALWAYS_INLINE
#define ALWAYS_INLINE __attribute__((always_inline))
#endif
#ifndef NEVER_INLINE
#define NEVER_INLINE __attribute__((noinline))
#endif
#define FALLTHROUGH LLVM_FALLTHROUGH
#define PACKED __attribute__((packed))

#ifndef DISALLOW_COPY
#define DISALLOW_COPY(klazz)     \
  klazz(const klazz &) = delete; \
  klazz &operator=(const klazz &) = delete;

#define DISALLOW_MOVE(klazz) \
  klazz(klazz &&) = delete;  \
  klazz &operator=(klazz &&) = delete;

#define DISALLOW_COPY_AND_MOVE(klazz) \
  DISALLOW_COPY(klazz)                \
  DISALLOW_MOVE(klazz)
#endif

#define TPL_LIKELY(x) LLVM_LIKELY(x)
#define TPL_UNLIKELY(x) LLVM_UNLIKELY(x)

#ifdef NDEBUG
#define TPL_ASSERT(expr, msg) ((void)0)
#else
#define TPL_ASSERT(expr, msg) assert((expr) && (msg))
#endif

#define UNREACHABLE(msg) llvm_unreachable(msg)
