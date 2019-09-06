// Copyright (c) 2012, Susumu Yata
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
//    this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.

#ifndef MADOKA_UTIL_H
#define MADOKA_UTIL_H

#ifdef __cplusplus
 #include <cstddef>
#else  // __cplusplus
 #include <stddef.h>
#endif  // __cplusplus

#ifdef _MSC_VER
 #ifdef __cplusplus
  #include <intrin.h>
  #ifdef _WIN64
   #pragma intrinsic(_BitScanReverse64)
  #else  // _WIN64
   #pragma intrinsic(_BitScanReverse)
  #endif  // _WIN64
 #endif  // __cplusplus
#else  // _MSC_VER
 #include <stdint.h>
#endif  // _MSC_VER

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

#ifdef _MSC_VER
typedef unsigned __int8  madoka_uint8;
typedef unsigned __int16 madoka_uint16;
typedef unsigned __int32 madoka_uint32;
typedef unsigned __int64 madoka_uint64;
#else  // _MSC_VER
typedef uint8_t  madoka_uint8;
typedef uint16_t madoka_uint16;
typedef uint32_t madoka_uint32;
typedef uint64_t madoka_uint64;
#endif  // _MSC_VER

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#ifdef __cplusplus
namespace madoka {

typedef ::madoka_uint8  UInt8;
typedef ::madoka_uint16 UInt16;
typedef ::madoka_uint32 UInt32;
typedef ::madoka_uint64 UInt64;

namespace util {

template <typename T>
inline void swap(T &lhs, T &rhs) noexcept {
  const T temp = lhs;
  lhs = rhs;
  rhs = temp;
}

// bit_scan_reverse() returns the index of the most significant 1 bit of
// `value'. For example, if `value' == 12, the result is 3. Note that if
// `value' == 0, the result is undefined.
inline UInt64 bit_scan_reverse(UInt64 value) noexcept {
#ifdef _MSC_VER
  unsigned long index;
 #ifdef _WIN64
  ::_BitScanReverse64(&index, value);
  return index;
 #else  // _WIN64
  if ((value >> 32) != 0) {
    ::_BitScanReverse(&index, static_cast<unsigned long>(value >> 32));
    return index + 32;
  }
  ::_BitScanReverse(&index, static_cast<unsigned long>(value));
  return index;
 #endif  // _WIN64
#else  // _MSC_VER
 #ifdef __x86_64__
  UInt64 index;
  __asm__ ("bsrq %1, %%rax; movq %%rax, %0"
    : "=r"(index) : "r"(value) : "%rax");
  return index;
 #else  // __x86_64__
  #ifdef __i386__
   UInt32 index;
   if ((value >> 32) != 0) {
     __asm__ ("bsrl %1, %%eax; movl %%eax, %0"
       : "=r"(index) : "r"(static_cast<UInt32>(value >> 32)) : "%eax");
     return index + 32;
   }
   __asm__ ("bsrl %1, %%eax; movl %%eax, %0"
     : "=r"(index) : "r"(static_cast<UInt32>(value)) : "%eax");
   return index;
  #else  // __i386__
   return 63 - ::__builtin_clzll(value);
  #endif  // __i386__
 #endif  // __x86_64__
#endif  // _MSC_VER
}

}  // namespace util
}  // namespace madoka
#endif  // __cplusplus

#endif  // MADOKA_UTIL_H
