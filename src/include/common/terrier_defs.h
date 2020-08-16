#pragma once

#include <cstddef>
#include <cstdint>

/* Define all typedefs here */
namespace terrier {
using byte = std::byte;
using int128_t = __int128;
using uint128_t = unsigned __int128;
using hash_t = uint64_t;

#ifndef BYTE_SIZE
#define BYTE_SIZE 8U
#endif

// Some platforms would have already defined the macro. But its presence is not standard and thus not portable. Pretty
// sure this is always 8 bits. If not, consider getting a new machine, and preferably not from another dimension :)
static_assert(BYTE_SIZE == 8U, "BYTE_SIZE should be set to 8!");

}  // namespace terrier
