#pragma once

#include "common/common.h"
#include "execution/sql/runtime_types.h"
#include "common/hash_util.h"

namespace terrier::execution::sql {

/**
 * Hash operation functor.
 */
template <typename T, typename Enable = void>
struct Hash {};

/**
 * Hash-with-seed functor.
 */
template <typename T, typename Enable = void>
struct HashCombine {};

/**
 * Primitive hashing: bool, 8-bit, 16-bit, 32-bit, 64-bit integers, 32-bit and 64-bit floats.
 */
template <typename T>
struct Hash<T, std::enable_if_t<std::is_arithmetic_v<T>>> {
  hash_t operator()(T input, bool null) const noexcept {
    return null ? hash_t(0) : util::HashUtil::HashCrc(input);
  }
};

/**
 * Primitive hashing with seed.
 */
template <typename T>
struct HashCombine<T, std::enable_if_t<std::is_arithmetic_v<T>>> {
  hash_t operator()(T input, bool null, const hash_t seed) const noexcept {
    return null ? hash_t(0) : util::HashUtil::HashCrc(input, seed);
  }
};

/**
 * Date hashing.
 */
template <>
struct Hash<Date> {
  hash_t operator()(Date input, bool null) const noexcept {
    return null ? hash_t(0) : input.Hash();
  }
};

/**
 * Dating hashing with seed.
 */
template <>
struct HashCombine<Date> {
  hash_t operator()(Date input, bool null, const hash_t seed) const noexcept {
    return null ? hash_t(0) : input.Hash(seed);
  }
};

/**
 * Timestamp hashing.
 */
template <>
struct Hash<Timestamp> {
  hash_t operator()(Timestamp input, bool null) const noexcept {
    return null ? hash_t(0) : input.Hash();
  }
};

/**
 * Timestamp hashing with seed.
 */
template <>
struct HashCombine<Timestamp> {
  hash_t operator()(Timestamp input, bool null, const hash_t seed) const noexcept {
    return null ? hash_t(0) : input.Hash(seed);
  }
};

/**
 * String hashing.
 */
template <>
struct Hash<VarlenEntry> {
  hash_t operator()(const VarlenEntry &input, bool null) const noexcept {
    return null ? hash_t(0) : input.Hash();
  }
};

/**
 * Varlen hashing with seed.
 */
template <>
struct HashCombine<VarlenEntry> {
  hash_t operator()(const VarlenEntry &input, bool null, const hash_t seed) const noexcept {
    return null ? hash_t(0) : input.Hash(seed);
  }
};

}  // namespace terrier::execution::sql
