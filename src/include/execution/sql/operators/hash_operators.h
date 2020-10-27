#pragma once

#include "common/hash_util.h"
#include "execution/sql/runtime_types.h"

namespace noisepage::execution::sql {

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
  /** @return The hash value for the input. */
  hash_t operator()(T input, bool null) const noexcept { return null ? hash_t(0) : common::HashUtil::HashCrc(input); }
};

/**
 * Primitive hashing with seed.
 */
template <typename T>
struct HashCombine<T, std::enable_if_t<std::is_arithmetic_v<T>>> {
  /** @return The hash value for the input with the given seed. */
  hash_t operator()(T input, bool null, const hash_t seed) const noexcept {
    return null ? hash_t(0) : common::HashUtil::HashCrc(input, seed);
  }
};

/**
 * Date hashing.
 */
template <>
struct Hash<Date> {
  /** @return Hash value for the specified date. */
  hash_t operator()(Date input, bool null) const noexcept { return null ? hash_t(0) : input.Hash(); }
};

/**
 * Dating hashing with seed.
 */
template <>
struct HashCombine<Date> {
  /** @return Hash value for the specified date with the given seed. */
  hash_t operator()(Date input, bool null, const hash_t seed) const noexcept {
    return null ? hash_t(0) : input.Hash(seed);
  }
};

/**
 * Timestamp hashing.
 */
template <>
struct Hash<Timestamp> {
  /** @return Hash value for the specified timestamp. */
  hash_t operator()(Timestamp input, bool null) const noexcept { return null ? hash_t(0) : input.Hash(); }
};

/**
 * Timestamp hashing with seed.
 */
template <>
struct HashCombine<Timestamp> {
  /** @return Hash value for the specified timestamp with the given seed. */
  hash_t operator()(Timestamp input, bool null, const hash_t seed) const noexcept {
    return null ? hash_t(0) : input.Hash(seed);
  }
};

/**
 * String hashing.
 */
template <>
struct Hash<storage::VarlenEntry> {
  /** @return Hash value for the specified varlen. */
  hash_t operator()(const storage::VarlenEntry &input, bool null) const noexcept {
    return null ? hash_t(0) : input.Hash();
  }
};

/**
 * Varlen hashing with seed.
 */
template <>
struct HashCombine<storage::VarlenEntry> {
  /** @return Hash value for the specified varlen with the given seed. */
  hash_t operator()(const storage::VarlenEntry &input, bool null, const hash_t seed) const noexcept {
    return null ? hash_t(0) : input.Hash(seed);
  }
};

}  // namespace noisepage::execution::sql
