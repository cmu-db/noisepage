#pragma once

// TODO(WAN): charconv discussion
// #include <charconv>
#include <limits>
#include <string>

#include "common/error/exception.h"
#include "execution/sql/runtime_types.h"
#include "execution/sql/sql.h"
#include "spdlog/fmt/fmt.h"
#include "storage/storage_defs.h"

namespace terrier::execution::sql {

//===----------------------------------------------------------------------===//
//
// Checked Cast.
//
//===----------------------------------------------------------------------===//

/**
 * TryCast is a function object implementing a "safe" cast. On invocation, it will attempt to cast
 * its single input with type @em InType into a value of type @em OutType. If the cast operation is
 * valid, it returns true and the output result parameter is set to the casted value. If the cast
 * produces a value NOT in the valid range for the output type, the cast return false. The output
 * parameter is in an invalid state.
 * @tparam InType The CPP type of the input.
 * @tparam OutType The CPP type the input type into.
 */
template <typename InType, typename OutType, typename Enable = void>
struct EXPORT TryCast {};

/**
 * Cast is a function object implementing a "checking" cast. On invocation, it attempts to cast its
 * input with type @em InType into a value of type @em OutType. If valid, the resulting value is
 * returned. If the cast is invalid, a ValueOutOfRangeException exception is thrown,
 * @tparam InType The CPP type of the input.
 * @tparam OutType The CPP type the input type into.
 */
template <typename InType, typename OutType>
struct EXPORT Cast {
  /** @return On a valid cast, the casted output value. Otherwise, throws an execution exception. */
  OutType operator()(InType input) const {
    OutType result;
    if (!TryCast<InType, OutType>{}(input, &result)) {
      throw EXECUTION_EXCEPTION(
          fmt::format("Type {} cannot be cast because the value is out of range for the target type {}.",
                      TypeIdToString(GetTypeId<InType>()), TypeIdToString(GetTypeId<OutType>())),
          common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    return result;  // NOLINT
  }
};

//===----------------------------------------------------------------------===//
//
// Trivial Cast
//
//===----------------------------------------------------------------------===//

/** True if the cast from input type to output type succeeds. */
template <typename T>
struct EXPORT TryCast<T, T> {
  /** @return True if the cast was successful. */
  bool operator()(const T input, T *output) const noexcept {
    *output = input;
    return true;
  }
};

namespace detail {

// Is the given type an integer type? Note: we cannot use std::is_integral<>
// because it includes the primitive bool type. We want to distinguish only
// primitive integer types.
template <typename T>
struct EXPORT IsIntegerType : std::integral_constant<bool, std::is_integral_v<T> && !std::is_same_v<bool, T>> {};

template <typename T>
constexpr bool IS_INTEGER_TYPE_V = IsIntegerType<T>::value;

// Is the given template type a floating point type?
template <typename T>
struct EXPORT IsFloatingType : std::integral_constant<bool, std::is_floating_point_v<T>> {};

template <typename T>
constexpr bool IS_FLOATING_TYPE_V = IsFloatingType<T>::value;

// Is the given type either an integer type or a floating point type?
template <typename T>
struct EXPORT IsNumberType : std::integral_constant<bool, IS_INTEGER_TYPE_V<T> || std::is_floating_point_v<T>> {};

template <typename T>
constexpr bool IS_NUMBER_TYPE_V = IsNumberType<T>::value;

/** Is the cast from the given input and output types a downward cast? */
template <typename InType, typename OutType>
struct EXPORT IsNumberDowncast {
  /** Is the cast from the given input and output types a downward cast? */
  static constexpr bool VALUE =
      // Both types are numbers.
      IS_NUMBER_TYPE_V<InType> && IS_NUMBER_TYPE_V<OutType> &&
      // Both have the same signed-ness.
      std::is_signed_v<InType> == std::is_signed_v<OutType> &&
      // Both have the same integer-ness.
      std::is_floating_point_v<InType> == std::is_floating_point_v<OutType> &&
      // The output type has a smaller domain the input. We use storage size to determine this.
      sizeof(OutType) < sizeof(InType);
};

template <typename InType, typename OutType>
constexpr bool IS_NUMBER_DOWNCAST_V = IsNumberDowncast<InType, OutType>::VALUE;

/** True if it is a cast from a integral signed to integral unsigned. */
template <typename InType, typename OutType>
struct EXPORT IsIntegralSignedToUnsigned {
  /** True if it is a cast from a integral signed to integral unsigned. */
  static constexpr bool VALUE =
      // Both types are integers (non-bool and non-float).
      IS_INTEGER_TYPE_V<InType> && IS_INTEGER_TYPE_V<OutType> &&
      // The input is signed and output is unsigned.
      std::is_signed_v<InType> && std::is_unsigned_v<OutType>;
};

template <typename InType, typename OutType>
constexpr bool IS_INTEGRAL_SIGNED_TO_UNSIGNED_V = IsIntegralSignedToUnsigned<InType, OutType>::VALUE;

/** True if it is a cast from integral unsigned to integral signed. */
template <typename InType, typename OutType>
struct EXPORT IsIntegralUnsignedToSigned {
  /** True if it is a cast from integral unsigned to integral signed. */
  static constexpr bool VALUE = IS_INTEGER_TYPE_V<InType> && IS_INTEGER_TYPE_V<OutType> && std::is_unsigned_v<InType> &&
                                std::is_signed_v<OutType>;
};

template <typename InType, typename OutType>
constexpr bool IS_INTEGRAL_UNSIGNED_TO_SIGNED_V = IsIntegralUnsignedToSigned<InType, OutType>::VALUE;

/** True if it is a safe numeric cast. */
template <typename InType, typename OutType>
struct EXPORT IsSafeNumericCast {
  /** True if it is a safe numeric cast. */
  static constexpr bool VALUE =
      /// @cond DOXYGEN_IGNORE Doxygen gets confused and wants to document OutType as a variable.
      // Both inputs are numbers.
      IS_NUMBER_TYPE_V<InType> && IS_NUMBER_TYPE_V<OutType> &&
      // Both have the same signed-ness.
      std::is_signed_v<InType> == std::is_signed_v<OutType> &&
      // Both have the same integer-ness.
      std::is_integral_v<InType> == std::is_integral_v<OutType> &&
      // The output type has a larger domain then input. We use storage size to determine this.
      sizeof(OutType) >= sizeof(InType) &&
      // They're different types.
      !std::is_same_v<InType, OutType>;
  /// @endcond
};

template <typename InType, typename OutType>
constexpr bool IS_SAFE_NUMERIC_CAST_V = IsSafeNumericCast<InType, OutType>::VALUE;

/** True if it is a float truncation. */
template <typename InType, typename OutType>
struct EXPORT IsFloatTruncate {
  /** True if it is a float truncation. */
  static constexpr bool VALUE =
      // The input is an integer and the output is a float.
      (IS_INTEGER_TYPE_V<InType> && IS_FLOATING_TYPE_V<OutType>) ||
      // Or, the input is float and output is an integer.
      (IS_FLOATING_TYPE_V<InType> && IS_INTEGER_TYPE_V<OutType>);
};

template <typename InType, typename OutType>
constexpr bool IS_FLOAT_TRUNCATE_V = IsFloatTruncate<InType, OutType>::VALUE;

}  // namespace detail

//===----------------------------------------------------------------------===//
//
// Numeric -> Boolean
//
//===----------------------------------------------------------------------===//

/**
 * Cast a numeric value into a boolean.
 * @tparam InType The numeric input type.
 */
template <typename InType>
struct EXPORT
    TryCast<InType, bool, std::enable_if_t<detail::IS_NUMBER_TYPE_V<InType> && !std::is_same_v<InType, bool>>> {
  /** @return True if the cast was successful. */
  bool operator()(const InType input, bool *output) noexcept {
    *output = static_cast<bool>(input);
    return true;
  }
};

//===----------------------------------------------------------------------===//
//
// Boolean -> Numeric
//
//===----------------------------------------------------------------------===//

/**
 * Cast a boolean into a numeric value.
 * @tparam OutType The numeric output type.
 */
template <typename OutType>
struct EXPORT TryCast<bool, OutType, std::enable_if_t<detail::IS_NUMBER_TYPE_V<OutType>>> {
  /** @return True if the cast was successful. */
  bool operator()(const bool input, OutType *output) const noexcept {
    *output = static_cast<OutType>(input);
    return true;
  }
};

//===----------------------------------------------------------------------===//
//
// Numeric cast.
//
// These casts are grouped into four categories:
// 1. Down-cast.
// 2. Signed-to-unsigned cast.
// 3. Unsigned-to-signed cast.
// 4. Floating point truncation.
//
//===----------------------------------------------------------------------===//

/**
 * Numeric down-cast, signed-to-unsigned, unsigned-to-signed, or float truncation cast.
 * @tparam InType The numeric input type. Must satisfy internal::is_number_type_v<InType>.
 * @tparam OutType The numeric output type.  Must satisfy internal::is_number_type_v<OutType>.
 */
template <typename InType, typename OutType>
struct EXPORT TryCast<
    InType, OutType,
    std::enable_if_t<
        detail::IS_NUMBER_DOWNCAST_V<InType, OutType> || detail::IS_INTEGRAL_SIGNED_TO_UNSIGNED_V<InType, OutType> ||
        detail::IS_INTEGRAL_UNSIGNED_TO_SIGNED_V<InType, OutType> || detail::IS_FLOAT_TRUNCATE_V<InType, OutType>>> {
  /** @return True if the cast was successful. */
  bool operator()(const InType input, OutType *output) const noexcept {
    constexpr OutType k_min = std::numeric_limits<OutType>::lowest();
    constexpr OutType k_max = std::numeric_limits<OutType>::max();

    *output = static_cast<OutType>(input);

    // Fixes this hideously obscure bug: https://godbolt.org/z/M14jdb
    if constexpr (std::numeric_limits<OutType>::is_integer && !std::numeric_limits<InType>::is_integer) {  // NOLINT
      return k_min <= input && static_cast<OutType>(input) < k_max;
    } else {  // NOLINT
      return k_min <= input && input <= k_max;
    }
  }
};

/**
 * Safe numeric up-cast, i.e., a regular cast.
 * @tparam InType The numeric input type. Must satisfy internal::is_number_type_v<InType>.
 * @tparam OutType The numeric output type.  Must satisfy internal::is_number_type_v<OutType>.
 */
template <typename InType, typename OutType>
struct EXPORT TryCast<InType, OutType,
                      std::enable_if_t<detail::IS_SAFE_NUMERIC_CAST_V<InType, OutType> &&
                                       !detail::IS_NUMBER_DOWNCAST_V<InType, OutType>>> {
  /** @return True if the cast was successful. */
  bool operator()(const InType input, OutType *output) const noexcept {
    *output = static_cast<OutType>(input);
    return true;
  }
};

/**
 * Numeric value to Date. This isn't a real cast, but let's support it for now.
 * @tparam InType The numeric input type. Must satisfy internal::is_number_type_v<InType>
 */
template <typename InType>
struct EXPORT TryCast<InType, Date, std::enable_if_t<detail::IS_INTEGER_TYPE_V<InType>>> {
  /** @return True if the cast was successful. */
  bool operator()(const InType input, Date *output) const noexcept {
    *output = Date(input);
    return true;
  }
};

/**
 * Boolean or numeric value to string.
 * @tparam InType The input type. Either a number or a boolean.
 */
template <typename InType>
struct EXPORT
    TryCast<InType, std::string, std::enable_if_t<detail::IS_NUMBER_TYPE_V<InType> || std::is_same_v<InType, bool>>> {
  /** @return True if the cast was successful. */
  bool operator()(const InType input, std::string *output) const noexcept {
    *output = std::to_string(input);
    return true;
  }
};

//===----------------------------------------------------------------------===//
//
// Date or Timestamp to string
//
//===----------------------------------------------------------------------===//

/**
 * Date or Timestamp to string conversion.
 * @tparam InType The type of the input, either a Date or Timestamp.
 */
template <typename InType>
struct EXPORT
    TryCast<InType, std::string, std::enable_if_t<std::is_same_v<InType, Date> || std::is_same_v<InType, Timestamp>>> {
  /** @return True if the cast was successful. */
  bool operator()(const InType input, std::string *output) const noexcept {
    *output = input.ToString();
    return true;
  }
};

/**
 * Date to Timestamp conversion.
 */
template <>
struct EXPORT TryCast<Date, Timestamp> {
  /** @return True if the cast was successful. */
  bool operator()(const Date input, Timestamp *output) const noexcept {
    *output = input.ConvertToTimestamp();
    return true;
  }
};

/**
 * Timestamp to Date conversion.
 */
template <>
struct EXPORT TryCast<Timestamp, Date> {
  /** @return True if the cast was successful. */
  bool operator()(const Timestamp input, Date *output) const noexcept {
    *output = input.ConvertToDate();
    return true;
  }
};

//===----------------------------------------------------------------------===//
//
// String Cast.
//
//===----------------------------------------------------------------------===//

/**
 * String to boolean.
 * @tparam OutType The input type. Either a number or a boolean.
 */
template <>
struct EXPORT TryCast<storage::VarlenEntry, bool> {
  /** @return True if the cast was successful. */
  bool operator()(const storage::VarlenEntry &input, bool *output) const {
    const auto buf = reinterpret_cast<const char *>(input.Content());
    if (input.Size() == 0) {
      return false;
    }
    if (buf[0] == 't' || buf[0] == 'T') {
      *output = true;
    } else if (buf[0] == 'f' || buf[0] == 'F') {
      *output = false;
    } else {
      return false;
    }
    return true;
  }
};

/**
 * String to integer.
 * @tparam OutType The input type. Either a number or a boolean.
 */
template <typename OutType>
struct EXPORT TryCast<storage::VarlenEntry, OutType, std::enable_if_t<detail::IS_INTEGER_TYPE_V<OutType>>> {
  /** @return True if the cast was successful. */
  bool operator()(const storage::VarlenEntry &input, OutType *output) const {
    *output = std::stol(std::string(input.StringView()));
    return true;
    // TODO(WAN): charconv support
#if 0
    const auto buf = reinterpret_cast<const char *>(input.Content());
    const auto len = input.Size();
    const auto [p, ec] = std::from_chars(buf, buf + len, *output);
    return ec == std::errc();
#endif
  }
};

/**
 * String to single-precision float.
 */
template <>
struct EXPORT TryCast<storage::VarlenEntry, float> {
  /** @return True if the cast was successful. */
  bool operator()(const storage::VarlenEntry &input, float *output) const;
};

/**
 * String to double-precision float.
 */
template <>
struct EXPORT TryCast<storage::VarlenEntry, double> {
  /** @return True if the cast was successful. */
  bool operator()(const storage::VarlenEntry &input, double *output) const;
};

/**
 * String to date.
 */
template <>
struct EXPORT TryCast<storage::VarlenEntry, Date> {
  /** @return True if the cast was successful. */
  bool operator()(const storage::VarlenEntry &input, Date *output) const {
    *output = Date::FromString(input.StringView().data());
    return true;
  }
};

/**
 * String to timestamp.
 */
template <>
struct EXPORT TryCast<storage::VarlenEntry, Timestamp> {
  /** @return True if the cast was successful. */
  bool operator()(const storage::VarlenEntry &input, Timestamp *output) const {
    try {
      *output = Timestamp::FromString(input.StringView());
      return true;
    } catch (const ConversionException &e) {
      return false;
    }
  }
};

}  // namespace terrier::execution::sql
