#pragma once

#include <charconv>
#include <string>

#include "execution/util/exception.h"

#include "execution/sql/sql.h"
#include "execution/sql/runtime_types.h"
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
struct TryCast {};

/**
 * Cast is a function object implementing a "checking" cast. On invocation, it attempts to cast its
 * input with type @em InType into a value of type @em OutType. If valid, the resulting value is
 * returned. If the cast is invalid, a ValueOutOfRangeException exception is thrown,
 * @tparam InType The CPP type of the input.
 * @tparam OutType The CPP type the input type into.
 */
template <typename InType, typename OutType>
struct Cast {
  OutType operator()(InType input) const {
    OutType result;
    if (!TryCast<InType, OutType>{}(input, &result)) {
      throw ValueOutOfRangeException(GetTypeId<InType>(), GetTypeId<OutType>());
    }
    return result;
  }
};

//===----------------------------------------------------------------------===//
//
// Trivial Cast
//
//===----------------------------------------------------------------------===//

template <typename T>
struct TryCast<T, T> {
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
struct is_integer_type
    : std::integral_constant<bool, std::is_integral_v<T> && !std::is_same_v<bool, T>> {};

template <typename T>
constexpr bool is_integer_type_v = is_integer_type<T>::value;

// Is the given template type a floating point type?
template <typename T>
struct is_floating_type : std::integral_constant<bool, std::is_floating_point_v<T>> {};

template <typename T>
constexpr bool is_floating_type_v = is_floating_type<T>::value;

// Is the given type either an integer type or a floating point type?
template <typename T>
struct is_number_type
    : std::integral_constant<bool, is_integer_type_v<T> || std::is_floating_point_v<T>> {};

template <typename T>
constexpr bool is_number_type_v = is_number_type<T>::value;

// Is the cast from the given input and output types a downward cast?
template <typename InType, typename OutType>
struct is_number_downcast {
  static constexpr bool value =
      // Both types are numbers.
      is_number_type_v<InType> && is_number_type_v<OutType> &&
      // Both have the same signed-ness.
      std::is_signed_v<InType> == std::is_signed_v<OutType> &&
      // Both have the same integer-ness.
      std::is_floating_point_v<InType> == std::is_floating_point_v<OutType> &&
      // The output type has a smaller domain the input. We use storage size to determine this.
      sizeof(OutType) < sizeof(InType);
};

template <typename InType, typename OutType>
constexpr bool is_number_downcast_v = is_number_downcast<InType, OutType>::value;

// Is the cast from the given input type to the output type a cast from a signed
// to an unsigned integer type?
template <typename InType, typename OutType>
struct is_integral_signed_to_unsigned {
  static constexpr bool value =
      // Both types are integers (non-bool and non-float).
      is_integer_type_v<InType> && is_integer_type_v<OutType> &&
      // The input is signed and output is unsigned.
      std::is_signed_v<InType> && std::is_unsigned_v<OutType>;
};

template <typename InType, typename OutType>
constexpr bool is_integral_signed_to_unsigned_v =
    is_integral_signed_to_unsigned<InType, OutType>::value;

template <typename InType, typename OutType>
struct is_integral_unsigned_to_signed {
  static constexpr bool value = is_integer_type_v<InType> && is_integer_type_v<OutType> &&
                                std::is_unsigned_v<InType> && std::is_signed_v<OutType>;
};

template <typename InType, typename OutType>
constexpr bool is_integral_unsigned_to_signed_v =
    is_integral_unsigned_to_signed<InType, OutType>::value;

template <typename InType, typename OutType>
struct is_safe_numeric_cast {
  static constexpr bool value =
      // Both inputs are numbers.
      is_number_type_v<InType> && is_number_type_v<OutType> &&
      // Both have the same signed-ness.
      std::is_signed_v<InType> == std::is_signed_v<OutType> &&
      // Both have the same integer-ness.
      std::is_integral_v<InType> == std::is_integral_v<OutType> &&
      // The output type has a larger domain then input. We use storage size to determine this.
      sizeof(OutType) >= sizeof(InType) &&
      // They're different types.
      !std::is_same_v<InType, OutType>;
};

template <typename InType, typename OutType>
constexpr bool is_safe_numeric_cast_v = is_safe_numeric_cast<InType, OutType>::value;

template <typename InType, typename OutType>
struct is_float_truncate {
  static constexpr bool value =
      // The input is an integer and the output is a float.
      (is_integer_type_v<InType> && is_floating_type_v<OutType>) ||
      // Or, the input is float and output is an integer.
      (is_floating_type_v<InType> && is_integer_type_v<OutType>);
};

template <typename InType, typename OutType>
constexpr bool is_float_truncate_v = is_float_truncate<InType, OutType>::value;

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
struct TryCast<
    InType, bool,
    std::enable_if_t<detail::is_number_type_v<InType> && !std::is_same_v<InType, bool>>> {
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
struct TryCast<bool, OutType, std::enable_if_t<detail::is_number_type_v<OutType>>> {
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
struct TryCast<InType, OutType,
               std::enable_if_t<detail::is_number_downcast_v<InType, OutType> ||
                                detail::is_integral_signed_to_unsigned_v<InType, OutType> ||
                                detail::is_integral_unsigned_to_signed_v<InType, OutType> ||
                                detail::is_float_truncate_v<InType, OutType>>> {
  bool operator()(const InType input, OutType *output) const noexcept {
    constexpr OutType kMin = std::numeric_limits<OutType>::lowest();
    constexpr OutType kMax = std::numeric_limits<OutType>::max();

    *output = static_cast<OutType>(input);
    return input >= kMin && input <= kMax;
  }
};

/**
 * Safe numeric up-cast, i.e., a regular cast.
 * @tparam InType The numeric input type. Must satisfy internal::is_number_type_v<InType>.
 * @tparam OutType The numeric output type.  Must satisfy internal::is_number_type_v<OutType>.
 */
template <typename InType, typename OutType>
struct TryCast<InType, OutType,
               std::enable_if_t<detail::is_safe_numeric_cast_v<InType, OutType> &&
                                !detail::is_number_downcast_v<InType, OutType>>> {
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
struct TryCast<InType, Date, std::enable_if_t<detail::is_integer_type_v<InType>>> {
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
struct TryCast<InType, std::string,
               std::enable_if_t<detail::is_number_type_v<InType> || std::is_same_v<InType, bool>>> {
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
struct TryCast<
    InType, std::string,
    std::enable_if_t<std::is_same_v<InType, Date> || std::is_same_v<InType, Timestamp>>> {
  bool operator()(const InType input, std::string *output) const noexcept {
    *output = input.ToString();
    return true;
  }
};

/**
 * Date to Timestamp conversion.
 */
template <>
struct TryCast<Date, Timestamp> {
  bool operator()(const Date input, Timestamp *output) const noexcept {
    *output = input.ConvertToTimestamp();
    return true;
  }
};

/**
 * Timestamp to Date conversion.
 */
template <>
struct TryCast<Timestamp, Date> {
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
struct TryCast<storage::VarlenEntry, bool> {
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
struct TryCast<storage::VarlenEntry, OutType, std::enable_if_t<detail::is_integer_type_v<OutType>>> {
  bool operator()(const storage::VarlenEntry &input, OutType *output) const {
    const auto buf = reinterpret_cast<const char *>(input.Content());
    const auto len = input.Size();
    const auto [p, ec] = std::from_chars(buf, buf + len, *output);
    return ec == std::errc();
  }
};

/**
 * String to single-precision float.
 */
template <>
struct TryCast<storage::VarlenEntry, float> {
  bool operator()(const storage::VarlenEntry &input, float *output) const;
};

/**
 * String to double-precision float.
 */
template <>
struct TryCast<storage::VarlenEntry, double> {
  bool operator()(const storage::VarlenEntry &input, double *output) const;
};

/**
 * String to date.
 */
template <>
struct TryCast<storage::VarlenEntry, Date> {
  bool operator()(const storage::VarlenEntry &input, Date *output) const {
    *output = Date::FromString(input.StringView().data());
    return true;
  }
};

/**
 * String to timestamp.
 */
template <>
struct TryCast<storage::VarlenEntry, Timestamp> {
  bool operator()(const storage::VarlenEntry &input, Timestamp *output) const {
    *output = Timestamp::FromString(input.StringView().data());
    return true;
  }
};

}  // namespace terrier::execution::sql
