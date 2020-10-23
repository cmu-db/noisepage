#pragma once

// Taken from https://github.com/lemire/fast_double_parser

#include <cfloat>
#include <cinttypes>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace noisepage::execution::util {

/** Parses doubles. */
class FastDoubleParser {
 private:
#define FASTFLOAT_SMALLEST_POWER -325
#define FASTFLOAT_LARGEST_POWER 308

#ifdef _MSC_VER
#ifndef really_inline
#define really_inline __forceinline
#endif  // really_inline
#ifndef unlikely
#define unlikely(x) x
#endif  // unlikely
#else   // _MSC_VER
#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif  // unlikely
#ifndef really_inline
#define really_inline __attribute__((always_inline)) inline
#endif  // really_inline
#endif  // _MSC_VER

  struct Value128 {
    uint64_t low_;
    uint64_t high_;
  };

  static inline Value128 FullMultiplication(uint64_t value1, uint64_t value2) {
    Value128 answer;
#ifdef _MSC_VER
    // todo: this might fail under visual studio for ARM
    answer.low = _umul128(value1, value2, &answer.high);
#else
    __uint128_t r = (static_cast<__uint128_t>(value1)) * value2;
    answer.low_ = static_cast<uint64_t>(r);
    answer.high_ = static_cast<uint64_t>(r >> 64);
#endif
    return answer;
  }

  /* result might be undefined when input_num is zero */
  static inline int LeadingZeroes(uint64_t input_num) {
#ifdef _MSC_VER
    uint64_t leading_zero = 0;
    // Search the mask data from most significant bit (MSB)
    // to least significant bit (LSB) for a set bit (1).
    if (_BitScanReverse64(&leading_zero, input_num))
      return static_cast<int>(63 - leading_zero);
    else
      return 64;
#else
    return __builtin_clzll(input_num);
#endif  // _MSC_VER
  }

  // Precomputed powers of ten from 10^0 to 10^22. These
  // can be represented exactly using the double type.
  static const double POWER_OF_TEN[];

  static inline bool IsInteger(char c) {
    return (c >= '0' && c <= '9');
    // this gets compiled to (uint8_t)(c - '0') <= 9 on all decent compilers
  }

  // the mantissas of powers of ten from FASTFLOAT_SMALLEST_POWER to FASTFLOAT_LARGEST_POWER, extended
  // out to sixty four bits This struct will likely get padded to 16 bytes.
  using components = struct {
    uint64_t mantissa_;
    int32_t exp_;
  };

  // The array power_of_ten_components contain the powers of ten approximated
  // as a 64-bit mantissa, with an exponent part. It goes from 10^
  // FASTFLOAT_SMALLEST_POWER to
  // 10^FASTFLOAT_LARGEST_POWER (inclusively). The mantissa is truncated, and
  // never rounded up.
  // Uses about 10KB.
  static const components POWER_OF_TEN_COMPONENTS[];
  // A complement from power_of_ten_components
  // complete to a 128-bit mantissa.
  static const uint64_t MANTISSA_128[];

  // Attempts to compute i * 10^(power) exactly; and if "negative" is
  // true, negate the result.
  // This function will only work in some cases, when it does not work, success is
  // set to false. This should work *most of the time* (like 99% of the time).
  // We assume that power is in the [FASTFLOAT_SMALLEST_POWER,
  // FASTFLOAT_LARGEST_POWER] interval: the caller is responsible for this check.
  really_inline static double ComputeFloat64(int64_t power, uint64_t i, bool negative, bool *success) {
    // we start with a fast path
    // It was described in
    // Clinger WD. How to read floating point numbers accurately.
    // ACM SIGPLAN Notices. 1990
    if (-22 <= power && power <= 22 && i <= 9007199254740991) {
      // convert the integer into a double. This is lossless since
      // 0 <= i <= 2^53 - 1.
      double d = i;
      //
      // The general idea is as follows.
      // If 0 <= s < 2^53 and if 10^0 <= p <= 10^22 then
      // 1) Both s and p can be represented exactly as 64-bit floating-point
      // values
      // (binary64).
      // 2) Because s and p can be represented exactly as floating-point values,
      // then s * p
      // and s / p will produce correctly rounded values.
      //
      if (power < 0) {
        d = d / POWER_OF_TEN[-power];
      } else {
        d = d * POWER_OF_TEN[power];
      }
      if (negative) {
        d = -d;
      }
      *success = true;
      return d;
    }
    // When 22 < power && power <  22 + 16, we could
    // hope for another, secondary fast path.  It wa
    // described by David M. Gay in  "Correctly rounded
    // binary-decimal and decimal-binary conversions." (1990)
    // If you need to compute i * 10^(22 + x) for x < 16,
    // first compute i * 10^x, if you know that result is exact
    // (e.g., when i * 10^x < 2^53),
    // then you can still proceed and do (i * 10^x) * 10^22.
    // Is this worth your time?
    // You need  22 < power *and* power <  22 + 16 *and* (i * 10^(x-22) < 2^53)
    // for this second fast path to work.
    // If you you have 22 < power *and* power <  22 + 16, and then you
    // optimistically compute "i * 10^(x-22)", there is still a chance that you
    // have wasted your time if i * 10^(x-22) >= 2^53. It makes the use cases of
    // this optimization maybe less common than we would like. Source:
    // http://www.exploringbinary.com/fast-path-decimal-to-floating-point-conversion/
    // also used in RapidJSON: https://rapidjson.org/strtod_8h_source.html

    // The fast path has now failed, so we are failing back on the slower path.

    // In the slow path, we need to adjust i so that it is > 1<<63 which is always
    // possible, except if i == 0, so we handle i == 0 separately.
    if (i == 0) {
      return 0.0;
    }

    // We are going to need to do some 64-bit arithmetic to get a more precise product.
    // We use a table lookup approach.
    components c = POWER_OF_TEN_COMPONENTS[power - FASTFLOAT_SMALLEST_POWER];  // safe because
    // power >= FASTFLOAT_SMALLEST_POWER
    // and power <= FASTFLOAT_LARGEST_POWER
    // we recover the mantissa of the power, it has a leading 1. It is always
    // rounded down.
    uint64_t factor_mantissa = c.mantissa_;
    // We want the most significant bit of i to be 1. Shift if needed.
    int lz = LeadingZeroes(i);
    i <<= lz;
    // We want the most significant 64 bits of the product. We know
    // this will be non-zero because the most significant bit of i is
    // 1.
    Value128 product = FullMultiplication(i, factor_mantissa);
    uint64_t lower = product.low_;
    uint64_t upper = product.high_;
    // We know that upper has at most one leading zero because
    // both i and  factor_mantissa have a leading one. This means
    // that the result is at least as large as ((1<<63)*(1<<63))/(1<<64).

    // As long as the first 9 bits of "upper" are not "1", then we
    // know that we have an exact computed value for the leading
    // 55 bits because any imprecision would play out as a +1, in
    // the worst case.
    // We expect this next branch to be rarely taken (say 1% of the time).
    // When (upper & 0x1FF) == 0x1FF, it can be common for
    // lower + i < lower to be true (proba. much higher than 1%).
    if (unlikely((upper & 0x1FF) == 0x1FF) && (lower + i < lower)) {
      uint64_t factor_mantissa_low = MANTISSA_128[power - FASTFLOAT_SMALLEST_POWER];
      // next, we compute the 64-bit x 128-bit multiplication, getting a 192-bit
      // result (three 64-bit values)
      product = FullMultiplication(i, factor_mantissa_low);
      uint64_t product_low = product.low_;
      uint64_t product_middle2 = product.high_;
      uint64_t product_middle1 = lower;
      uint64_t product_high = upper;
      uint64_t product_middle = product_middle1 + product_middle2;
      if (product_middle < product_middle1) {
        product_high++;  // overflow carry
      }
      // we want to check whether mantissa *i + i would affect our result
      // This does happen, e.g. with 7.3177701707893310e+15
      if (((product_middle + 1 == 0) && ((product_high & 0x1FF) == 0x1FF) &&
           (product_low + i < product_low))) {  // let us be prudent and bail out.
        *success = false;
        return 0;
      }
      upper = product_high;
      lower = product_middle;
    }
    // The final mantissa should be 53 bits with a leading 1.
    // We shift it so that it occupies 54 bits with a leading 1.
    ///////
    uint64_t upperbit = upper >> 63;
    uint64_t mantissa = upper >> (upperbit + 9);
    lz += static_cast<int>(1 ^ upperbit);
    // Here we have mantissa < (1<<54).

    // We have to round to even. The "to even" part
    // is only a problem when we are right in between two floats
    // which we guard against.
    // If we have lots of trailing zeros, we may fall right between two
    // floating-point values.
    if (unlikely((lower == 0) && ((upper & 0x1FF) == 0) && ((mantissa & 3) == 1))) {
      // if mantissa & 1 == 1 we might need to round up.
      //
      // Scenarios:
      // 1. We are not in the middle. Then we should round up.
      //
      // 2. We are right in the middle. Whether we round up depends
      // on the last significant bit: if it is "one" then we round
      // up (round to even) otherwise, we do not.
      //
      // So if the last significant bit is 1, we can safely round up.
      // Hence we only need to bail out if (mantissa & 3) == 1.
      // Otherwise we may need more accuracy or analysis to determine whether
      // we are exactly between two floating-point numbers.
      // It can be triggered with 1e23.
      // Note: because the factor_mantissa and factor_mantissa_low are
      // almost always rounded down (except for small positive powers),
      // almost always should round up.
      *success = false;
      return 0;
    }
    mantissa += mantissa & 1;
    mantissa >>= 1;
    // Here we have mantissa < (1<<53), unless there was an overflow
    if (mantissa >= (1ULL << 53)) {
      //////////
      // This will happen when parsing values such as 7.2057594037927933e+16
      ////////
      mantissa = (1ULL << 52);
      lz--;  // undo previous addition
    }
    mantissa &= ~(1ULL << 52);
    uint64_t real_exponent = c.exp_ - lz;
    // we have to check that real_exponent is in range, otherwise we bail out
    if (unlikely((real_exponent < 1) || (real_exponent > 2046))) {
      *success = false;
      return 0;
    }
    mantissa |= real_exponent << 52;
    mantissa |= ((static_cast<uint64_t>(negative)) << 63);
    double d;
    memcpy(&d, &mantissa, sizeof(d));
    *success = true;
    return d;
  }

  static bool ParseFloatStrtod(const char *ptr, double *outDouble) {
    char *endptr;
    *outDouble = strtod(ptr, &endptr);
    // Some libraries will set errno = ERANGE when the value is subnormal,
    // yet we may want to be able to parse subnormal values.
    // However, we do not want to tolerate NAN or infinite values.
    // There isno realistic application where you might need values so large than
    // they can't fit in binary64. The maximal value is about  1.7976931348623157
    // × 10^308 It is an unimaginable large number. There will never be any piece
    // of engineering involving as many as 10^308 parts. It is estimated that
    // there are about 10^80 atoms in the universe. The estimate for the total
    // number of electrons is similar. Using a double-precision floating-point
    // value, we can represent easily the number of atoms in the universe. We
    // could  also represent the number of ways you can pick any three individual
    // atoms at random in the universe.
    return !((endptr == ptr) || (!std::isfinite(*outDouble)));
  }

#if (__cplusplus < 201703L)
  template <char First, char... Rest>
  struct one_of_impl {
    really_inline static bool call(char v) { return First == v || one_of_impl<Rest...>::call(v); }
  };
  template <char First>
  struct one_of_impl<First> {
    really_inline static bool call(char v) { return First == v; }
  };
  template <char... Values>
  really_inline bool is_one_of(char v) {
    return one_of_impl<Values...>::call(v);
  }
#else
  template <char... Values>
  bool static IsOneOf(char v) {
    return ((v == Values) || ...);
  }
#endif

  // We need to check that the character following a zero is valid. This is
  // probably frequent and it is hard than it looks. We are building all of this
  // just to differentiate between 0x1 (invalid), 0,1 (valid) 0e1 (valid)...
  constexpr static bool STRUCTURAL_OR_WHITESPACE_OR_EXPONENT_OR_DECIMAL_NEGATED[256] = {
      true,  true, true, true, true, true,  true, true, true, false, false, true,  true,  false, true,  true,
      true,  true, true, true, true, true,  true, true, true, true,  true,  true,  true,  true,  true,  true,
      false, true, true, true, true, true,  true, true, true, true,  true,  true,  false, true,  false, true,
      true,  true, true, true, true, true,  true, true, true, true,  false, true,  true,  true,  true,  true,
      true,  true, true, true, true, false, true, true, true, true,  true,  true,  true,  true,  true,  true,
      true,  true, true, true, true, true,  true, true, true, true,  true,  false, true,  false, true,  true,
      true,  true, true, true, true, false, true, true, true, true,  true,  true,  true,  true,  true,  true,
      true,  true, true, true, true, true,  true, true, true, true,  true,  false, true,  false, true,  true,
      true,  true, true, true, true, true,  true, true, true, true,  true,  true,  true,  true,  true,  true,
      true,  true, true, true, true, true,  true, true, true, true,  true,  true,  true,  true,  true,  true,
      true,  true, true, true, true, true,  true, true, true, true,  true,  true,  true,  true,  true,  true,
      true,  true, true, true, true, true,  true, true, true, true,  true,  true,  true,  true,  true,  true,
      true,  true, true, true, true, true,  true, true, true, true,  true,  true,  true,  true,  true,  true,
      true,  true, true, true, true, true,  true, true, true, true,  true,  true,  true,  true,  true,  true,
      true,  true, true, true, true, true,  true, true, true, true,  true,  true,  true,  true,  true,  true,
      true,  true, true, true, true, true,  true, true, true, true,  true,  true,  true,  true,  true,  true};

  really_inline static bool IsNotStructuralOrWhitespaceOrExponentOrDecimal(unsigned char c) {
    return STRUCTURAL_OR_WHITESPACE_OR_EXPONENT_OR_DECIMAL_NEGATED[c];
  }

  // parse the number at p
  template <char... DecSeparators>
  really_inline static bool ParseNumberBase(const char *p, double *outDouble) {
    const char *pinit = p;
    bool found_minus = (*p == '-');
    bool negative = false;
    if (found_minus) {
      ++p;
      negative = true;
      if (!IsInteger(*p)) {  // a negative sign must be followed by an integer
        return false;
      }
    }
    const char *const start_digits = p;

    uint64_t i;       // an unsigned int avoids signed overflows (which are bad)
    if (*p == '0') {  // 0 cannot be followed by an integer
      ++p;
      if (IsNotStructuralOrWhitespaceOrExponentOrDecimal(*p)) {
        return false;
      }
      i = 0;
    } else {
      if (!(IsInteger(*p))) {  // must start with an integer
        return false;
      }
      unsigned char digit = *p - '0';
      i = digit;
      p++;
      // the is_made_of_eight_digits_fast routine is unlikely to help here because
      // we rarely see large integer parts like 123456789
      while (IsInteger(*p)) {
        digit = *p - '0';
        // a multiplication by 10 is cheaper than an arbitrary integer
        // multiplication
        i = 10 * i + digit;  // might overflow, we will handle the overflow later
        ++p;
      }
    }
    int64_t exponent = 0;
    const char *first_after_period = nullptr;
    if (IsOneOf<DecSeparators...>(*p)) {
      ++p;
      first_after_period = p;
      if (IsInteger(*p)) {
        auto digit = static_cast<unsigned char>(*p - '0');
        ++p;
        i = i * 10 + digit;  // might overflow + multiplication by 10 is likely
        // cheaper than arbitrary mult.
        // we will handle the overflow later
      } else {
        return false;
      }
      while (IsInteger(*p)) {
        auto digit = static_cast<unsigned char>(*p - '0');
        ++p;
        i = i * 10 + digit;  // in rare cases, this will overflow, but that's ok
        // because we have parse_highprecision_float later.
      }
      exponent = static_cast<int64_t>(first_after_period - p);
    }
    int digit_count = static_cast<int>(p - start_digits - 1);  // used later to guard against overflows
    int64_t exp_number = 0;                                    // exponential part
    if (('e' == *p) || ('E' == *p)) {
      ++p;
      bool neg_exp = false;
      if ('-' == *p) {
        neg_exp = true;
        ++p;
      } else if ('+' == *p) {
        ++p;
      }
      if (!IsInteger(*p)) {
        return false;
      }
      auto digit = static_cast<unsigned char>(*p - '0');
      exp_number = digit;
      p++;
      if (IsInteger(*p)) {
        digit = static_cast<unsigned char>(*p - '0');
        exp_number = 10 * exp_number + digit;
        ++p;
      }
      if (IsInteger(*p)) {
        digit = static_cast<unsigned char>(*p - '0');
        exp_number = 10 * exp_number + digit;
        ++p;
      }
      while (IsInteger(*p)) {
        if (exp_number > 0x100000000) {  // we need to check for overflows
          // we refuse to parse this
          return false;
        }
        digit = static_cast<unsigned char>(*p - '0');
        exp_number = 10 * exp_number + digit;
        ++p;
      }
      exponent += (neg_exp ? -exp_number : exp_number);
    }
    // If we frequently had to deal with long strings of digits,
    // we could extend our code by using a 128-bit integer instead
    // of a 64-bit integer. However, this is uncommon.
    if (unlikely((digit_count >= 19))) {  // this is uncommon
      // It is possible that the integer had an overflow.
      // We have to handle the case where we have 0.0000somenumber.
      const char *start = start_digits;
      while (*start == '0' || IsOneOf<DecSeparators...>(*start)) {
        start++;
      }
      // we over-decrement by one when there is a decimal separator
      digit_count -= static_cast<int>((start - start_digits));
      if (digit_count >= 19) {
        // Chances are good that we had an overflow!
        // We start anew.
        // This will happen in the following examples:
        // 10000000000000000000000000000000000000000000e+308
        // 3.1415926535897932384626433832795028841971693993751
        //
        return ParseFloatStrtod(pinit, outDouble);
      }
    }
    if (unlikely(exponent < FASTFLOAT_SMALLEST_POWER) || (exponent > FASTFLOAT_LARGEST_POWER)) {
      // this is almost never going to get called!!!
      // exponent could be as low as 325
      return ParseFloatStrtod(pinit, outDouble);
    }
    // from this point forward, exponent >= FASTFLOAT_SMALLEST_POWER and
    // exponent <= FASTFLOAT_LARGEST_POWER
    bool success = true;
    *outDouble = ComputeFloat64(exponent, i, negative, &success);
    if (!success) {
      // we are almost never going to get here.
      return ParseFloatStrtod(pinit, outDouble);
    }
    return true;
  }

 public:
  /** @return True if the parse was successful. */
  really_inline static bool ParseNumber(const char *p, double *outDouble) {
    return ParseNumberBase<'.', ','>(p, outDouble);
  }

  /** @return True if the parse was successful. */
  really_inline static bool ParseNumberDecimalSeparatorDot(const char *p, double *outDouble) {
    return ParseNumberBase<'.'>(p, outDouble);
  }

  /** @return True if the parse was successful. */
  really_inline static bool ParseNumberDecimalSeparatorComma(const char *p, double *outDouble) {
    return ParseNumberBase<','>(p, outDouble);
  }

// Hygiene.
#undef really_inline
#undef unlikely
#undef FASTFLOAT_LARGEST_POWER
#undef FASTFLOAT_SMALLEST_POWER
};
}  // namespace noisepage::execution::util
