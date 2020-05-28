#pragma once

#include <string>

#include "common/exception.h"
#include "common/managed_pointer.h"
#include "type/type_id.h"

namespace terrier::parser {
class ConstantValueExpression;
}

namespace terrier::binder {

class BinderUtil {
 public:
  BinderUtil() = delete;

  /**
   * Attempt to convert the transient value to the desired type.
   * Note that type promotion could be an upcast or downcast size-wise.
   *
   * @param value The transient value to be checked and potentially promoted.
   * @param desired_type The type to promote the transient value to.
   */
  static void CheckAndTryPromoteType(common::ManagedPointer<parser::ConstantValueExpression> value,
                                     type::TypeId desired_type);

  /**
   * Convenience function. Used by the visitor sheep to report that an error has occurred, causing BINDER_EXCEPTION.
   * @param message The error message.
   */
  static void ReportFailure(const std::string &message) { throw BINDER_EXCEPTION(message.c_str()); }

  /**
   * @return True if the value of @p int_val fits in the Output type, false otherwise.
   */
  template <typename Output, typename Input>
  static bool IsRepresentable(Input int_val);

  /**
   * @return Casted numeric type, or an exception if the cast fails.
   */
  template <typename Input>
  static void TryCastNumericAll(common::ManagedPointer<parser::ConstantValueExpression> value, Input int_val,
                                type::TypeId desired_type);
};

/// @cond DOXYGEN_IGNORE
extern template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                                   const int8_t int_val, const type::TypeId desired_type);
extern template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                                   const int16_t int_val, const type::TypeId desired_type);
extern template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                                   const int32_t int_val, const type::TypeId desired_type);
extern template void BinderUtil::TryCastNumericAll(const common::ManagedPointer<parser::ConstantValueExpression> value,
                                                   const int64_t int_val, const type::TypeId desired_type);

extern template bool BinderUtil::IsRepresentable<int8_t>(const int8_t int_val);
extern template bool BinderUtil::IsRepresentable<int16_t>(const int8_t int_val);
extern template bool BinderUtil::IsRepresentable<int32_t>(const int8_t int_val);
extern template bool BinderUtil::IsRepresentable<int64_t>(const int8_t int_val);
extern template bool BinderUtil::IsRepresentable<double>(const int8_t int_val);

extern template bool BinderUtil::IsRepresentable<int8_t>(const int16_t int_val);
extern template bool BinderUtil::IsRepresentable<int16_t>(const int16_t int_val);
extern template bool BinderUtil::IsRepresentable<int32_t>(const int16_t int_val);
extern template bool BinderUtil::IsRepresentable<int64_t>(const int16_t int_val);
extern template bool BinderUtil::IsRepresentable<double>(const int16_t int_val);

extern template bool BinderUtil::IsRepresentable<int8_t>(const int32_t int_val);
extern template bool BinderUtil::IsRepresentable<int16_t>(const int32_t int_val);
extern template bool BinderUtil::IsRepresentable<int32_t>(const int32_t int_val);
extern template bool BinderUtil::IsRepresentable<int64_t>(const int32_t int_val);
extern template bool BinderUtil::IsRepresentable<double>(const int32_t int_val);

extern template bool BinderUtil::IsRepresentable<int8_t>(const int64_t int_val);
extern template bool BinderUtil::IsRepresentable<int16_t>(const int64_t int_val);
extern template bool BinderUtil::IsRepresentable<int32_t>(const int64_t int_val);
extern template bool BinderUtil::IsRepresentable<int64_t>(const int64_t int_val);
extern template bool BinderUtil::IsRepresentable<double>(const int64_t int_val);

extern template bool BinderUtil::IsRepresentable<int8_t>(const double int_val);
extern template bool BinderUtil::IsRepresentable<int16_t>(const double int_val);
extern template bool BinderUtil::IsRepresentable<int32_t>(const double int_val);
extern template bool BinderUtil::IsRepresentable<int64_t>(const double int_val);
extern template bool BinderUtil::IsRepresentable<double>(const double int_val);
/// @endcond

}  // namespace terrier::binder