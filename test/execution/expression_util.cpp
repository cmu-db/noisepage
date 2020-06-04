#include "execution/compiler/expression_util.h"

#include "date/date.h"

namespace terrier::execution::compiler {

using ManagedExpression = common::ManagedPointer<parser::AbstractExpression>;

ManagedExpression ExpressionMaker::Constant(date::year_month_day ymd) {
  auto year = static_cast<int32_t>(ymd.year());
  auto month = static_cast<uint32_t>(ymd.month());
  auto day = static_cast<uint32_t>(ymd.day());
  return this->Constant(year, month, day);
}

}  // namespace terrier::execution::compiler
