#pragma once

#include "execution/query_parameters_map.h"
#include "expression/parameter.h"
#include "sql/plannode/abstract_plannode.h"
#include "sql/type/type_id.h"
#include "type/value_peeker.h"

// TODO(Justin):
// -terrier::type::Value
// -expression
// -value_peeker

namespace terrier::execution {

class QueryParameters {
 public:
  QueryParameters() = default;

  // OBSOLETE: This is for the legacy interpreted execution engine!
  // This is not marked as explicit, because we want implicit casting from
  // vector of values.
  QueryParameters(std::vector<terrier::type::Value> values)
      : parameters_map_(), parameters_values_(std::move(values)) {}

  QueryParameters(sql::plannode::AbstractPlanNode &plan, const std::vector<terrier::type::Value> &values) {
    // Extract Parameters information and set value type for all the PVE
    plan.VisitParameters(parameters_map_, parameters_values_, values);
  }

  DISALLOW_COPY(QueryParameters);

  QueryParameters(QueryParameters &&other) noexcept
      : parameters_map_(std::move(other.parameters_map_)), parameters_values_(std::move(other.parameters_values_)) {}

  QueryParameters &operator=(QueryParameters &&other) noexcept {
    parameters_map_ = std::move(other.parameters_map_);
    parameters_values_ = std::move(other.parameters_values_);
    return *this;
  }

  // Set the values from the user's query parameters
  const QueryParametersMap &GetQueryParametersMap() const { return parameters_map_; }

  const std::vector<terrier::type::Value> &GetParameterValues() const { return parameters_values_; }

  uint32_t GetParameterIdx(const expression::AbstractExpression *expression) const {
    return parameters_map_.GetIndex(expression);
  }

  // Get the parameter value's type at the specified index
  ::terrier::type::TypeId GetValueType(uint32_t index) const { return parameters_values_[index].GetTypeId(); }

  // Get the parameter object vector
  const std::vector<expression::Parameter> &GetParameters() const { return parameters_map_.GetParameters(); }

  bool GetBoolean(uint32_t index) const { return terrier::type::ValuePeeker::PeekBoolean(parameters_values_[index]); }

  int8_t GetTinyInt(uint32_t index) const { return terrier::type::ValuePeeker::PeekTinyInt(parameters_values_[index]); }

  int16_t GetSmallInt(uint32_t index) const {
    return terrier::type::ValuePeeker::PeekSmallInt(parameters_values_[index]);
  }

  int32_t GetInteger(uint32_t index) const {
    return terrier::type::ValuePeeker::PeekInteger(parameters_values_[index]);
  }

  int64_t GetBigInt(uint32_t index) const { return terrier::type::ValuePeeker::PeekBigInt(parameters_values_[index]); }

  double GetDouble(uint32_t index) const { return terrier::type::ValuePeeker::PeekDouble(parameters_values_[index]); }

  int32_t GetDate(uint32_t index) const { return terrier::type::ValuePeeker::PeekDate(parameters_values_[index]); }

  uint64_t GetTimestamp(uint32_t index) const {
    return terrier::type::ValuePeeker::PeekTimestamp(parameters_values_[index]);
  }

  const char *GetVarcharVal(uint32_t index) const {
    return terrier::type::ValuePeeker::PeekVarchar(parameters_values_[index]);
  }

  uint32_t GetVarcharLen(uint32_t index) const { return parameters_values_[index].GetLength(); }

  const char *GetVarbinaryVal(uint32_t index) const {
    return terrier::type::ValuePeeker::PeekVarbinary(parameters_values_[index]);
  }

  uint32_t GetVarbinaryLen(uint32_t index) const { return parameters_values_[index].GetLength(); }

  // Get the nullability for the value at the index
  bool IsNull(uint32_t index) const { return parameters_values_[index].IsNull(); }

 private:
  // Parameter Map
  QueryParametersMap parameters_map_;

  // Parameter's value
  std::vector<terrier::type::Value> parameters_values_;
};

}  // namespace terrier::execution
