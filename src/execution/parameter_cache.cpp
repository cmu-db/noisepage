//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_cache.cpp
//
// Identification: src/include/execution/parameter_cache.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/parameter_cache.h"
#include "execution/codegen.h"
#include "execution/proxy/query_parameters_proxy.h"
#include "execution/type/sql_type.h"
#include "execution/type/type.h"
#include "execution/updateable_storage.h"
#include "execution/value.h"

namespace terrier::execution {

void ParameterCache::Populate(CodeGen &codegen, llvm::Value *query_parameters_ptr) {
  auto &parameters = parameters_map_.GetParameters();
  for (uint32_t i = 0; i < parameters.size(); i++) {
    auto &parameter = parameters[i];
    auto type_id = parameter.GetValueType();
    auto is_nullable = parameter.IsNullable();
    auto val = DeriveParameterValue(codegen, query_parameters_ptr, i, type_id, is_nullable);
    values_.push_back(val);
  }
}

Value ParameterCache::GetValue(uint32_t index) const { return values_[index]; }

Value ParameterCache::GetValue(const expression::AbstractExpression *expr) const {
  return GetValue(parameters_map_.GetIndex(expr));
}

void ParameterCache::Reset() { values_.clear(); }

Value ParameterCache::DeriveParameterValue(CodeGen &codegen, llvm::Value *query_parameters_ptr, uint32_t index,
                                                    ::terrier::type::TypeId type_id, bool is_nullable) {
  llvm::Value *val = nullptr, *len = nullptr;
  std::vector<llvm::Value *> args = {query_parameters_ptr, codegen.Const32(index)};
  switch (type_id) {
    case ::terrier::type::TypeId::BOOLEAN: {
      val = codegen.Call(QueryParametersProxy::GetBoolean, args);
      break;
    }
    case ::terrier::type::TypeId::TINYINT: {
      val = codegen.Call(QueryParametersProxy::GetTinyInt, args);
      break;
    }
    case ::terrier::type::TypeId::SMALLINT: {
      val = codegen.Call(QueryParametersProxy::GetSmallInt, args);
      break;
    }
    case ::terrier::type::TypeId::INTEGER: {
      val = codegen.Call(QueryParametersProxy::GetInteger, args);
      break;
    }
    case ::terrier::type::TypeId::BIGINT: {
      val = codegen.Call(QueryParametersProxy::GetBigInt, args);
      break;
    }
    case ::terrier::type::TypeId::DECIMAL: {
      val = codegen.Call(QueryParametersProxy::GetDouble, args);
      break;
    }
    case ::terrier::type::TypeId::DATE: {
      val = codegen.Call(QueryParametersProxy::GetDate, args);
      break;
    }
    case ::terrier::type::TypeId::TIMESTAMP: {
      val = codegen.Call(QueryParametersProxy::GetTimestamp, args);
      break;
    }
    case ::terrier::type::TypeId::VARCHAR: {
      val = codegen.Call(QueryParametersProxy::GetVarcharVal, args);
      len = codegen.Call(QueryParametersProxy::GetVarcharLen, args);
      break;
    }
    case ::terrier::type::TypeId::VARBINARY: {
      val = codegen.Call(QueryParametersProxy::GetVarbinaryVal, args);
      len = codegen.Call(QueryParametersProxy::GetVarbinaryLen, args);
      break;
    }
    default: { throw Exception{"Unknown parameter storage value type " + TypeIdToString(type_id)}; }
  }
  llvm::Value *is_null = nullptr;
  if (is_nullable) {
    is_null = codegen.Call(QueryParametersProxy::IsNull, args);
  }
  return Value{type::Type{type_id, is_nullable}, val, len, is_null};
}

}  // namespace terrier::execution
