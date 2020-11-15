#include <string>

#include "common/error/exception.h"
#include "execution/sql/operators/cast_operators.h"
#include "execution/sql/vector_operations/unary_operation_executor.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::sql {

namespace {

template <typename InType, typename OutType, bool IgnoreNull = true>
void StandardTemplatedCastOperation(const exec::ExecutionSettings &exec_settings, const Vector &source,
                                    Vector *target) {
  UnaryOperationExecutor::Execute<InType, OutType, noisepage::execution::sql::Cast<InType, OutType>, IgnoreNull>(
      exec_settings, source, target);
}

template <typename InType>
void CastToStringOperation(const exec::ExecutionSettings &exec_settings, const Vector &source, Vector *target) {
  NOISEPAGE_ASSERT(target->GetTypeId() == TypeId::Varchar, "Result vector must be string");
  noisepage::execution::sql::Cast<InType, std::string> cast_op;
  UnaryOperationExecutor::Execute<InType, storage::VarlenEntry, true>(
      exec_settings, source, target,
      [&](const InType in) { return target->GetMutableStringHeap()->AddVarlen(cast_op(in)); });
}

// Cast from a numeric-ish type into one of the many supported types.
template <typename InType>
void CastNumericOperation(const exec::ExecutionSettings &exec_settings, const Vector &source, Vector *target,
                          SqlTypeId target_type) {
  switch (target_type) {
    case SqlTypeId::Boolean:
      StandardTemplatedCastOperation<InType, bool>(exec_settings, source, target);
      break;
    case SqlTypeId::TinyInt:
      StandardTemplatedCastOperation<InType, int8_t>(exec_settings, source, target);
      break;
    case SqlTypeId::SmallInt:
      StandardTemplatedCastOperation<InType, int16_t>(exec_settings, source, target);
      break;
    case SqlTypeId::Integer:
      StandardTemplatedCastOperation<InType, int32_t>(exec_settings, source, target);
      break;
    case SqlTypeId::BigInt:
      StandardTemplatedCastOperation<InType, int64_t>(exec_settings, source, target);
      break;
    case SqlTypeId::Real:
      StandardTemplatedCastOperation<InType, float>(exec_settings, source, target);
      break;
    case SqlTypeId::Double:
      StandardTemplatedCastOperation<InType, double>(exec_settings, source, target);
      break;
    case SqlTypeId::Varchar:
      CastToStringOperation<InType>(exec_settings, source, target);
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("unsupported cast: {} -> {}", TypeIdToString(source.GetTypeId()),
                                                  TypeIdToString(target->GetTypeId()))
                                          .data());
  }
}

void CastDateOperation(const exec::ExecutionSettings &exec_settings, const Vector &source, Vector *target,
                       SqlTypeId target_type) {
  switch (target_type) {
    case SqlTypeId::Timestamp:
      StandardTemplatedCastOperation<Date, Timestamp>(exec_settings, source, target);
      break;
    case SqlTypeId::Varchar:
      CastToStringOperation<Date>(exec_settings, source, target);
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("unsupported cast: {} -> {}", TypeIdToString(source.GetTypeId()),
                                                  TypeIdToString(target->GetTypeId()))
                                          .data());
  }
}

void CastTimestampOperation(const exec::ExecutionSettings &exec_settings, const Vector &source, Vector *target,
                            SqlTypeId target_type) {
  switch (target_type) {
    case SqlTypeId::Date:
      StandardTemplatedCastOperation<Timestamp, Date>(exec_settings, source, target);
      break;
    case SqlTypeId::Varchar:
      CastToStringOperation<Timestamp>(exec_settings, source, target);
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("unsupported cast: {} -> {}", TypeIdToString(source.GetTypeId()),
                                                  TypeIdToString(target->GetTypeId()))
                                          .data());
  }
}

void CastStringOperation(const exec::ExecutionSettings &exec_settings, const Vector &source, Vector *target,
                         SqlTypeId target_type) {
  switch (target_type) {
    case SqlTypeId::Boolean:
      StandardTemplatedCastOperation<storage::VarlenEntry, bool, true>(exec_settings, source, target);
      break;
    case SqlTypeId::TinyInt:
      StandardTemplatedCastOperation<storage::VarlenEntry, int8_t, true>(exec_settings, source, target);
      break;
    case SqlTypeId::SmallInt:
      StandardTemplatedCastOperation<storage::VarlenEntry, int16_t, true>(exec_settings, source, target);
      break;
    case SqlTypeId::Integer:
      StandardTemplatedCastOperation<storage::VarlenEntry, int32_t, true>(exec_settings, source, target);
      break;
    case SqlTypeId::BigInt:
      StandardTemplatedCastOperation<storage::VarlenEntry, int64_t, true>(exec_settings, source, target);
      break;
    case SqlTypeId::Real:
      StandardTemplatedCastOperation<storage::VarlenEntry, float, true>(exec_settings, source, target);
      break;
    case SqlTypeId::Double:
      StandardTemplatedCastOperation<storage::VarlenEntry, double, true>(exec_settings, source, target);
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("unsupported cast: {} -> {}", TypeIdToString(source.GetTypeId()),
                                                  TypeIdToString(target->GetTypeId()))
                                          .data());
  }
}

}  // namespace

void VectorOps::Cast(const exec::ExecutionSettings &exec_settings, const Vector &source, Vector *target,
                     SqlTypeId source_type, SqlTypeId target_type) {
  switch (source_type) {
    case SqlTypeId::Boolean:
      CastNumericOperation<bool>(exec_settings, source, target, target_type);
      break;
    case SqlTypeId::TinyInt:
      CastNumericOperation<int8_t>(exec_settings, source, target, target_type);
      break;
    case SqlTypeId::SmallInt:
      CastNumericOperation<int16_t>(exec_settings, source, target, target_type);
      break;
    case SqlTypeId::Integer:
      CastNumericOperation<int32_t>(exec_settings, source, target, target_type);
      break;
    case SqlTypeId::BigInt:
      CastNumericOperation<int64_t>(exec_settings, source, target, target_type);
      break;
    case SqlTypeId::Real:
      CastNumericOperation<float>(exec_settings, source, target, target_type);
      break;
    case SqlTypeId::Double:
      CastNumericOperation<double>(exec_settings, source, target, target_type);
      break;
    case SqlTypeId::Date:
      CastDateOperation(exec_settings, source, target, target_type);
      break;
    case SqlTypeId::Timestamp:
      CastTimestampOperation(exec_settings, source, target, target_type);
      break;
    case SqlTypeId::Char:
    case SqlTypeId::Varchar:
      CastStringOperation(exec_settings, source, target, target_type);
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("unsupported cast: {} -> {}", TypeIdToString(source.GetTypeId()),
                                                  TypeIdToString(target->GetTypeId()))
                                          .data());
  }
}

void VectorOps::Cast(const exec::ExecutionSettings &exec_settings, const Vector &source, Vector *target) {
  const SqlTypeId src_type = GetSqlTypeFromInternalType(source.type_);
  const SqlTypeId target_type = GetSqlTypeFromInternalType(target->type_);
  Cast(exec_settings, source, target, src_type, target_type);
}

}  // namespace noisepage::execution::sql
