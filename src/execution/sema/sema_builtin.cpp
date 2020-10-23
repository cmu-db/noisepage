#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/sema/sema.h"

namespace noisepage::execution::sema {

namespace {

bool IsPointerToSpecificBuiltin(ast::Type *type, ast::BuiltinType::Kind kind) {
  if (auto *pointee_type = type->GetPointeeType()) {
    return pointee_type->IsSpecificBuiltin(kind);
  }
  return false;
}

bool IsPointerToSQLValue(ast::Type *type) {
  if (auto *pointee_type = type->GetPointeeType()) {
    return pointee_type->IsSqlValueType();
  }
  return false;
}

bool IsPointerToAggregatorValue(ast::Type *type) {
  if (auto *pointee_type = type->GetPointeeType()) {
    return pointee_type->IsSqlAggregatorType();
  }
  return false;
}

template <typename... ArgTypes>
bool AreAllFunctions(const ArgTypes... type) {
  return (true && ... && type->IsFunctionType());
}

}  // namespace

void Sema::CheckSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin) {
  // Handle the builtins whose API is different from the other builtins.
  if (builtin == ast::Builtin::DateToSql) {
    if (!CheckArgCount(call, 3)) {
      return;
    }
    const auto int32_kind = ast::BuiltinType::Int32;
    if (!call->Arguments()[0]->GetType()->IsSpecificBuiltin(int32_kind) ||
        !call->Arguments()[1]->GetType()->IsSpecificBuiltin(int32_kind) ||
        !call->Arguments()[2]->GetType()->IsSpecificBuiltin(int32_kind)) {
      GetErrorReporter()->Report(call->Position(), ErrorMessages::kInvalidCastToSqlDate,
                                 call->Arguments()[0]->GetType(), call->Arguments()[1]->GetType(),
                                 call->Arguments()[2]->GetType());
    }
    // All good. Set return type as SQL Date.
    call->SetType(GetBuiltinType(ast::BuiltinType::Date));
    return;
  }

  // SQL Timestamp.
  if (builtin == ast::Builtin::TimestampToSql) {
    if (!CheckArgCountAtLeast(call, 1)) {
      return;
    }
    auto uint64_t_kind = ast::BuiltinType::Uint64;
    // First argument (julian_usec) is a uint64_t
    if (!call->Arguments()[0]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 0, GetBuiltinType(uint64_t_kind));
      return;
    }
    call->SetType(GetBuiltinType(ast::BuiltinType::Timestamp));
    return;
  }

  // SQL Timestamp, YMDHMSMU.
  if (builtin == ast::Builtin::TimestampToSqlYMDHMSMU) {
    if (!CheckArgCountAtLeast(call, 8)) {
      return;
    }
    auto int32_t_kind = ast::BuiltinType::Int32;
    // First argument (year) is a int32_t
    if (!call->Arguments()[0]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 0, GetBuiltinType(int32_t_kind));
      return;
    }
    // Second argument (month) is a int32_t
    if (!call->Arguments()[1]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 1, GetBuiltinType(int32_t_kind));
      return;
    }
    // Third argument (day) is a int32_t
    if (!call->Arguments()[2]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 2, GetBuiltinType(int32_t_kind));
      return;
    }
    // Fourth argument (hour) is a int32_t
    if (!call->Arguments()[3]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 3, GetBuiltinType(int32_t_kind));
      return;
    }
    // Fifth argument (minute) is a int32_t
    if (!call->Arguments()[4]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 4, GetBuiltinType(int32_t_kind));
      return;
    }
    // Sixth argument (second) is a int32_t
    if (!call->Arguments()[5]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 5, GetBuiltinType(int32_t_kind));
      return;
    }
    // Seventh argument (millisecond) is a int32_t
    if (!call->Arguments()[6]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 6, GetBuiltinType(int32_t_kind));
      return;
    }
    // Eighth argument (microsecond) is a int32_t
    if (!call->Arguments()[7]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 7, GetBuiltinType(int32_t_kind));
      return;
    }
    call->SetType(GetBuiltinType(ast::BuiltinType::Timestamp));
    return;
  }

  // Handle all the one-argument builtins.
  if (!CheckArgCount(call, 1)) {
    return;
  }

  auto input_type = call->Arguments()[0]->GetType();
  switch (builtin) {
    case ast::Builtin::BoolToSql: {
      if (!input_type->IsSpecificBuiltin(ast::BuiltinType::Bool)) {
        ReportIncorrectCallArg(call, 0, "boolean literal");
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Boolean));
      break;
    }
    case ast::Builtin::IntToSql: {
      if (!input_type->IsIntegerType()) {
        ReportIncorrectCallArg(call, 0, "integer literal");
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Integer));
      break;
    }
    case ast::Builtin::FloatToSql: {
      if (!input_type->IsFloatType()) {
        ReportIncorrectCallArg(call, 0, "floating point number literal");
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Real));
      break;
    }
    case ast::Builtin::StringToSql: {
      if (!input_type->IsStringType() || !call->Arguments()[0]->IsLitExpr()) {
        ReportIncorrectCallArg(call, 0, "string literal");
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::StringVal));
      break;
    }
    case ast::Builtin::SqlToBool: {
      if (!input_type->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
        GetErrorReporter()->Report(call->Position(), ErrorMessages::kInvalidSqlCastToBool, input_type);
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }

#define CONVERSION_CASE(Op, InputType, OutputType)                     \
  case ast::Builtin::Op: {                                             \
    if (!input_type->IsSpecificBuiltin(ast::BuiltinType::InputType)) { \
      ReportIncorrectCallArg(call, 0, "SQL " #InputType);              \
      return;                                                          \
    }                                                                  \
    call->SetType(GetBuiltinType(ast::BuiltinType::OutputType));       \
    break;                                                             \
  }
      CONVERSION_CASE(ConvertBoolToInteger, Boolean, Integer);
      CONVERSION_CASE(ConvertIntegerToReal, Integer, Real);
      CONVERSION_CASE(ConvertDateToTimestamp, Date, Timestamp);
      CONVERSION_CASE(ConvertStringToBool, StringVal, Boolean);
      CONVERSION_CASE(ConvertStringToInt, StringVal, Integer);
      CONVERSION_CASE(ConvertStringToReal, StringVal, Real);
      CONVERSION_CASE(ConvertStringToDate, StringVal, Date);
      CONVERSION_CASE(ConvertStringToTime, StringVal, Timestamp);
#undef CONVERSION_CASE

    default: {
      UNREACHABLE("Impossible SQL conversion call");
    }
  }
}

void Sema::CheckNullValueCall(ast::CallExpr *call, UNUSED_ATTRIBUTE ast::Builtin builtin) {
  if (!CheckArgCount(call, 1)) {
    return;
  }
  auto input_type = call->Arguments()[0]->GetType();
  switch (builtin) {
    case ast::Builtin::IsValNull: {
      // Input must be a SQL value.
      if (!input_type->IsSqlValueType()) {
        ReportIncorrectCallArg(call, 0, "sql_type");
        return;
      }
      // Returns a primitive boolean.
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::InitSqlNull: {
      if (!input_type->IsPointerType() || !input_type->GetPointeeType()->IsSqlValueType()) {
        ReportIncorrectCallArg(call, 0, "&sql_type");
        return;
      }
      call->SetType(input_type->GetPointeeType());
      break;
    }
    default:
      UNREACHABLE("Unsupported NULL type.");
  }
}

void Sema::CheckBuiltinStringLikeCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 2)) {
    return;
  }

  // Both arguments must be SQL strings
  auto str_kind = ast::BuiltinType::StringVal;
  if (!call->Arguments()[0]->GetType()->IsSpecificBuiltin(str_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(str_kind));
    return;
  }
  if (!call->Arguments()[1]->GetType()->IsSpecificBuiltin(str_kind)) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(str_kind));
    return;
  }

  // Returns a SQL boolean
  call->SetType(GetBuiltinType(ast::BuiltinType::Boolean));
}

void Sema::CheckBuiltinDateFunctionCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }
  // First arg must be a date.
  auto date_kind = ast::BuiltinType::Date;
  auto integer_kind = ast::BuiltinType::Integer;
  if (!call->Arguments()[0]->GetType()->IsSpecificBuiltin(date_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(date_kind));
    return;
  }

  switch (builtin) {
    case ast::Builtin::DatePart:
      if (!call->Arguments()[1]->GetType()->IsSpecificBuiltin(integer_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(integer_kind));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Integer));
      return;
    default:
      // TODO(Amadou): Support other date function.
      UNREACHABLE("Impossible date function");
  }
}

void Sema::CheckBuiltinAggHashTableCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &args = call->Arguments();

  const auto agg_ht_kind = ast::BuiltinType::AggregationHashTable;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), agg_ht_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(agg_ht_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::AggHashTableInit: {
      if (!CheckArgCount(call, 3)) {
        return;
      }
      // Second argument is the execution context.
      auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
      if (!IsPointerToSpecificBuiltin(call->Arguments()[1]->GetType(), exec_ctx_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(exec_ctx_kind)->PointerTo());
        return;
      }
      // Third argument is the payload size, a 32-bit value
      const auto uint_kind = ast::BuiltinType::Uint32;
      if (!args[2]->GetType()->IsSpecificBuiltin(uint_kind)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(uint_kind));
        return;
      }
      // Nil return
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggHashTableGetTupleCount: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
      break;
    }
    case ast::Builtin::AggHashTableGetInsertCount: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
      break;
    }
    case ast::Builtin::AggHashTableInsert: {
      if (!CheckArgCountAtLeast(call, 2)) {
        return;
      }
      // Second argument is the hash value
      const auto hash_val_kind = ast::BuiltinType::Uint64;
      if (!args[1]->GetType()->IsSpecificBuiltin(hash_val_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(hash_val_kind));
        return;
      }
      // If there's a third argument indicating regular or partitioned insertion, it must be a bool
      if (args.size() > 2 &&
          (!args[2]->IsLitExpr() || !args[2]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Bool))) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Bool));
        return;
      }
      // Return a byte pointer
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
      break;
    }
    case ast::Builtin::AggHashTableLinkEntry: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // Second argument is a HashTableEntry*
      const auto entry_kind = ast::BuiltinType::HashTableEntry;
      if (!IsPointerToSpecificBuiltin(args[1]->GetType(), entry_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(entry_kind)->PointerTo());
        return;
      }
      // Return nothing
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggHashTableLookup: {
      if (!CheckArgCount(call, 4)) {
        return;
      }
      // Second argument is the hash value
      const auto hash_val_kind = ast::BuiltinType::Uint64;
      if (!args[1]->GetType()->IsSpecificBuiltin(hash_val_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(hash_val_kind));
        return;
      }
      // Third argument is the key equality function
      if (!args[2]->GetType()->IsFunctionType()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(hash_val_kind));
        return;
      }
      // Fourth argument is the probe tuple, but any pointer will do
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
      break;
    }
    case ast::Builtin::AggHashTableProcessBatch: {
      if (!CheckArgCount(call, 6)) {
        return;
      }
      // Second argument is the input VPI.
      const auto vpi_kind = ast::BuiltinType::VectorProjectionIterator;
      if (!IsPointerToSpecificBuiltin(args[1]->GetType(), vpi_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(vpi_kind)->PointerTo());
        return;
      }
      // Third argument is an array of key columns.
      if (auto array_type = args[2]->GetType()->SafeAs<ast::ArrayType>();
          array_type == nullptr || !array_type->HasKnownLength()) {
        ReportIncorrectCallArg(call, 2, "array with known length");
        return;
      }
      // Fourth and fifth argument is the initialization and advance functions.
      if (!AreAllFunctions(args[3]->GetType(), args[4]->GetType())) {
        ReportIncorrectCallArg(call, 3, "function");
        return;
      }
      // Last arg must be a boolean.
      if (!args[5]->GetType()->IsBoolType()) {
        ReportIncorrectCallArg(call, 5, GetBuiltinType(ast::BuiltinType::Bool));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggHashTableMovePartitions: {
      if (!CheckArgCount(call, 4)) {
        return;
      }
      // Second argument is the thread state container pointer
      const auto tls_kind = ast::BuiltinType::ThreadStateContainer;
      if (!IsPointerToSpecificBuiltin(args[1]->GetType(), tls_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(tls_kind)->PointerTo());
        return;
      }
      // Third argument is the offset of the hash table in thread local state
      const auto uint32_kind = ast::BuiltinType::Uint32;
      if (!args[2]->GetType()->IsSpecificBuiltin(uint32_kind)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(uint32_kind));
        return;
      }
      // Fourth argument is the merging function
      if (!args[3]->GetType()->IsFunctionType()) {
        ReportIncorrectCallArg(call, 3, GetBuiltinType(uint32_kind));
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggHashTableParallelPartitionedScan: {
      if (!CheckArgCount(call, 4)) {
        return;
      }
      // Second argument is an opaque context pointer
      if (!args[1]->GetType()->IsPointerType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(agg_ht_kind));
        return;
      }
      // Third argument is the thread state container pointer
      const auto tls_kind = ast::BuiltinType::ThreadStateContainer;
      if (!IsPointerToSpecificBuiltin(args[2]->GetType(), tls_kind)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(tls_kind)->PointerTo());
        return;
      }
      // Fourth argument is the scanning function
      if (!args[3]->GetType()->IsFunctionType()) {
        ReportIncorrectCallArg(call, 3, GetBuiltinType(tls_kind));
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggHashTableFree: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregation hash table call");
    }
  }
}

void Sema::CheckBuiltinAggHashTableIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &args = call->Arguments();

  const auto agg_ht_iter_kind = ast::BuiltinType::AHTIterator;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), agg_ht_iter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(agg_ht_iter_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::AggHashTableIterInit: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      const auto agg_ht_kind = ast::BuiltinType::AggregationHashTable;
      if (!IsPointerToSpecificBuiltin(args[1]->GetType(), agg_ht_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(agg_ht_kind)->PointerTo());
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggHashTableIterHasNext: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::AggHashTableIterNext: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggHashTableIterGetRow: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      const auto byte_kind = ast::BuiltinType::Uint8;
      call->SetType(GetBuiltinType(byte_kind)->PointerTo());
      break;
    }
    case ast::Builtin::AggHashTableIterClose: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregation hash table iterator call");
    }
  }
}

void Sema::CheckBuiltinAggPartIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  const auto &args = call->Arguments();

  const auto part_iter_kind = ast::BuiltinType::AHTOverflowPartitionIterator;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), part_iter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(part_iter_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::AggPartIterHasNext: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::AggPartIterNext: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggPartIterGetRowEntry: {
      call->SetType(GetBuiltinType(ast::BuiltinType::HashTableEntry)->PointerTo());
      break;
    }
    case ast::Builtin::AggPartIterGetRow: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
      break;
    }
    case ast::Builtin::AggPartIterGetHash: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint64));
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregation partition iterator call");
    }
  }
}

void Sema::CheckBuiltinAggregatorCall(ast::CallExpr *call, ast::Builtin builtin) {
  const auto &args = call->Arguments();
  switch (builtin) {
    case ast::Builtin::AggInit:
    case ast::Builtin::AggReset: {
      // All arguments to @aggInit() or @aggReset() must be SQL aggregators
      for (uint32_t idx = 0; idx < call->NumArgs(); idx++) {
        if (!IsPointerToAggregatorValue(args[idx]->GetType())) {
          GetErrorReporter()->Report(call->Position(), ErrorMessages::kNotASQLAggregate, args[idx]->GetType());
          return;
        }
      }
      // Init returns nil
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggAdvance: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // First argument to @aggAdvance() must be a SQL aggregator, second must be a SQL value
      if (!IsPointerToAggregatorValue(args[0]->GetType())) {
        GetErrorReporter()->Report(call->Position(), ErrorMessages::kNotASQLAggregate, args[0]->GetType());
        return;
      }
      if (!IsPointerToSQLValue(args[1]->GetType())) {
        GetErrorReporter()->Report(call->Position(), ErrorMessages::kNotASQLAggregate, args[1]->GetType());
        return;
      }
      // Advance returns nil
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggMerge: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // Both arguments must be SQL aggregators
      bool arg0_is_agg = IsPointerToAggregatorValue(args[0]->GetType());
      bool arg1_is_agg = IsPointerToAggregatorValue(args[1]->GetType());
      if (!arg0_is_agg || !arg1_is_agg) {
        GetErrorReporter()->Report(call->Position(), ErrorMessages::kNotASQLAggregate,
                                   (!arg0_is_agg ? args[0]->GetType() : args[1]->GetType()));
        return;
      }
      // Merge returns nil
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::AggResult: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      // Argument must be a SQL aggregator
      if (!IsPointerToAggregatorValue(args[0]->GetType())) {
        GetErrorReporter()->Report(call->Position(), ErrorMessages::kNotASQLAggregate, args[0]->GetType());
        return;
      }
      auto type = args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
      switch (type) {
        case ast::BuiltinType::Kind::CountAggregate:
        case ast::BuiltinType::Kind::CountStarAggregate:
        case ast::BuiltinType::Kind::IntegerMaxAggregate:
        case ast::BuiltinType::Kind::IntegerMinAggregate:
        case ast::BuiltinType::Kind::IntegerSumAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Integer));
          break;
        case ast::BuiltinType::Kind::RealMaxAggregate:
        case ast::BuiltinType::Kind::RealMinAggregate:
        case ast::BuiltinType::Kind::RealSumAggregate:
        case ast::BuiltinType::Kind::AvgAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Real));
          break;
        case ast::BuiltinType::Kind::StringMaxAggregate:
        case ast::BuiltinType::Kind::StringMinAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::StringVal));
          break;
        default:
          UNREACHABLE("Impossible aggregate type!");
      }
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregator call");
    }
  }
}

void Sema::CheckBuiltinJoinHashTableInit(ast::CallExpr *call) {
  if (!CheckArgCount(call, 3)) {
    return;
  }

  const auto &args = call->Arguments();

  // First argument must be a pointer to a JoinHashTable
  const auto jht_kind = ast::BuiltinType::JoinHashTable;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), jht_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(jht_kind)->PointerTo());
    return;
  }

  // Second argument is the execution context.
  auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[1]->GetType(), exec_ctx_kind)) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(exec_ctx_kind)->PointerTo());
    return;
  }

  // Third and last argument must be a 32-bit number representing the tuple size
  if (!args[2]->GetType()->IsIntegerType()) {
    ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Uint32));
    return;
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinJoinHashTableInsert(ast::CallExpr *call) {
  if (!CheckArgCount(call, 2)) {
    return;
  }

  const auto &args = call->Arguments();

  // First argument is a pointer to a JoinHashTable
  const auto jht_kind = ast::BuiltinType::JoinHashTable;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), jht_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(jht_kind)->PointerTo());
    return;
  }

  // Second argument is a 64-bit unsigned hash value
  if (!args[1]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Uint64)) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Uint64));
    return;
  }

  // This call returns a byte pointer
  const auto byte_kind = ast::BuiltinType::Uint8;
  call->SetType(GetBuiltinType(byte_kind)->PointerTo());
}

void Sema::CheckBuiltinJoinHashTableGetTupleCount(ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // The first and only argument must be a pointer to a JoinHashTable.
  const auto jht_kind = ast::BuiltinType::JoinHashTable;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), jht_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(jht_kind)->PointerTo());
    return;
  }

  // This call returns the tuple count.
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
}

void Sema::CheckBuiltinJoinHashTableBuild(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // The first argument must be a pointer to a JoinHashTable
  const auto jht_kind = ast::BuiltinType::JoinHashTable;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), jht_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(jht_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::JoinHashTableBuild: {
      break;
    }
    case ast::Builtin::JoinHashTableBuildParallel: {
      if (!CheckArgCount(call, 3)) {
        return;
      }
      // Second argument must be a thread state container pointer
      const auto tls_kind = ast::BuiltinType::ThreadStateContainer;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), tls_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(tls_kind)->PointerTo());
        return;
      }
      // Third argument must be a 32-bit integer representing the offset
      const auto uint32_kind = ast::BuiltinType::Uint32;
      if (!call_args[2]->GetType()->IsSpecificBuiltin(uint32_kind)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(uint32_kind));
        return;
      }
      break;
    }
    default: {
      UNREACHABLE("Impossible join hash table build call");
    }
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinJoinHashTableLookup(ast::CallExpr *call) {
  if (!CheckArgCount(call, 3)) {
    return;
  }

  const auto &args = call->Arguments();

  // First argument must be a pointer to a JoinHashTable
  const auto jht_kind = ast::BuiltinType::JoinHashTable;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), jht_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(jht_kind)->PointerTo());
    return;
  }

  // Second argument is a HashTableEntryIterator
  auto iter_kind = ast::BuiltinType::HashTableEntryIterator;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[1]->GetType(), iter_kind)) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(iter_kind)->PointerTo());
    return;
  }

  // Third argument is a 64-bit unsigned hash value
  if (!args[2]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Uint64)) {
    ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Uint64));
    return;
  }

  call->SetType(GetBuiltinType(ast::BuiltinType::HashTableEntryIterator));
}

void Sema::CheckBuiltinJoinHashTableFree(ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  const auto &args = call->Arguments();

  // The first and only argument must be a pointer to a JoinHashTable
  const auto jht_kind = ast::BuiltinType::JoinHashTable;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), jht_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(jht_kind)->PointerTo());
    return;
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinHashTableEntryIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  // First argument must be the hash table entry iterator
  auto iter_kind = ast::BuiltinType::HashTableEntryIterator;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), iter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(iter_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::HashTableEntryIterHasNext: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::HashTableEntryIterGetRow: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
      break;
    }
    default: {
      UNREACHABLE("Impossible hash table entry iterator call");
    }
  }
}

void Sema::CheckBuiltinJoinHashTableIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &args = call->Arguments();

  const auto ht_iter_kind = ast::BuiltinType::JoinHashTableIterator;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), ht_iter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(ht_iter_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::JoinHashTableIterInit: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      const auto ht_kind = ast::BuiltinType::JoinHashTable;
      if (!IsPointerToSpecificBuiltin(args[1]->GetType(), ht_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ht_kind)->PointerTo());
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::JoinHashTableIterHasNext: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::JoinHashTableIterNext: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::JoinHashTableIterGetRow: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      const auto byte_kind = ast::BuiltinType::Uint8;
      call->SetType(GetBuiltinType(byte_kind)->PointerTo());
      break;
    }
    case ast::Builtin::JoinHashTableIterFree: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible hash table naive iterator call");
    }
  }
}

void Sema::CheckBuiltinExecutionContextCall(ast::CallExpr *call, ast::Builtin builtin) {
  uint32_t expected_arg_count = 1;

  switch (builtin) {
    case ast::Builtin::RegisterThreadWithMetricsManager:
    case ast::Builtin::CheckTrackersStopped:
    case ast::Builtin::AggregateMetricsThread:
    case ast::Builtin::ExecutionContextGetMemoryPool:
    case ast::Builtin::ExecutionContextGetTLS:
    case ast::Builtin::ExecutionContextClearHooks:
      expected_arg_count = 1;
      break;
    case ast::Builtin::ExecutionContextAddRowsAffected:
    case ast::Builtin::ExecutionContextInitHooks:
    case ast::Builtin::ExecutionContextStartResourceTracker:
    case ast::Builtin::ExecutionContextStartPipelineTracker:
    case ast::Builtin::ExecutionContextSetMemoryUseOverride:
    case ast::Builtin::ExecutionContextEndResourceTracker:
      expected_arg_count = 2;
      break;
    case ast::Builtin::ExecutionContextRegisterHook:
      expected_arg_count = 3;
      break;
    case ast::Builtin::ExecutionContextEndPipelineTracker:
      expected_arg_count = 4;
      break;
    case ast::Builtin::ExecOUFeatureVectorInitialize:
      expected_arg_count = 4;
      break;
    default:
      UNREACHABLE("Impossible execution context call");
  }

  if (!CheckArgCount(call, expected_arg_count)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // First argument should be the execution context
  auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), exec_ctx_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(exec_ctx_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::RegisterThreadWithMetricsManager:
    case ast::Builtin::CheckTrackersStopped:
    case ast::Builtin::AggregateMetricsThread: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecutionContextAddRowsAffected: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      // Number of rows affected, can be negative.
      if (!call_args[1]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 1, "Second argument should be an integer type.");
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecutionContextClearHooks: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecutionContextRegisterHook: {
      if (!CheckArgCount(call, 3)) {
        return;
      }

      // Hook Index
      if (!call_args[1]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 1, "Second argument should be an integer type.");
        return;
      }

      auto *hook_fn_type = call_args[2]->GetType()->SafeAs<ast::FunctionType>();
      if (hook_fn_type == nullptr) {
        GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadHookFunction, call_args[2]->GetType());
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecutionContextInitHooks: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      // Number of hooks
      if (!call_args[1]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 1, "Second argument should be an integer type.");
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecutionContextGetMemoryPool: {
      call->SetType(GetBuiltinType(ast::BuiltinType::MemoryPool)->PointerTo());
      break;
    }
    case ast::Builtin::ExecutionContextGetTLS: {
      call->SetType(GetBuiltinType(ast::BuiltinType::ThreadStateContainer)->PointerTo());
      break;
    }
    case ast::Builtin::ExecutionContextEndResourceTracker: {
      // Second argument is a string name
      if (!call_args[1]->GetType()->IsSqlValueType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::StringVal));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecutionContextEndPipelineTracker: {
      // query_id
      if (!call_args[1]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Uint64));
        return;
      }
      // pipeline_id
      if (!call_args[2]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Uint64));
        return;
      }
      auto ou_kind = ast::BuiltinType::ExecOUFeatureVector;
      if (!IsPointerToSpecificBuiltin(call_args[3]->GetType(), ou_kind)) {
        ReportIncorrectCallArg(call, 3, GetBuiltinType(ou_kind)->PointerTo());
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecutionContextStartResourceTracker: {
      // MetricsComponent
      if (!call_args[1]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Uint64));
        return;
      }
      // Init returns nil
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecutionContextSetMemoryUseOverride: {
      if (!call_args[1]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      // Init returns nil
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecutionContextStartPipelineTracker: {
      // Pipeline ID.
      if (!call_args[1]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Uint64));
        return;
      }
      // Init returns nil
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecOUFeatureVectorInitialize: {
      auto ou_kind = ast::BuiltinType::ExecOUFeatureVector;
      auto *outype = call_args[1]->GetType();
      if (!outype->IsPointerType() || !IsPointerToSpecificBuiltin(outype, ou_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ou_kind)->PointerTo());
        return;
      }
      // Pipeline ID.
      if (!call_args[2]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      // is_parallel
      ast::Expr *is_parallel_arg = call_args[3];
      if (is_parallel_arg->GetType()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
        is_parallel_arg =
            ImplCastExprToType(is_parallel_arg, GetBuiltinType(ast::BuiltinType::Bool), ast::CastKind::SqlBoolToBool);
        call->SetArgument(3, is_parallel_arg);
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible execution context call");
    }
  }
}

void Sema::CheckBuiltinExecOUFeatureVectorCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();
  auto ou_kind = ast::BuiltinType::ExecOUFeatureVector;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), ou_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(ou_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::ExecOUFeatureVectorReset: {
      if (!CheckArgCount(call, 1)) {
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecOUFeatureVectorFilter: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // Filter
      if (!call_args[1]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ExecOUFeatureVectorRecordFeature: {
      if (!CheckArgCount(call, 6)) {
        return;
      }
      // Pipeline ID.
      if (!call_args[1]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      // Feature ID.
      if (!call_args[2]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      // Feature attribute.
      if (!call_args[3]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 3, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      // Update Mode
      if (!call_args[4]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 4, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      // call_args[5] is the value to be recorded, currently unchecked.
      // Doesn't return anything.
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible ExecOUFeatureVector call");
    }
  }
}

void Sema::CheckBuiltinThreadStateContainerCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // First argument must be thread state container pointer
  auto tls_kind = ast::BuiltinType::ThreadStateContainer;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), tls_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(tls_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::ThreadStateContainerClear: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ThreadStateContainerGetState: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
      break;
    }
    case ast::Builtin::ThreadStateContainerReset: {
      if (!CheckArgCount(call, 5)) {
        return;
      }
      // Second argument must be an integer size of the state
      const auto uint_kind = ast::BuiltinType::Uint32;
      if (!call_args[1]->GetType()->IsSpecificBuiltin(uint_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(uint_kind));
        return;
      }
      // Third and fourth arguments must be functions
      // TODO(pmenon): More thorough check
      if (!AreAllFunctions(call_args[2]->GetType(), call_args[3]->GetType())) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      // Fifth argument must be a pointer to something or nil
      if (!call_args[4]->GetType()->IsPointerType() && !call_args[4]->GetType()->IsNilType()) {
        ReportIncorrectCallArg(call, 4, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::ThreadStateContainerIterate: {
      if (!CheckArgCount(call, 3)) {
        return;
      }
      // Second argument is a pointer to some context
      if (!call_args[1]->GetType()->IsPointerType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      // Third argument is the iteration function callback
      if (!call_args[2]->GetType()->IsFunctionType()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void Sema::CheckBuiltinTableIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  const auto &call_args = call->Arguments();

  const auto tvi_kind = ast::BuiltinType::TableVectorIterator;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), tvi_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(tvi_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::TableIterInit: {
      if (!CheckArgCount(call, 4)) {
        return;
      }
      // The second argument is the execution context
      auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), exec_ctx_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(exec_ctx_kind)->PointerTo());
        return;
      }
      // The third argument is a table oid
      if (!call_args[2]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 2, "Second argument should be an integer type.");
        return;
      }
      // The fourth argument is a uint32_t array
      if (!call_args[3]->GetType()->IsArrayType()) {
        ReportIncorrectCallArg(call, 3, "Fourth argument should be a fixed length uint32 array");
        return;
      }
      auto *arr_type = call_args[3]->GetType()->SafeAs<ast::ArrayType>();
      if (!arr_type->GetElementType()->IsSpecificBuiltin(ast::BuiltinType::Uint32) || !arr_type->HasKnownLength()) {
        ReportIncorrectCallArg(call, 3, "Fourth argument should be a fixed length uint32 array");
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::TableIterAdvance: {
      // A single-arg builtin returning a boolean
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::TableIterGetVPINumTuples: {
      // A single-arg builtin returning the number of tuples in the table's current VPI.
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
      break;
    }
    case ast::Builtin::TableIterGetVPI: {
      // A single-arg builtin return a pointer to the current VPI
      const auto vpi_kind = ast::BuiltinType::VectorProjectionIterator;
      call->SetType(GetBuiltinType(vpi_kind)->PointerTo());
      break;
    }
    case ast::Builtin::TableIterClose: {
      // A single-arg builtin returning void
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void Sema::CheckBuiltinTableIterParCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 5)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // The first argument is a table oid.
  if (!call_args[0]->GetType()->IsIntegerType()) {
    ReportIncorrectCallArg(call, 0, "First argument should be an integer type.");
    return;
  }

  // The second argument is a uint32_t array.
  if (!call_args[1]->GetType()->IsArrayType()) {
    ReportIncorrectCallArg(call, 1, "Second argument should be a fixed length uint32 array.");
    return;
  }
  auto *arr_type = call_args[1]->GetType()->SafeAs<ast::ArrayType>();
  if (!arr_type->GetElementType()->IsSpecificBuiltin(ast::BuiltinType::Uint32) || !arr_type->HasKnownLength()) {
    ReportIncorrectCallArg(call, 1, "Second argument should be a fixed length uint32 array");
  }

  // The third argument is an opaque query state. For now, check it's a pointer.
  const auto void_kind = ast::BuiltinType::Nil;
  if (!call_args[2]->GetType()->IsPointerType()) {
    ReportIncorrectCallArg(call, 2, GetBuiltinType(void_kind)->PointerTo());
    return;
  }

  // The fourth argument must be a pointer to the execution context.
  const auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[3]->GetType(), exec_ctx_kind)) {
    ReportIncorrectCallArg(call, 3, GetBuiltinType(exec_ctx_kind)->PointerTo());
    return;
  }

  // The fifth argument is the scanner function.
  auto *scan_fn_type = call_args[4]->GetType()->SafeAs<ast::FunctionType>();
  if (scan_fn_type == nullptr) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadParallelScanFunction, call_args[4]->GetType());
    return;
  }
  // Check the type of the scanner function parameters. See TableVectorIterator::ScanFn.
  const auto tvi_kind = ast::BuiltinType::TableVectorIterator;
  const auto &params = scan_fn_type->GetParams();
  if (params.size() != 3                                            // Scan function has 3 arguments.
      || !params[0].type_->IsPointerType()                          // QueryState, must contain execCtx.
      || !params[1].type_->IsPointerType()                          // Thread state.
      || !IsPointerToSpecificBuiltin(params[2].type_, tvi_kind)) {  // TableVectorIterator.
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadParallelScanFunction, call_args[4]->GetType());
    return;
  }

  // This builtin does not return a value.
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinVPICall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // The first argument must be a *VPI
  const auto vpi_kind = ast::BuiltinType::VectorProjectionIterator;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), vpi_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(vpi_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::VPIInit: {
      if (!CheckArgCountAtLeast(call, 2)) {
        return;
      }

      // The second argument must be a *VectorProjection
      const auto vp_kind = ast::BuiltinType::VectorProjection;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), vp_kind)) {
        ReportIncorrectCallArg(call, 0, GetBuiltinType(vp_kind)->PointerTo());
        return;
      }

      // The third optional argument must be a *TupleIdList
      const auto tid_list_kind = ast::BuiltinType::TupleIdList;
      if (call_args.size() > 2 && !IsPointerToSpecificBuiltin(call_args[2]->GetType(), tid_list_kind)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(tid_list_kind)->PointerTo());
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::VPIFree: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::VPIIsFiltered:
    case ast::Builtin::VPIHasNext:
    case ast::Builtin::VPIHasNextFiltered:
    case ast::Builtin::VPIAdvance:
    case ast::Builtin::VPIAdvanceFiltered:
    case ast::Builtin::VPIReset:
    case ast::Builtin::VPIResetFiltered: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::VPIGetSelectedRowCount: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
      break;
    }
    case ast::Builtin::VPIGetVectorProjection: {
      call->SetType(GetBuiltinType(ast::BuiltinType::VectorProjection)->PointerTo());
      break;
    }
    case ast::Builtin::VPISetPosition:
    case ast::Builtin::VPISetPositionFiltered: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      auto unsigned_kind = ast::BuiltinType::Uint32;
      if (!call_args[1]->GetType()->IsSpecificBuiltin(unsigned_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(unsigned_kind));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::VPIMatch: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // If the match argument is a SQL boolean, implicitly cast to native
      ast::Expr *match_arg = call_args[1];
      if (match_arg->GetType()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
        match_arg = ImplCastExprToType(match_arg, GetBuiltinType(ast::BuiltinType::Bool), ast::CastKind::SqlBoolToBool);
        call->SetArgument(1, match_arg);
      }
      // If the match argument isn't a native boolean , error
      if (!match_arg->GetType()->IsBoolType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Bool));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::VPIGetSlot: {
      call->SetType(GetBuiltinType(ast::BuiltinType::TupleSlot));
      break;
    }
    case ast::Builtin::VPIGetBool:
    case ast::Builtin::VPIGetBoolNull: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Boolean));
      break;
    }
    case ast::Builtin::VPIGetTinyInt:
    case ast::Builtin::VPIGetTinyIntNull:
    case ast::Builtin::VPIGetSmallInt:
    case ast::Builtin::VPIGetSmallIntNull:
    case ast::Builtin::VPIGetInt:
    case ast::Builtin::VPIGetIntNull:
    case ast::Builtin::VPIGetBigInt:
    case ast::Builtin::VPIGetBigIntNull: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Integer));
      break;
    }
    case ast::Builtin::VPIGetReal:
    case ast::Builtin::VPIGetRealNull:
    case ast::Builtin::VPIGetDouble:
    case ast::Builtin::VPIGetDoubleNull: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Real));
      break;
    }
    case ast::Builtin::VPIGetDate:
    case ast::Builtin::VPIGetDateNull: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Date));
      break;
    }
    case ast::Builtin::VPIGetTimestamp:
    case ast::Builtin::VPIGetTimestampNull: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Timestamp));
      break;
    }
    case ast::Builtin::VPIGetString:
    case ast::Builtin::VPIGetStringNull: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::StringVal));
      break;
    }
    case ast::Builtin::VPIGetPointer: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
      break;
    }
    case ast::Builtin::VPISetSmallInt:
    case ast::Builtin::VPISetSmallIntNull:
    case ast::Builtin::VPISetInt:
    case ast::Builtin::VPISetIntNull:
    case ast::Builtin::VPISetBigInt:
    case ast::Builtin::VPISetBigIntNull:
    case ast::Builtin::VPISetReal:
    case ast::Builtin::VPISetRealNull:
    case ast::Builtin::VPISetDouble:
    case ast::Builtin::VPISetDoubleNull:
    case ast::Builtin::VPISetDate:
    case ast::Builtin::VPISetDateNull:
    case ast::Builtin::VPISetTimestamp:
    case ast::Builtin::VPISetTimestampNull:
    case ast::Builtin::VPISetString:
    case ast::Builtin::VPISetStringNull: {
      if (!CheckArgCount(call, 3)) {
        return;
      }
      ast::BuiltinType::Kind sql_kind;
      switch (builtin) {
        case ast::Builtin::VPISetReal:
        case ast::Builtin::VPISetRealNull:
        case ast::Builtin::VPISetDouble:
        case ast::Builtin::VPISetDoubleNull: {
          sql_kind = ast::BuiltinType::Real;
          break;
        }
        case ast::Builtin::VPISetDate:
        case ast::Builtin::VPISetDateNull: {
          sql_kind = ast::BuiltinType::Date;
          break;
        }
        case ast::Builtin::VPISetTimestamp:
        case ast::Builtin::VPISetTimestampNull: {
          sql_kind = ast::BuiltinType::Timestamp;
          break;
        }
        default: {
          sql_kind = ast::BuiltinType::Integer;
          break;
        }
      }
      if (!call_args[1]->GetType()->IsSpecificBuiltin(sql_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(sql_kind));
        return;
      }
      // Third argument must be an integer
      const auto int32_kind = ast::BuiltinType::Int32;
      if (!call_args[2]->GetType()->IsSpecificBuiltin(int32_kind)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(int32_kind));
        return;
      }
      break;
    }
    default: {
      UNREACHABLE("Impossible VPI call");
    }
  }
}

void Sema::CheckBuiltinHashCall(ast::CallExpr *call, UNUSED_ATTRIBUTE ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  // All arguments must be SQL types
  for (const auto &arg : call->Arguments()) {
    if (!arg->GetType()->IsSqlValueType()) {
      GetErrorReporter()->Report(arg->Position(), ErrorMessages::kBadHashArg, arg->GetType());
      return;
    }
  }

  // Result is a hash value
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint64));
}

void Sema::CheckBuiltinFilterManagerCall(ast::CallExpr *const call, const ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  // The first argument must be a *FilterManagerBuilder
  const auto fm_kind = ast::BuiltinType::FilterManager;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), fm_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(fm_kind)->PointerTo());
    return;
  }

  const auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  switch (builtin) {
    case ast::Builtin::FilterManagerInit: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // The second argument must be a pointer to the execution context.
      if (!IsPointerToSpecificBuiltin(call->Arguments()[1]->GetType(), exec_ctx_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(exec_ctx_kind)->PointerTo());
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::FilterManagerFree: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::FilterManagerInsertFilter: {
      for (uint32_t arg_idx = 1; arg_idx < call->NumArgs(); arg_idx++) {
        const auto vector_proj_kind = ast::BuiltinType::VectorProjection;
        const auto tid_list_kind = ast::BuiltinType::TupleIdList;
        auto *arg_type = call->Arguments()[arg_idx]->GetType()->SafeAs<ast::FunctionType>();
        if (arg_type == nullptr || arg_type->GetNumParams() != 4 ||
            !IsPointerToSpecificBuiltin(arg_type->GetParams()[0].type_, exec_ctx_kind) ||
            !IsPointerToSpecificBuiltin(arg_type->GetParams()[1].type_, vector_proj_kind) ||
            !IsPointerToSpecificBuiltin(arg_type->GetParams()[2].type_, tid_list_kind) ||
            !arg_type->GetParams()[3].type_->IsPointerType()) {
          ReportIncorrectCallArg(call, arg_idx, "(*ExecutionContext, *VectorProjection, *TupleIdList, *uint8)->nil");
          return;
        }
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::FilterManagerRunFilters: {
      if (!CheckArgCount(call, 3)) {
        return;
      }

      const auto vpi_kind = ast::BuiltinType::VectorProjectionIterator;
      if (!IsPointerToSpecificBuiltin(call->Arguments()[1]->GetType(), vpi_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(vpi_kind)->PointerTo());
        return;
      }
      if (!IsPointerToSpecificBuiltin(call->Arguments()[2]->GetType(), exec_ctx_kind)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(exec_ctx_kind)->PointerTo());
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible FilterManager call");
    }
  }
}

void Sema::CheckBuiltinVectorFilterCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 5)) {
    return;
  }

  // The first argument must be a *ExecutionContext.
  const auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), exec_ctx_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(exec_ctx_kind)->PointerTo());
    return;
  }

  // The second argument must be a *VectorProjection.
  const auto vector_proj_kind = ast::BuiltinType::VectorProjection;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[1]->GetType(), vector_proj_kind)) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(vector_proj_kind)->PointerTo());
    return;
  }

  // The third argument is the column index.
  const auto &call_args = call->Arguments();
  const auto int32_kind = ast::BuiltinType::Int32;
  const auto uint32_kind = ast::BuiltinType::Uint32;
  if (!call_args[2]->GetType()->IsSpecificBuiltin(int32_kind) &&
      !call_args[2]->GetType()->IsSpecificBuiltin(uint32_kind)) {
    ReportIncorrectCallArg(call, 2, GetBuiltinType(int32_kind));
    return;
  }

  // The fourth argument is either an integer or a pointer to a generic value.
  if (!call_args[3]->GetType()->IsSpecificBuiltin(int32_kind) && !call_args[3]->GetType()->IsSqlValueType()) {
    ReportIncorrectCallArg(call, 3, GetBuiltinType(int32_kind));
    return;
  }

  // The fifth and last argument is the *TupleIdList.
  const auto tid_list_kind = ast::BuiltinType::TupleIdList;
  if (!IsPointerToSpecificBuiltin(call_args[4]->GetType(), tid_list_kind)) {
    ReportIncorrectCallArg(call, 4, GetBuiltinType(tid_list_kind)->PointerTo());
    return;
  }

  // Done
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckMathTrigCall(ast::CallExpr *call, ast::Builtin builtin) {
  const auto real_kind = ast::BuiltinType::Real;
  const auto int_kind = ast::BuiltinType::Integer;
  auto return_kind = real_kind;

  const auto &call_args = call->Arguments();
  switch (builtin) {
    case ast::Builtin::ATan2:
    case ast::Builtin::Pow: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      if (!call_args[0]->GetType()->IsSpecificBuiltin(real_kind) ||
          !call_args[1]->GetType()->IsSpecificBuiltin(real_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(real_kind));
        return;
      }
      break;
    }
    case ast::Builtin::Exp: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      if (!call_args[1]->GetType()->IsSpecificBuiltin(real_kind)) {
        ReportIncorrectCallArg(call, 0, GetBuiltinType(real_kind));
        return;
      }
      break;
    }
    case ast::Builtin::Cos:
    case ast::Builtin::Cot:
    case ast::Builtin::Sin:
    case ast::Builtin::Tan:
    case ast::Builtin::Cosh:
    case ast::Builtin::Sinh:
    case ast::Builtin::Tanh:
    case ast::Builtin::ACos:
    case ast::Builtin::ASin:
    case ast::Builtin::ATan:
    case ast::Builtin::Ceil:
    case ast::Builtin::Floor:
    case ast::Builtin::Truncate:
    case ast::Builtin::Log10:
    case ast::Builtin::Log2:
    case ast::Builtin::Sqrt:
    case ast::Builtin::Cbrt:
    case ast::Builtin::Round: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      if (!call_args[0]->GetType()->IsSpecificBuiltin(real_kind)) {
        ReportIncorrectCallArg(call, 0, GetBuiltinType(real_kind));
        return;
      }
      break;
    }
    case ast::Builtin::Abs: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      if (!call_args[0]->GetType()->IsArithmetic()) {
        // TODO(jkosh44): would be nice to be able to provide multiple types
        // to ReportIncorrectCallArg to indicate multiple valid types
        ReportIncorrectCallArg(call, 0, GetBuiltinType(real_kind));
        return;
      }
      if (call->Arguments()[0]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
        return_kind = ast::BuiltinType::Integer;
      }
      break;
    }
    case ast::Builtin::Round2: {
      // input arguments may include decimal places
      if (!CheckArgCount(call, 2)) {
        return;
      }
      if (!call_args[0]->GetType()->IsSpecificBuiltin(real_kind)) {
        ReportIncorrectCallArg(call, 0, GetBuiltinType(real_kind));
        return;
      }
      // check to make sure the decimal_places argument is an integer
      if (!call_args[1]->GetType()->IsSpecificBuiltin(int_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(int_kind));
        return;
      }
      break;
    }
    case ast::Builtin::Mod: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      auto first_operand_type = call_args[0]->GetType();
      auto second_operand_type = call_args[1]->GetType();

      bool first_operand_type_correct = first_operand_type->IsArithmetic();
      bool second_operand_type_correct = second_operand_type->IsArithmetic();

      // TODO(jkosh44): would be nice to be able to report real or int as expected type
      if (!first_operand_type_correct && !second_operand_type_correct) {
        ReportIncorrectCallArg(call, 0, GetBuiltinType(real_kind));
        ReportIncorrectCallArg(call, 1, GetBuiltinType(real_kind));
        return;
      }

      if (!first_operand_type_correct) {
        ReportIncorrectCallArg(call, 0, GetBuiltinType(real_kind));
        return;
      }

      if (!second_operand_type_correct) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(real_kind));
        return;
      }

      // If both operands are ints then we return an int, otherwise we return a real
      if (first_operand_type->IsSpecificBuiltin(ast::BuiltinType::Integer) &&
          second_operand_type->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
        return_kind = int_kind;
      }

      break;
    }

    default: {
      UNREACHABLE("Impossible math trig function call");
    }
  }

  call->SetType(GetBuiltinType(return_kind));
}

void Sema::CheckResultBufferCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCount(call, 1)) {
    return;
  }
  if (builtin == ast::Builtin::ResultBufferNew) {
    const auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
    if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), exec_ctx_kind)) {
      ReportIncorrectCallArg(call, 0, GetBuiltinType(exec_ctx_kind)->PointerTo());
      return;
    }
  } else {
    const auto out_buffer_kind = ast::BuiltinType::OutputBuffer;
    if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), out_buffer_kind)) {
      ReportIncorrectCallArg(call, 0, GetBuiltinType(out_buffer_kind)->PointerTo());
      return;
    }
  }

  if (builtin == ast::Builtin::ResultBufferAllocOutRow) {
    call->SetType(ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Uint8)->PointerTo());
  } else if (builtin == ast::Builtin::ResultBufferNew) {
    call->SetType(ast::BuiltinType::Get(GetContext(), ast::BuiltinType::OutputBuffer)->PointerTo());
  } else {
    call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
  }
}

/*
void Sema::CheckCSVReaderCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // First argument must be a *CSVReader.
  const auto csv_reader = ast::BuiltinType::CSVReader;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), csv_reader)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(csv_reader));
    return;
  }

  switch (builtin) {
    case ast::Builtin::CSVReaderInit: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      // Second argument is either a raw string, or a string representing the
      // name of the CSV file to read. At this stage, we don't care. It just
      // needs to be a string.
      if (!call_args[1]->GetType()->IsStringType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(csv_reader));
        return;
      }

      // Third, fourth, and fifth must be characters.

      // Returns boolean indicating if initialization succeeded.
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::CSVReaderAdvance: {
      // Returns a boolean indicating if there's more data.
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::CSVReaderGetField: {
      if (!CheckArgCount(call, 3)) {
        return;
      }
      // Second argument must be the index, third is a pointer to a SQL string.
      if (!call_args[1]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Uint32));
      }
      // Second argument must be the index, third is a pointer to a SQL string.
      const auto string_kind = ast::BuiltinType::StringVal;
      if (!IsPointerToSpecificBuiltin(call_args[2]->GetType(), string_kind)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(string_kind)->PointerTo());
      }
      // Returns nothing.
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::CSVReaderGetRecordNumber: {
      // Returns a 32-bit number indicating the current record number.
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
      break;
    }
    case ast::Builtin::CSVReaderClose: {
      // Returns nothing.
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default:
      UNREACHABLE("Impossible math trig function call");
  }
}
 */

void Sema::CheckBuiltinSizeOfCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  // This call returns an unsigned 32-bit value for the size of the type
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
}

void Sema::CheckBuiltinOffsetOfCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 2)) {
    return;
  }

  // First argument must be a resolved composite type
  auto *type = Resolve(call->Arguments()[0]);
  if (type == nullptr || !type->IsStructType()) {
    ReportIncorrectCallArg(call, 0, "composite");
    return;
  }

  // Second argument must be an identifier expression
  auto field = call->Arguments()[1]->SafeAs<ast::IdentifierExpr>();
  if (field == nullptr) {
    ReportIncorrectCallArg(call, 1, "identifier expression");
    return;
  }

  // Field with the given name must exist in the composite type
  if (type->As<ast::StructType>()->LookupFieldByName(field->Name()) == nullptr) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kFieldObjectDoesNotExist, field->Name(), type);
    return;
  }

  // Returns a 32-bit value for the offset of the type
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
}

void Sema::CheckBuiltinPtrCastCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 2)) {
    return;
  }

  // The first argument will be a UnaryOpExpr with the '*' (star) op. This is
  // because parsing function calls assumes expression arguments, not types. So,
  // something like '*Type', which would be the first argument to @ptrCast, will
  // get parsed as a dereference expression before a type expression.
  // TODO(pmenon): Fix the above to parse correctly

  auto unary_op = call->Arguments()[0]->SafeAs<ast::UnaryOpExpr>();
  if (unary_op == nullptr || unary_op->Op() != parsing::Token::Type::STAR) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadArgToPtrCast, call->Arguments()[0]->GetType(), 1);
    return;
  }

  // Replace the unary with a PointerTypeRepr node and resolve it
  call->SetArgument(
      0, GetContext()->GetNodeFactory()->NewPointerType(call->Arguments()[0]->Position(), unary_op->Input()));

  for (auto *arg : call->Arguments()) {
    auto *resolved_type = Resolve(arg);
    if (resolved_type == nullptr) {
      return;
    }
  }

  // Both arguments must be pointer types
  if (!call->Arguments()[0]->GetType()->IsPointerType() || !call->Arguments()[1]->GetType()->IsPointerType()) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadArgToPtrCast, call->Arguments()[0]->GetType(), 1);
    return;
  }

  // Apply the cast
  call->SetType(call->Arguments()[0]->GetType());
}

void Sema::CheckBuiltinSorterInit(ast::CallExpr *call) {
  if (!CheckArgCount(call, 4)) {
    return;
  }

  const auto &args = call->Arguments();

  // First argument must be a pointer to a Sorter
  const auto sorter_kind = ast::BuiltinType::Sorter;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), sorter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(sorter_kind)->PointerTo());
    return;
  }

  // Second argument must be a pointer to a ExecutionContext
  const auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  if (!IsPointerToSpecificBuiltin(args[1]->GetType(), exec_ctx_kind)) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(exec_ctx_kind)->PointerTo());
    return;
  }

  // Third argument must be a function
  auto *const cmp_func_type = args[2]->GetType()->SafeAs<ast::FunctionType>();
  if (cmp_func_type == nullptr || cmp_func_type->GetNumParams() != 2 ||
      !cmp_func_type->GetReturnType()->IsSpecificBuiltin(ast::BuiltinType::Int32) ||
      !cmp_func_type->GetParams()[0].type_->IsPointerType() || !cmp_func_type->GetParams()[1].type_->IsPointerType()) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadComparisonFunctionForSorter, args[2]->GetType());
    return;
  }

  // Fourth and last argument must be a 32-bit number representing the tuple size
  const auto uint_kind = ast::BuiltinType::Uint32;
  if (!args[3]->GetType()->IsSpecificBuiltin(uint_kind)) {
    ReportIncorrectCallArg(call, 3, GetBuiltinType(uint_kind));
    return;
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinSorterGetTupleCount(ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  const auto &args = call->Arguments();

  // First argument must be a pointer to a Sorter
  const auto sorter_kind = ast::BuiltinType::Sorter;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), sorter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(sorter_kind)->PointerTo());
    return;
  }

  call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
}

void Sema::CheckBuiltinSorterInsert(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  // First argument must be a pointer to a Sorter
  const auto sorter_kind = ast::BuiltinType::Sorter;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), sorter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(sorter_kind)->PointerTo());
    return;
  }

  // If it's an insertion for Top-K, the second argument must be an unsigned integer.
  if (builtin == ast::Builtin::SorterInsertTopK || builtin == ast::Builtin::SorterInsertTopKFinish) {
    if (!CheckArgCount(call, 2)) {
      return;
    }

    // Error if the top-k argument isn't an integer
    ast::Type *uint_type = GetBuiltinType(ast::BuiltinType::Uint32);
    if (!call->Arguments()[1]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 1, uint_type);
      return;
    }
    if (call->Arguments()[1]->GetType() != uint_type) {
      call->SetArgument(1, ImplCastExprToType(call->Arguments()[1], uint_type, ast::CastKind::IntegralCast));
    }
  } else {
    // Regular sorter insert, expect one argument.
    if (!CheckArgCount(call, 1)) {
      return;
    }
  }

  // This call returns a pointer to the allocated tuple
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
}

void Sema::CheckBuiltinSorterSort(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // First argument must be a pointer to a Sorter
  const auto sorter_kind = ast::BuiltinType::Sorter;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), sorter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(sorter_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::SorterSort: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      break;
    }
    case ast::Builtin::SorterSortParallel:
    case ast::Builtin::SorterSortTopKParallel: {
      // Second argument is the *ThreadStateContainer.
      const auto tls_kind = ast::BuiltinType::ThreadStateContainer;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), tls_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(tls_kind)->PointerTo());
        return;
      }

      // Third argument must be a 32-bit integer representing the offset.
      ast::Type *uint_type = GetBuiltinType(ast::BuiltinType::Uint32);
      if (call_args[2]->GetType() != uint_type) {
        ReportIncorrectCallArg(call, 2, uint_type);
        return;
      }

      // If it's for top-k, the last argument must be the top-k value
      if (builtin == ast::Builtin::SorterSortParallel) {
        if (!CheckArgCount(call, 3)) {
          return;
        }
      } else {
        if (!CheckArgCount(call, 4)) {
          return;
        }
        if (!call_args[3]->GetType()->IsIntegerType()) {
          ReportIncorrectCallArg(call, 3, uint_type);
          return;
        }
        if (call_args[3]->GetType() != uint_type) {
          call->SetArgument(3, ImplCastExprToType(call_args[3], uint_type, ast::CastKind::IntegralCast));
        }
      }
      break;
    }
    default: {
      UNREACHABLE("Impossible sorter sort call");
    }
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinSorterFree(ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  // First argument must be a pointer to a Sorter
  const auto sorter_kind = ast::BuiltinType::Sorter;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), sorter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(sorter_kind)->PointerTo());
    return;
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinSorterIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &args = call->Arguments();

  const auto sorter_iter_kind = ast::BuiltinType::SorterIterator;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), sorter_iter_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(sorter_iter_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::SorterIterInit: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      // The second argument is the sorter instance to iterate over
      const auto sorter_kind = ast::BuiltinType::Sorter;
      if (!IsPointerToSpecificBuiltin(args[1]->GetType(), sorter_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(sorter_kind)->PointerTo());
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::SorterIterHasNext: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::SorterIterNext: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::SorterIterSkipRows: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      const auto uint_kind = ast::BuiltinType::Kind::Uint32;
      if (!args[1]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(uint_kind));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::SorterIterGetRow: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
      break;
    }
    case ast::Builtin::SorterIterClose: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void Sema::CheckBuiltinIndexIteratorInit(execution::ast::CallExpr *call, ast::Builtin builtin) {
  // First argument must be a pointer to a IndexIterator
  const auto index_kind = ast::BuiltinType::IndexIterator;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), index_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(index_kind)->PointerTo());
    return;
  }
  switch (builtin) {
    case ast::Builtin::IndexIteratorInit: {
      if (!CheckArgCount(call, 6)) {
        return;
      }
      // The second argument is an execution context
      auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
      if (!IsPointerToSpecificBuiltin(call->Arguments()[1]->GetType(), exec_ctx_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(exec_ctx_kind)->PointerTo());
        return;
      }
      // The third argument is num_attrs
      if (!call->Arguments()[2]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Int32));
        return;
      }
      // The fourth argument is a table oid
      if (!call->Arguments()[3]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 3, GetBuiltinType(ast::BuiltinType::Int32));
        return;
      }
      // The fifth argument is an index oid
      if (!call->Arguments()[4]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 4, GetBuiltinType(ast::BuiltinType::Int32));
        return;
      }
      // The sixth argument is a uint32_t array
      if (!call->Arguments()[5]->GetType()->IsArrayType()) {
        ReportIncorrectCallArg(call, 5, "Sixth argument should be a fixed length uint32 array");
        return;
      }
      auto *arr_type = call->Arguments()[5]->GetType()->SafeAs<ast::ArrayType>();
      auto uint32_t_kind = ast::BuiltinType::Uint32;
      if (!arr_type->GetElementType()->IsSpecificBuiltin(uint32_t_kind) || !arr_type->HasKnownLength()) {
        ReportIncorrectCallArg(call, 5, "Sixth argument should be a fixed length uint32 array");
      }
      break;
    }
    default:
      UNREACHABLE("Unreachable index iterator in builtin");
  }

  // Return nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinIndexIteratorGetSize(execution::ast::CallExpr *call) {
  // First argument must be a pointer to an IndexIterator
  const auto index_kind = ast::BuiltinType::IndexIterator;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), index_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(index_kind)->PointerTo());
    return;
  }
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
}

void Sema::CheckBuiltinIndexIteratorScan(execution::ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }
  // First argument must be a pointer to a IndexIterator
  auto index_kind = ast::BuiltinType::IndexIterator;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), index_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(index_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::IndexIteratorScanKey:
    case ast::Builtin::IndexIteratorScanDescending: {
      if (!CheckArgCount(call, 1)) return;
      break;
    }
    case ast::Builtin::IndexIteratorScanAscending: {
      if (!CheckArgCount(call, 3)) return;
      break;
    }
    case ast::Builtin::IndexIteratorScanLimitDescending: {
      if (!CheckArgCount(call, 2)) return;
      auto uint32_kind = ast::BuiltinType::Uint32;
      // Second argument is an integer
      if (!call->Arguments()[1]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(uint32_kind));
        return;
      }
      break;
    }
    default:
      UNREACHABLE("Impossible Scan call!");
  }
  // Return nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinIndexIteratorAdvance(execution::ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }
  // First argument must be a pointer to a IndexIterator
  auto index_kind = ast::BuiltinType::IndexIterator;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), index_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(index_kind)->PointerTo());
    return;
  }

  // Return boolean
  call->SetType(ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Bool));
}

void Sema::CheckBuiltinIndexIteratorFree(execution::ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }
  // First argument must be a pointer to a IndexIterator
  auto *index_type = call->Arguments()[0]->GetType()->GetPointeeType();
  if (index_type == nullptr || !index_type->IsSpecificBuiltin(ast::BuiltinType::IndexIterator)) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadArgToIndexIteratorFree,
                               call->Arguments()[0]->GetType(), 0);
    return;
  }
  // Return nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinIndexIteratorPRCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCount(call, 1)) {
    return;
  }
  // First argument must be a pointer to a IndexIterator
  auto *index_type = call->Arguments()[0]->GetType()->GetPointeeType();
  if (index_type == nullptr || !index_type->IsSpecificBuiltin(ast::BuiltinType::IndexIterator)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(ast::BuiltinType::IndexIterator)->PointerTo());
    return;
  }
  switch (builtin) {
    case ast::Builtin::IndexIteratorGetPR:
    case ast::Builtin::IndexIteratorGetLoPR:
    case ast::Builtin::IndexIteratorGetHiPR:
    case ast::Builtin::IndexIteratorGetTablePR:
      call->SetType(GetBuiltinType(ast::BuiltinType::ProjectedRow)->PointerTo());
      break;
    case ast::Builtin::IndexIteratorGetSlot:
      call->SetType(GetBuiltinType(ast::BuiltinType::TupleSlot));
      break;
    default:
      UNREACHABLE("Impossible Index PR call!");
  }
}

void Sema::CheckBuiltinPRCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 2)) {
    return;
  }
  // Calls to set varlen take an extra boolean to indicate ownership.
  bool is_set_varlen = false;
  bool is_set_call = false;
  // Type of the input or output sql value
  ast::BuiltinType::Kind sql_type;
  switch (builtin) {
    case ast::Builtin::PRSetBool:
    case ast::Builtin::PRSetBoolNull: {
      is_set_call = true;
      sql_type = ast::BuiltinType::Boolean;
      break;
    }
    case ast::Builtin::PRSetTinyInt:
    case ast::Builtin::PRSetSmallInt:
    case ast::Builtin::PRSetInt:
    case ast::Builtin::PRSetBigInt:
    case ast::Builtin::PRSetTinyIntNull:
    case ast::Builtin::PRSetSmallIntNull:
    case ast::Builtin::PRSetIntNull:
    case ast::Builtin::PRSetBigIntNull: {
      is_set_call = true;
      sql_type = ast::BuiltinType::Integer;
      break;
    }
    case ast::Builtin::PRSetReal:
    case ast::Builtin::PRSetDouble:
    case ast::Builtin::PRSetRealNull:
    case ast::Builtin::PRSetDoubleNull: {
      is_set_call = true;
      sql_type = ast::BuiltinType::Real;
      break;
    }
    case ast::Builtin::PRSetDate:
    case ast::Builtin::PRSetDateNull: {
      is_set_call = true;
      sql_type = ast::BuiltinType::Date;
      break;
    }
    case ast::Builtin::PRSetTimestamp:
    case ast::Builtin::PRSetTimestampNull: {
      is_set_call = true;
      sql_type = ast::BuiltinType::Timestamp;
      break;
    }
    case ast::Builtin::PRSetVarlen:
    case ast::Builtin::PRSetVarlenNull: {
      is_set_call = true;
      is_set_varlen = true;
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    case ast::Builtin::PRGetBool:
    case ast::Builtin::PRGetBoolNull: {
      sql_type = ast::BuiltinType::Boolean;
      break;
    }
    case ast::Builtin::PRGetTinyInt:
    case ast::Builtin::PRGetSmallInt:
    case ast::Builtin::PRGetInt:
    case ast::Builtin::PRGetBigInt:
    case ast::Builtin::PRGetTinyIntNull:
    case ast::Builtin::PRGetSmallIntNull:
    case ast::Builtin::PRGetIntNull:
    case ast::Builtin::PRGetBigIntNull: {
      sql_type = ast::BuiltinType::Integer;
      break;
    }
    case ast::Builtin::PRGetReal:
    case ast::Builtin::PRGetDouble:
    case ast::Builtin::PRGetRealNull:
    case ast::Builtin::PRGetDoubleNull: {
      sql_type = ast::BuiltinType::Real;
      break;
    }
    case ast::Builtin::PRGetDate:
    case ast::Builtin::PRGetDateNull: {
      sql_type = ast::BuiltinType::Date;
      break;
    }
    case ast::Builtin::PRGetTimestamp:
    case ast::Builtin::PRGetTimestampNull: {
      sql_type = ast::BuiltinType::Timestamp;
      break;
    }
    case ast::Builtin::PRGetVarlen:
    case ast::Builtin::PRGetVarlenNull: {
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    default:
      UNREACHABLE("Undefined projected row call!!");
  }

  // First argument must be a pointer to a ProjectedRow
  auto pr_kind = ast::BuiltinType::ProjectedRow;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), pr_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(pr_kind)->PointerTo());
    return;
  }
  // Second argument must be an integer literal
  if (!call->Arguments()[1]->GetType()->IsIntegerType()) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Int32));
    return;
  }
  if (is_set_call) {
    if (!CheckArgCount(call, is_set_varlen ? 4 : 3)) {
      return;
    }
    // Third argument depends of call
    if (GetBuiltinType(sql_type) != call->Arguments()[2]->GetType()) {
      ReportIncorrectCallArg(call, 2, GetBuiltinType(sql_type));
      return;
    }
    // For varlens, there is a fourth boolean argument.
    if (is_set_varlen) {
      auto bool_kind = ast::BuiltinType::Bool;
      if (!call->Arguments()[3]->GetType()->IsSpecificBuiltin(bool_kind)) {
        ReportIncorrectCallArg(call, 3, GetBuiltinType(bool_kind));
        return;
      }
    }
    // Return nothing
    call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
  } else {
    if (!CheckArgCount(call, 2)) {
      return;
    }
    // Return sql type
    call->SetType(ast::BuiltinType::Get(GetContext(), sql_type));
  }
}

void Sema::CheckBuiltinStorageInterfaceCall(ast::CallExpr *call, ast::Builtin builtin) {
  const auto &call_args = call->Arguments();

  const auto storage_interface_kind = ast::BuiltinType::StorageInterface;
  const auto int32_kind = ast::BuiltinType::Int32;

  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), storage_interface_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(storage_interface_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::StorageInterfaceInit: {
      if (!CheckArgCount(call, 5)) {
        return;
      }

      // exec_ctx
      auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), exec_ctx_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(exec_ctx_kind)->PointerTo());
        return;
      }

      // table_oid
      if (!call_args[2]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(int32_kind));
        return;
      }

      // uint32_t *col_oids
      if (!call_args[3]->GetType()->IsArrayType()) {
        ReportIncorrectCallArg(call, 3, "Third argument should be a fixed length uint32 array");
        return;
      }
      auto *arr_type = call_args[3]->GetType()->SafeAs<ast::ArrayType>();
      auto uint32_t_kind = ast::BuiltinType::Uint32;
      if (!arr_type->GetElementType()->IsSpecificBuiltin(uint32_t_kind)) {
        ReportIncorrectCallArg(call, 3, "Third argument should be a fixed length uint32 array");
      }

      // needs_indexes for non-indexed updates
      if (!call_args[4]->GetType()->IsBoolType()) {
        ReportIncorrectCallArg(call, 4, GetBuiltinType(ast::BuiltinType::Bool));
        return;
      }

      // void
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::GetTablePR: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::ProjectedRow)->PointerTo());
      break;
    }
    case ast::Builtin::TableInsert: {
      if (!CheckArgCount(call, 1)) {
        return;
      }

      auto tuple_slot_type = ast::BuiltinType::TupleSlot;
      call->SetType(GetBuiltinType(tuple_slot_type));
      break;
    }
    case ast::Builtin::TableDelete: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      auto tuple_slot_type = ast::BuiltinType::TupleSlot;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), tuple_slot_type)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(tuple_slot_type)->PointerTo());
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::TableUpdate: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      auto tuple_slot_type = ast::BuiltinType::TupleSlot;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), tuple_slot_type)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(tuple_slot_type)->PointerTo());
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::GetIndexPR: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      if (!call_args[1]->GetType()->IsIntegerType()) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(int32_kind));
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::ProjectedRow)->PointerTo());
      break;
    }
    case ast::Builtin::IndexGetSize: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
      break;
    }
    case ast::Builtin::StorageInterfaceGetIndexHeapSize: {
      if (!CheckArgCount(call, 1)) {
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
      break;
    }
    case ast::Builtin::IndexInsert: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::IndexInsertUnique: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::IndexInsertWithSlot: {
      if (!CheckArgCount(call, 3)) {
        return;
      }
      // Second argument is a tuple slot
      auto tuple_slot_type = ast::BuiltinType::TupleSlot;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), tuple_slot_type)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(tuple_slot_type)->PointerTo());
        return;
      }
      // Third argument is an bool
      if (!call_args[2]->GetType()->IsBoolType()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Bool));
        return;
      }

      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::IndexDelete: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // Second argument is a tuple slot
      auto tuple_slot_type = ast::BuiltinType::TupleSlot;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), tuple_slot_type)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(tuple_slot_type)->PointerTo());
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::StorageInterfaceFree: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      // Return nothing
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    default:
      UNREACHABLE("Undefined updater call!");
  }
}

void Sema::CheckBuiltinAbortCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), ast::BuiltinType::ExecutionContext)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(ast::BuiltinType::ExecutionContext)->PointerTo());
    return;
  }

  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinParamCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCount(call, 2)) {
    return;
  }

  // first argument is an exec ctx
  auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), exec_ctx_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(exec_ctx_kind)->PointerTo());
    return;
  }

  // second argument is the index of the parameter
  if (!call->Arguments()[1]->GetType()->IsIntegerType()) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(ast::BuiltinType::Kind::Uint32));
    return;
  }

  // Type output sql value
  ast::BuiltinType::Kind sql_type;
  switch (builtin) {
    case ast::Builtin::GetParamBool: {
      sql_type = ast::BuiltinType::Boolean;
      break;
    }
    case ast::Builtin::GetParamTinyInt:
    case ast::Builtin::GetParamSmallInt:
    case ast::Builtin::GetParamInt:
    case ast::Builtin::GetParamBigInt: {
      sql_type = ast::BuiltinType::Integer;
      break;
    }
    case ast::Builtin::GetParamReal:
    case ast::Builtin::GetParamDouble: {
      sql_type = ast::BuiltinType::Real;
      break;
    }
    case ast::Builtin::GetParamDate: {
      sql_type = ast::BuiltinType::Date;
      break;
    }
    case ast::Builtin::GetParamTimestamp: {
      sql_type = ast::BuiltinType::Timestamp;
      break;
    }
    case ast::Builtin::GetParamString: {
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    default:
      UNREACHABLE("Undefined parameter call!!");
  }

  // Return sql type
  call->SetType(ast::BuiltinType::Get(GetContext(), sql_type));
}

void Sema::CheckBuiltinStringCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::BuiltinType::Kind sql_type;

  // Checking to see if the first argument is an execution context
  // All string functions have the execution context as their first argument
  auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), exec_ctx_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(exec_ctx_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::SplitPart: {
      // check to make sure this function has four arguments
      if (!CheckArgCount(call, 4)) {
        return;
      }

      // checking to see if the second argument is a string
      if (!call->Arguments()[1]->GetType()->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(GetContext()));
        return;
      }

      // checking to see if the third argument is a string
      if (!call->Arguments()[2]->GetType()->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 2, ast::StringType::Get(GetContext()));
        return;
      }

      // checking to see if the forth argument is an integer
      if (!call->Arguments()[3]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
        ReportIncorrectCallArg(call, 3, GetBuiltinType(ast::BuiltinType::Int32));
        return;
      }

      // this function returns a string
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    case ast::Builtin::Chr: {
      // check to make sure this function has two arguments
      if (!CheckArgCount(call, 2)) {
        return;
      }

      // checking to see if the second argument is a number
      auto *resolved_type = call->Arguments()[1]->GetType();
      if (!resolved_type->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(GetContext()));
        return;
      }

      // this function returns a string
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    case ast::Builtin::ASCII:
    case ast::Builtin::CharLength: {
      // check to make sure this function has two arguments
      if (!CheckArgCount(call, 2)) {
        return;
      }

      // checking to see if the second argument is a string
      auto *resolved_type = call->Arguments()[1]->GetType();
      if (!resolved_type->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(GetContext()));
        return;
      }

      // this function returns an Integer
      sql_type = ast::BuiltinType::Integer;
      break;
    }
    case ast::Builtin::Trim2:
    case ast::Builtin::Concat: {
      // check to make sure this function has three arguments
      if (!CheckArgCountAtLeast(call, 2)) {
        return;
      }

      // checking to see if the arguments are strings
      for (uint32_t i = 1; i < call->NumArgs(); i++) {
        auto *resolved_type = call->Arguments()[i]->GetType();
        if (!resolved_type->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
          ReportIncorrectCallArg(call, i, ast::StringType::Get(GetContext()));
          return;
        }
      }

      // this function returns a string
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    case ast::Builtin::Trim:
    case ast::Builtin::Lower:
    case ast::Builtin::Upper:
    case ast::Builtin::Reverse:
    case ast::Builtin::InitCap: {
      // check to make sure this function has two arguments
      if (!CheckArgCount(call, 2)) {
        return;
      }

      // checking to see if the second argument is a string
      auto *resolved_type = call->Arguments()[1]->GetType();
      if (!resolved_type->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(GetContext()));
        return;
      }

      // this function returns a string
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    case ast::Builtin::Left:
    case ast::Builtin::Right:
    case ast::Builtin::Repeat: {
      // check to make sure this function has three arguments
      if (!CheckArgCount(call, 3)) {
        return;
      }

      // checking to see if the second argument is a string
      auto *resolved_type = call->Arguments()[1]->GetType();
      if (!resolved_type->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(GetContext()));
        return;
      }

      resolved_type = call->Arguments()[2]->GetType();
      if (!resolved_type->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Integer));
        return;
      }

      // this function returns a string
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    case ast::Builtin::Substring: {
      // check to make sure this function has four arguments
      if (!CheckArgCount(call, 4)) {
        return;
      }

      // checking to see if the second argument is a string
      auto *resolved_type = call->Arguments()[1]->GetType();
      if (!resolved_type->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(GetContext()));
        return;
      }

      auto int_t_kind = ast::BuiltinType::Integer;
      // checking to see if the third argument is an Integer
      if (call->Arguments()[2]->GetType() != GetBuiltinType(int_t_kind)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(int_t_kind));
        return;
      }

      // checking to see if the fourth argument is an Integer
      if (call->Arguments()[3]->GetType() != GetBuiltinType(int_t_kind)) {
        ReportIncorrectCallArg(call, 3, GetBuiltinType(int_t_kind));
        return;
      }

      // this function returns a string
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    case ast::Builtin::Version: {
      // check to make sure this function has one arguments
      if (!CheckArgCount(call, 1)) {
        return;
      }

      // this function returns a string
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    case ast::Builtin::Position: {
      // check to make sure this function has three arguments
      if (!CheckArgCount(call, 3)) {
        return;
      }

      // checking to see if the second argument is a string
      auto *resolved_type = call->Arguments()[1]->GetType();
      if (!resolved_type->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(GetContext()));
        return;
      }

      // checking to see if the third argument is a string
      resolved_type = call->Arguments()[2]->GetType();
      if (!resolved_type->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 2, ast::StringType::Get(GetContext()));
        return;
      }

      // this function returns an Integer
      sql_type = ast::BuiltinType::Integer;
      break;
    }
    case ast::Builtin::Length: {
      // check to make sure this function has two arguments
      if (!CheckArgCount(call, 2)) {
        return;
      }

      // checking to see if the second argument is a string
      if (!call->Arguments()[1]->GetType()->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(GetContext()));
        return;
      }

      // this function returns an integer
      sql_type = ast::BuiltinType::Integer;
      break;
    }
    case ast::Builtin::StartsWith: {
      // check to make sure this function has two arguments
      if (!CheckArgCount(call, 3)) {
        return;
      }

      // checking to see if the second argument is a string
      auto *resolved_type = call->Arguments()[1]->GetType();
      if (!resolved_type->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(GetContext()));
        return;
      }

      // checking to see if the third argument is a string
      resolved_type = call->Arguments()[2]->GetType();
      if (!resolved_type->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 2, ast::StringType::Get(GetContext()));
        return;
      }

      // this function returns a boolean
      sql_type = ast::BuiltinType::Boolean;
      break;
    }
    case ast::Builtin::Lpad:
    case ast::Builtin::Rpad: {
      if (!CheckArgCountBetween(call, 3, 4)) {
        return;
      }

      // checking to see if the second argument is a string
      if (!call->Arguments()[1]->GetType()->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(GetContext()));
        return;
      }

      // checking to see if the third argument is an integer
      if (!call->Arguments()[2]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Integer));
        return;
      }

      if (call->NumArgs() == 4 && !call->Arguments()[3]->GetType()->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 3, ast::StringType::Get(GetContext()));
        return;
      }

      // this function returns a string
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    case ast::Builtin::Ltrim:
    case ast::Builtin::Rtrim: {
      if (!CheckArgCountBetween(call, 2, 3)) {
        return;
      }

      // checking to see if the second argument is a string
      if (!call->Arguments()[1]->GetType()->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 1, ast::StringType::Get(GetContext()));
        return;
      }

      // checking to see if the third argument is a string
      if (call->NumArgs() == 3 && !call->Arguments()[2]->GetType()->IsSpecificBuiltin(ast::BuiltinType::StringVal)) {
        ReportIncorrectCallArg(call, 2, ast::StringType::Get(GetContext()));
        return;
      }

      // this function returns a string
      sql_type = ast::BuiltinType::StringVal;
      break;
    }
    default:
      UNREACHABLE("Unimplemented string call!!");
  }
  call->SetType(ast::BuiltinType::Get(GetContext(), sql_type));
}

void Sema::CheckBuiltinTestCatalogLookup(ast::CallExpr *call) {
  if (!CheckArgCount(call, 3)) {
    return;
  }
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), ast::BuiltinType::ExecutionContext)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(ast::BuiltinType::ExecutionContext)->PointerTo());
    return;
  }
  if (!call->Arguments()[1]->GetType()->IsStringType() || !call->Arguments()[1]->IsLitExpr()) {
    ReportIncorrectCallArg(call, 1, "string literal");
    return;
  }
  if (!call->Arguments()[2]->GetType()->IsStringType() || !call->Arguments()[2]->IsLitExpr()) {
    ReportIncorrectCallArg(call, 2, "string literal");
    return;
  }
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
}

void Sema::CheckBuiltinTestCatalogIndexLookup(ast::CallExpr *call) {
  if (!CheckArgCount(call, 2)) {
    return;
  }
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), ast::BuiltinType::ExecutionContext)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(ast::BuiltinType::ExecutionContext)->PointerTo());
    return;
  }
  if (!call->Arguments()[1]->GetType()->IsStringType() || !call->Arguments()[1]->IsLitExpr()) {
    ReportIncorrectCallArg(call, 1, "string literal");
    return;
  }
  call->SetType(GetBuiltinType(ast::BuiltinType::Uint32));
}

void Sema::CheckBuiltinCall(ast::CallExpr *call) {
  ast::Builtin builtin;
  if (!GetContext()->IsBuiltinFunction(call->GetFuncName(), &builtin)) {
    GetErrorReporter()->Report(call->Function()->Position(), ErrorMessages::kInvalidBuiltinFunction,
                               call->GetFuncName());
    return;
  }

  if (builtin == ast::Builtin::PtrCast) {
    CheckBuiltinPtrCastCall(call);
    return;
  }

  if (builtin == ast::Builtin::OffsetOf) {
    CheckBuiltinOffsetOfCall(call);
    return;
  }

  // First, resolve all call arguments. If any fail, exit immediately.
  for (auto *arg : call->Arguments()) {
    auto *resolved_type = Resolve(arg);
    if (resolved_type == nullptr) {
      return;
    }
  }

  switch (builtin) {
    case ast::Builtin::BoolToSql:
    case ast::Builtin::IntToSql:
    case ast::Builtin::FloatToSql:
    case ast::Builtin::DateToSql:
    case ast::Builtin::TimestampToSql:
    case ast::Builtin::TimestampToSqlYMDHMSMU:
    case ast::Builtin::StringToSql:
    case ast::Builtin::SqlToBool:
    case ast::Builtin::ConvertBoolToInteger:
    case ast::Builtin::ConvertIntegerToReal:
    case ast::Builtin::ConvertDateToTimestamp:
    case ast::Builtin::ConvertStringToBool:
    case ast::Builtin::ConvertStringToInt:
    case ast::Builtin::ConvertStringToReal:
    case ast::Builtin::ConvertStringToDate:
    case ast::Builtin::ConvertStringToTime: {
      CheckSqlConversionCall(call, builtin);
      break;
    }
    case ast::Builtin::IsValNull:
    case ast::Builtin::InitSqlNull: {
      CheckNullValueCall(call, builtin);
      break;
    }
    case ast::Builtin::Like: {
      CheckBuiltinStringLikeCall(call);
      break;
    }
    case ast::Builtin::DatePart: {
      CheckBuiltinDateFunctionCall(call, builtin);
      break;
    }
    case ast::Builtin::RegisterThreadWithMetricsManager:
    case ast::Builtin::CheckTrackersStopped:
    case ast::Builtin::AggregateMetricsThread:
    case ast::Builtin::ExecutionContextAddRowsAffected:
    case ast::Builtin::ExecutionContextRegisterHook:
    case ast::Builtin::ExecutionContextClearHooks:
    case ast::Builtin::ExecutionContextInitHooks:
    case ast::Builtin::ExecutionContextGetMemoryPool:
    case ast::Builtin::ExecutionContextGetTLS:
    case ast::Builtin::ExecutionContextStartResourceTracker:
    case ast::Builtin::ExecutionContextSetMemoryUseOverride:
    case ast::Builtin::ExecutionContextEndResourceTracker:
    case ast::Builtin::ExecutionContextStartPipelineTracker:
    case ast::Builtin::ExecutionContextEndPipelineTracker:
    case ast::Builtin::ExecOUFeatureVectorInitialize: {
      CheckBuiltinExecutionContextCall(call, builtin);
      break;
    }
    case ast::Builtin::ExecOUFeatureVectorReset:
    case ast::Builtin::ExecOUFeatureVectorFilter:
    case ast::Builtin::ExecOUFeatureVectorRecordFeature: {
      CheckBuiltinExecOUFeatureVectorCall(call, builtin);
      break;
    }
    case ast::Builtin::ThreadStateContainerReset:
    case ast::Builtin::ThreadStateContainerGetState:
    case ast::Builtin::ThreadStateContainerIterate:
    case ast::Builtin::ThreadStateContainerClear: {
      CheckBuiltinThreadStateContainerCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterInit:
    case ast::Builtin::TableIterAdvance:
    case ast::Builtin::TableIterGetVPINumTuples:
    case ast::Builtin::TableIterGetVPI:
    case ast::Builtin::TableIterClose: {
      CheckBuiltinTableIterCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterParallel: {
      CheckBuiltinTableIterParCall(call);
      break;
    }
    case ast::Builtin::VPIInit:
    case ast::Builtin::VPIFree:
    case ast::Builtin::VPIIsFiltered:
    case ast::Builtin::VPIGetSelectedRowCount:
    case ast::Builtin::VPIGetVectorProjection:
    case ast::Builtin::VPIHasNext:
    case ast::Builtin::VPIHasNextFiltered:
    case ast::Builtin::VPIAdvance:
    case ast::Builtin::VPIAdvanceFiltered:
    case ast::Builtin::VPISetPosition:
    case ast::Builtin::VPISetPositionFiltered:
    case ast::Builtin::VPIMatch:
    case ast::Builtin::VPIReset:
    case ast::Builtin::VPIResetFiltered:
    case ast::Builtin::VPIGetSlot:
    case ast::Builtin::VPIGetBool:
    case ast::Builtin::VPIGetBoolNull:
    case ast::Builtin::VPIGetTinyInt:
    case ast::Builtin::VPIGetTinyIntNull:
    case ast::Builtin::VPIGetSmallInt:
    case ast::Builtin::VPIGetSmallIntNull:
    case ast::Builtin::VPIGetInt:
    case ast::Builtin::VPIGetIntNull:
    case ast::Builtin::VPIGetBigInt:
    case ast::Builtin::VPIGetBigIntNull:
    case ast::Builtin::VPIGetReal:
    case ast::Builtin::VPIGetRealNull:
    case ast::Builtin::VPIGetDouble:
    case ast::Builtin::VPIGetDoubleNull:
    case ast::Builtin::VPIGetDate:
    case ast::Builtin::VPIGetDateNull:
    case ast::Builtin::VPIGetTimestamp:
    case ast::Builtin::VPIGetTimestampNull:
    case ast::Builtin::VPIGetString:
    case ast::Builtin::VPIGetStringNull:
    case ast::Builtin::VPIGetPointer:
    case ast::Builtin::VPISetBool:
    case ast::Builtin::VPISetBoolNull:
    case ast::Builtin::VPISetTinyInt:
    case ast::Builtin::VPISetTinyIntNull:
    case ast::Builtin::VPISetSmallInt:
    case ast::Builtin::VPISetSmallIntNull:
    case ast::Builtin::VPISetInt:
    case ast::Builtin::VPISetIntNull:
    case ast::Builtin::VPISetBigInt:
    case ast::Builtin::VPISetBigIntNull:
    case ast::Builtin::VPISetReal:
    case ast::Builtin::VPISetRealNull:
    case ast::Builtin::VPISetDouble:
    case ast::Builtin::VPISetDoubleNull:
    case ast::Builtin::VPISetDate:
    case ast::Builtin::VPISetDateNull:
    case ast::Builtin::VPISetTimestamp:
    case ast::Builtin::VPISetTimestampNull:
    case ast::Builtin::VPISetString:
    case ast::Builtin::VPISetStringNull: {
      CheckBuiltinVPICall(call, builtin);
      break;
    }
    case ast::Builtin::Hash: {
      CheckBuiltinHashCall(call, builtin);
      break;
    }
    case ast::Builtin::FilterManagerInit:
    case ast::Builtin::FilterManagerInsertFilter:
    case ast::Builtin::FilterManagerRunFilters:
    case ast::Builtin::FilterManagerFree: {
      CheckBuiltinFilterManagerCall(call, builtin);
      break;
    }
    case ast::Builtin::VectorFilterEqual:
    case ast::Builtin::VectorFilterGreaterThan:
    case ast::Builtin::VectorFilterGreaterThanEqual:
    case ast::Builtin::VectorFilterLessThan:
    case ast::Builtin::VectorFilterLessThanEqual:
    case ast::Builtin::VectorFilterNotEqual:
    case ast::Builtin::VectorFilterLike:
    case ast::Builtin::VectorFilterNotLike: {
      CheckBuiltinVectorFilterCall(call);
      break;
    }
    case ast::Builtin::AggHashTableInit:
    case ast::Builtin::AggHashTableGetTupleCount:
    case ast::Builtin::AggHashTableGetInsertCount:
    case ast::Builtin::AggHashTableInsert:
    case ast::Builtin::AggHashTableLinkEntry:
    case ast::Builtin::AggHashTableLookup:
    case ast::Builtin::AggHashTableProcessBatch:
    case ast::Builtin::AggHashTableMovePartitions:
    case ast::Builtin::AggHashTableParallelPartitionedScan:
    case ast::Builtin::AggHashTableFree: {
      CheckBuiltinAggHashTableCall(call, builtin);
      break;
    }
    case ast::Builtin::AggHashTableIterInit:
    case ast::Builtin::AggHashTableIterHasNext:
    case ast::Builtin::AggHashTableIterNext:
    case ast::Builtin::AggHashTableIterGetRow:
    case ast::Builtin::AggHashTableIterClose: {
      CheckBuiltinAggHashTableIterCall(call, builtin);
      break;
    }
    case ast::Builtin::AggPartIterHasNext:
    case ast::Builtin::AggPartIterNext:
    case ast::Builtin::AggPartIterGetRow:
    case ast::Builtin::AggPartIterGetRowEntry:
    case ast::Builtin::AggPartIterGetHash: {
      CheckBuiltinAggPartIterCall(call, builtin);
      break;
    }
    case ast::Builtin::AggInit:
    case ast::Builtin::AggAdvance:
    case ast::Builtin::AggMerge:
    case ast::Builtin::AggReset:
    case ast::Builtin::AggResult: {
      CheckBuiltinAggregatorCall(call, builtin);
      break;
    }
    case ast::Builtin::JoinHashTableInit: {
      CheckBuiltinJoinHashTableInit(call);
      break;
    }
    case ast::Builtin::JoinHashTableInsert: {
      CheckBuiltinJoinHashTableInsert(call);
      break;
    }
    case ast::Builtin::JoinHashTableGetTupleCount: {
      CheckBuiltinJoinHashTableGetTupleCount(call);
      break;
    }
    case ast::Builtin::JoinHashTableBuild:
    case ast::Builtin::JoinHashTableBuildParallel: {
      CheckBuiltinJoinHashTableBuild(call, builtin);
      break;
    }
    case ast::Builtin::JoinHashTableLookup: {
      CheckBuiltinJoinHashTableLookup(call);
      break;
    }
    case ast::Builtin::JoinHashTableFree: {
      CheckBuiltinJoinHashTableFree(call);
      break;
    }
    case ast::Builtin::HashTableEntryIterHasNext:
    case ast::Builtin::HashTableEntryIterGetRow: {
      CheckBuiltinHashTableEntryIterCall(call, builtin);
      break;
    }
    case ast::Builtin::JoinHashTableIterInit:
    case ast::Builtin::JoinHashTableIterHasNext:
    case ast::Builtin::JoinHashTableIterNext:
    case ast::Builtin::JoinHashTableIterGetRow:
    case ast::Builtin::JoinHashTableIterFree: {
      CheckBuiltinJoinHashTableIterCall(call, builtin);
      break;
    }
    case ast::Builtin::SorterInit: {
      CheckBuiltinSorterInit(call);
      break;
    }
    case ast::Builtin::SorterGetTupleCount: {
      CheckBuiltinSorterGetTupleCount(call);
      break;
    }
    case ast::Builtin::SorterInsert:
    case ast::Builtin::SorterInsertTopK:
    case ast::Builtin::SorterInsertTopKFinish: {
      CheckBuiltinSorterInsert(call, builtin);
      break;
    }
    case ast::Builtin::SorterSort:
    case ast::Builtin::SorterSortParallel:
    case ast::Builtin::SorterSortTopKParallel: {
      CheckBuiltinSorterSort(call, builtin);
      break;
    }
    case ast::Builtin::SorterFree: {
      CheckBuiltinSorterFree(call);
      break;
    }
    case ast::Builtin::SorterIterInit:
    case ast::Builtin::SorterIterHasNext:
    case ast::Builtin::SorterIterNext:
    case ast::Builtin::SorterIterSkipRows:
    case ast::Builtin::SorterIterGetRow:
    case ast::Builtin::SorterIterClose: {
      CheckBuiltinSorterIterCall(call, builtin);
      break;
    }
    case ast::Builtin::ResultBufferNew:
    case ast::Builtin::ResultBufferAllocOutRow:
    case ast::Builtin::ResultBufferFinalize:
    case ast::Builtin::ResultBufferFree: {
      CheckResultBufferCall(call, builtin);
      break;
    }
    case ast::Builtin::IndexIteratorInit: {
      CheckBuiltinIndexIteratorInit(call, builtin);
      break;
    }
    case ast::Builtin::IndexIteratorGetSize: {
      CheckBuiltinIndexIteratorGetSize(call);
      break;
    }
    case ast::Builtin::IndexIteratorScanKey:
    case ast::Builtin::IndexIteratorScanAscending:
    case ast::Builtin::IndexIteratorScanDescending:
    case ast::Builtin::IndexIteratorScanLimitDescending: {
      CheckBuiltinIndexIteratorScan(call, builtin);
      break;
    }
    case ast::Builtin::IndexIteratorAdvance: {
      CheckBuiltinIndexIteratorAdvance(call);
      break;
    }
    case ast::Builtin::IndexIteratorGetPR:
    case ast::Builtin::IndexIteratorGetLoPR:
    case ast::Builtin::IndexIteratorGetHiPR:
    case ast::Builtin::IndexIteratorGetSlot:
    case ast::Builtin::IndexIteratorGetTablePR: {
      CheckBuiltinIndexIteratorPRCall(call, builtin);
      break;
    }
    case ast::Builtin::IndexIteratorFree: {
      CheckBuiltinIndexIteratorFree(call);
      break;
    }
      /*
    case ast::Builtin::CSVReaderInit:
    case ast::Builtin::CSVReaderAdvance:
    case ast::Builtin::CSVReaderGetField:
    case ast::Builtin::CSVReaderGetRecordNumber:
    case ast::Builtin::CSVReaderClose: {
      CheckCSVReaderCall(call, builtin);
      break;
    }
       */
    case ast::Builtin::PRSetBool:
    case ast::Builtin::PRSetTinyInt:
    case ast::Builtin::PRSetSmallInt:
    case ast::Builtin::PRSetInt:
    case ast::Builtin::PRSetBigInt:
    case ast::Builtin::PRSetReal:
    case ast::Builtin::PRSetDouble:
    case ast::Builtin::PRSetDate:
    case ast::Builtin::PRSetTimestamp:
    case ast::Builtin::PRSetVarlen:
    case ast::Builtin::PRSetBoolNull:
    case ast::Builtin::PRSetTinyIntNull:
    case ast::Builtin::PRSetSmallIntNull:
    case ast::Builtin::PRSetIntNull:
    case ast::Builtin::PRSetBigIntNull:
    case ast::Builtin::PRSetRealNull:
    case ast::Builtin::PRSetDoubleNull:
    case ast::Builtin::PRSetDateNull:
    case ast::Builtin::PRSetTimestampNull:
    case ast::Builtin::PRSetVarlenNull:
    case ast::Builtin::PRGetBool:
    case ast::Builtin::PRGetTinyInt:
    case ast::Builtin::PRGetSmallInt:
    case ast::Builtin::PRGetInt:
    case ast::Builtin::PRGetBigInt:
    case ast::Builtin::PRGetReal:
    case ast::Builtin::PRGetDouble:
    case ast::Builtin::PRGetDate:
    case ast::Builtin::PRGetTimestamp:
    case ast::Builtin::PRGetVarlen:
    case ast::Builtin::PRGetBoolNull:
    case ast::Builtin::PRGetTinyIntNull:
    case ast::Builtin::PRGetSmallIntNull:
    case ast::Builtin::PRGetIntNull:
    case ast::Builtin::PRGetBigIntNull:
    case ast::Builtin::PRGetRealNull:
    case ast::Builtin::PRGetDoubleNull:
    case ast::Builtin::PRGetDateNull:
    case ast::Builtin::PRGetTimestampNull:
    case ast::Builtin::PRGetVarlenNull: {
      CheckBuiltinPRCall(call, builtin);
      break;
    }
    case ast::Builtin::StorageInterfaceInit:
    case ast::Builtin::GetTablePR:
    case ast::Builtin::StorageInterfaceGetIndexHeapSize:
    case ast::Builtin::TableInsert:
    case ast::Builtin::TableDelete:
    case ast::Builtin::TableUpdate:
    case ast::Builtin::GetIndexPR:
    case ast::Builtin::IndexGetSize:
    case ast::Builtin::IndexInsert:
    case ast::Builtin::IndexInsertUnique:
    case ast::Builtin::IndexInsertWithSlot:
    case ast::Builtin::IndexDelete:
    case ast::Builtin::StorageInterfaceFree: {
      CheckBuiltinStorageInterfaceCall(call, builtin);
      break;
    }
    case ast::Builtin::Mod:
    case ast::Builtin::Exp:
    case ast::Builtin::ACos:
    case ast::Builtin::ASin:
    case ast::Builtin::ATan:
    case ast::Builtin::ATan2:
    case ast::Builtin::Cosh:
    case ast::Builtin::Sinh:
    case ast::Builtin::Tanh:
    case ast::Builtin::Cos:
    case ast::Builtin::Cot:
    case ast::Builtin::Sin:
    case ast::Builtin::Tan:
    case ast::Builtin::Ceil:
    case ast::Builtin::Floor:
    case ast::Builtin::Truncate:
    case ast::Builtin::Log10:
    case ast::Builtin::Log2:
    case ast::Builtin::Abs:
    case ast::Builtin::Sqrt:
    case ast::Builtin::Cbrt:
    case ast::Builtin::Round:
    case ast::Builtin::Round2:
    case ast::Builtin::Pow: {
      CheckMathTrigCall(call, builtin);
      break;
    }
    case ast::Builtin::SizeOf: {
      CheckBuiltinSizeOfCall(call);
      break;
    }
    case ast::Builtin::OffsetOf: {
      CheckBuiltinOffsetOfCall(call);
      break;
    }
    case ast::Builtin::PtrCast: {
      UNREACHABLE("Pointer cast should be handled outside switch ...");
    }
    case ast::Builtin::AbortTxn: {
      CheckBuiltinAbortCall(call);
      break;
    }
    case ast::Builtin::GetParamBool:
    case ast::Builtin::GetParamTinyInt:
    case ast::Builtin::GetParamSmallInt:
    case ast::Builtin::GetParamInt:
    case ast::Builtin::GetParamBigInt:
    case ast::Builtin::GetParamReal:
    case ast::Builtin::GetParamDouble:
    case ast::Builtin::GetParamDate:
    case ast::Builtin::GetParamTimestamp:
    case ast::Builtin::GetParamString: {
      CheckBuiltinParamCall(call, builtin);
      break;
    }
    case ast::Builtin::SplitPart:
    case ast::Builtin::Chr:
    case ast::Builtin::CharLength:
    case ast::Builtin::ASCII:
    case ast::Builtin::Trim:
    case ast::Builtin::Trim2:
    case ast::Builtin::Lower:
    case ast::Builtin::Upper:
    case ast::Builtin::Version:
    case ast::Builtin::StartsWith:
    case ast::Builtin::Substring:
    case ast::Builtin::Reverse:
    case ast::Builtin::Right:
    case ast::Builtin::Left:
    case ast::Builtin::Repeat:
    case ast::Builtin::Position:
    case ast::Builtin::Length:
    case ast::Builtin::InitCap:
    case ast::Builtin::Lpad:
    case ast::Builtin::Rpad:
    case ast::Builtin::Ltrim:
    case ast::Builtin::Rtrim:
    case ast::Builtin::Concat: {
      CheckBuiltinStringCall(call, builtin);
      break;
    }
    case ast::Builtin::NpRunnersEmitInt:
    case ast::Builtin::NpRunnersEmitReal: {
      if (!CheckArgCount(call, 5)) {
        return;
      }

      // checking to see if the first argument is an execution context
      auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
      if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), exec_ctx_kind)) {
        ReportIncorrectCallArg(call, 0, GetBuiltinType(exec_ctx_kind)->PointerTo());
        return;
      }

      const auto &call_args = call->Arguments();
      if (!call_args[1]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Integer));
        return;
      }

      if (!call_args[2]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Integer));
        return;
      }

      if (!call_args[3]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
        ReportIncorrectCallArg(call, 3, GetBuiltinType(ast::BuiltinType::Integer));
        return;
      }

      if (!call_args[4]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
        ReportIncorrectCallArg(call, 4, GetBuiltinType(ast::BuiltinType::Integer));
        return;
      }

      auto builtin_type = GetBuiltinType(ast::BuiltinType::Integer);
      if (builtin == ast::Builtin::NpRunnersEmitReal) builtin_type = GetBuiltinType(ast::BuiltinType::Real);
      call->SetType(builtin_type);
      break;
    }
    case ast::Builtin::NpRunnersDummyInt:
    case ast::Builtin::NpRunnersDummyReal: {
      if (!CheckArgCount(call, 1)) {
        return;
      }

      // checking to see if the first argument is an execution context
      auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
      if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), exec_ctx_kind)) {
        ReportIncorrectCallArg(call, 0, GetBuiltinType(exec_ctx_kind)->PointerTo());
        return;
      }

      auto builtin_type = GetBuiltinType(ast::BuiltinType::Integer);
      if (builtin == ast::Builtin::NpRunnersDummyReal) builtin_type = GetBuiltinType(ast::BuiltinType::Real);
      call->SetType(builtin_type);
      break;
    }
    case ast::Builtin::TestCatalogLookup: {
      CheckBuiltinTestCatalogLookup(call);
      break;
    }
    case ast::Builtin::TestCatalogIndexLookup: {
      CheckBuiltinTestCatalogIndexLookup(call);
      break;
    }
    default:
      UNREACHABLE("Unhandled builtin!");
  }
}

}  // namespace noisepage::execution::sema
