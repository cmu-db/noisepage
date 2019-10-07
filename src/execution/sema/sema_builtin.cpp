#include "execution/sema/sema.h"

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"

namespace terrier::execution::sema {

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

void Sema::CheckBuiltinMapCall(UNUSED_ATTRIBUTE ast::CallExpr *call) {}

void Sema::CheckBuiltinSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (builtin == ast::Builtin::DateToSql) {
    if (!CheckArgCountAtLeast(call, 3)) return;
    auto uint16_t_kind = ast::BuiltinType::Uint16;
    auto uint8_t_kind = ast::BuiltinType::Uint8;
    // First argument (year) is a uint16_t
    if (!call->Arguments()[0]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 0, GetBuiltinType(uint16_t_kind));
      return;
    }
    // First argument (month) is a uint8_t
    if (!call->Arguments()[1]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 1, GetBuiltinType(uint8_t_kind));
      return;
    }
    // First argument (day) is a uint8_t
    if (!call->Arguments()[2]->GetType()->IsIntegerType()) {
      ReportIncorrectCallArg(call, 2, GetBuiltinType(uint8_t_kind));
      return;
    }
    // Return a date type
    call->SetType(GetBuiltinType(ast::BuiltinType::Date));
    return;
  }
  if (!CheckArgCount(call, 1)) {
    return;
  }
  auto input_type = call->Arguments()[0]->GetType();
  switch (builtin) {
    case ast::Builtin::BoolToSql: {
      if (!input_type->IsSpecificBuiltin(ast::BuiltinType::Bool)) {
        GetErrorReporter()->Report(call->Position(), ErrorMessages::kInvalidSqlCastToBool, input_type);
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Boolean));
      break;
    }
    case ast::Builtin::IntToSql: {
      if (!input_type->IsIntegerType()) {
        GetErrorReporter()->Report(call->Position(), ErrorMessages::kInvalidSqlCastToBool, input_type);
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Integer));
      break;
    }
    case ast::Builtin::FloatToSql: {
      if (!input_type->IsFloatType()) {
        GetErrorReporter()->Report(call->Position(), ErrorMessages::kInvalidSqlCastToBool, input_type);
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Real));
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
    case ast::Builtin::StringToSql: {
      if (!input_type->IsStringType()) {
        ReportIncorrectCallArg(call, 0, ast::StringType::Get(GetContext()));
        return;
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::StringVal));
      break;
    }
    case ast::Builtin::VarlenToSql: {
      if (!input_type->IsIntegerType()) {
        ReportIncorrectCallArg(call, 0, GetBuiltinType(ast::BuiltinType::Uint64));
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::StringVal));
      break;
    }
    default: {
      UNREACHABLE("Impossible SQL conversion call");
    }
  }
}

void Sema::CheckBuiltinFilterCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 4)) {
    return;
  }

  const auto &args = call->Arguments();

  // The first call argument must be a pointer to a ProjectedColumnsIterator
  const auto pci_kind = ast::BuiltinType::ProjectedColumnsIterator;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), pci_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(pci_kind)->PointerTo());
    return;
  }

  // The second call argument must be an integer for the column index
  auto int32_kind = ast::BuiltinType::Int32;
  if (!args[1]->IsIntegerLiteral()) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(int32_kind));
    return;
  }

  // The third call argument must be an type represented by an integer.
  // TODO(Amadou): This is subject to change. Ideally, there should be a builtin for every type like for PCIGet.
  if (!args[2]->IsIntegerLiteral()) {
    ReportIncorrectCallArg(call, 2, GetBuiltinType(int32_kind));
    return;
  }

  // Set return type
  call->SetType(GetBuiltinType(ast::BuiltinType::Int64));
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
      // Second argument is a memory pool pointer
      const auto mem_pool_kind = ast::BuiltinType::MemoryPool;
      if (!IsPointerToSpecificBuiltin(args[1]->GetType(), mem_pool_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(mem_pool_kind)->PointerTo());
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
    case ast::Builtin::AggHashTableInsert: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // Second argument is the hash value
      const auto hash_val_kind = ast::BuiltinType::Uint64;
      if (!args[1]->GetType()->IsSpecificBuiltin(hash_val_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(hash_val_kind));
        return;
      }
      // Return a byte pointer
      call->SetType(GetBuiltinType(ast::BuiltinType::Uint8)->PointerTo());
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
      if (!CheckArgCount(call, 7)) {
        return;
      }
      // Second argument is the PCIs
      const auto pci_kind = ast::BuiltinType::Uint64;
      if (!args[1]->GetType()->IsPointerType() ||
          IsPointerToSpecificBuiltin(args[1]->GetType()->GetPointeeType(), pci_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(pci_kind)->PointerTo());
        return;
      }
      // Third, fourth, fifth, and sixth are all functions
      if (!AreAllFunctions(args[2]->GetType(), args[3]->GetType(), args[4]->GetType(), args[5]->GetType())) {
        ReportIncorrectCallArg(call, 2, "function");
        return;
      }
      // Last arg must be a boolean
      if (!args[6]->GetType()->IsBoolType()) {
        ReportIncorrectCallArg(call, 6, GetBuiltinType(ast::BuiltinType::Bool));
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

  const auto agg_ht_iter_kind = ast::BuiltinType::AggregationHashTableIterator;
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

  const auto part_iter_kind = ast::BuiltinType::AggOverflowPartIter;
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
    case ast::Builtin::AggPartIterGetRow: {
      const auto byte_kind = ast::BuiltinType::Uint8;
      call->SetType(GetBuiltinType(byte_kind)->PointerTo());
      break;
    }
    case ast::Builtin::AggPartIterGetHash: {
      const auto hash_val_kind = ast::BuiltinType::Uint64;
      call->SetType(GetBuiltinType(hash_val_kind));
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
      // First argument to @aggAdvance() must be a SQL aggregator, second must
      // be a SQL value
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
      // Set the return type according to the aggregate type.
      switch (args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind()) {
        case ast::BuiltinType::Kind::CountAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Integer));
          break;
        case ast::BuiltinType::Kind::CountStarAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Integer));
          break;
        case ast::BuiltinType::Kind::IntegerAvgAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Real));
          break;
        case ast::BuiltinType::Kind::IntegerSumAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Integer));
          break;
        case ast::BuiltinType::Kind::IntegerMaxAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Integer));
          break;
        case ast::BuiltinType::Kind::IntegerMinAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Integer));
          break;
        case ast::BuiltinType::Kind::RealAvgAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Real));
          break;
        case ast::BuiltinType::Kind::RealSumAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Real));
          break;
        case ast::BuiltinType::Kind::RealMaxAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Real));
          break;
        case ast::BuiltinType::Kind::RealMinAggregate:
          call->SetType(GetBuiltinType(ast::BuiltinType::Real));
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

  // Second argument must be a pointer to a MemoryPool
  const auto region_kind = ast::BuiltinType::MemoryPool;
  if (!IsPointerToSpecificBuiltin(args[1]->GetType(), region_kind)) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(region_kind)->PointerTo());
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

void Sema::CheckBuiltinJoinHashTableBuild(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // The first and only argument must be a pointer to a JoinHashTable
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

void Sema::CheckBuiltinJoinHashTableIterInit(ast::CallExpr *call) {
  if (!CheckArgCount(call, 3)) {
    return;
  }

  const auto &args = call->Arguments();

  // First argument is a pointer to a JoinHashTableIterator
  const auto jht_iterator_kind = ast::BuiltinType::JoinHashTableIterator;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), jht_iterator_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(jht_iterator_kind)->PointerTo());
    return;
  }

  // Second argument is a pointer to a JoinHashTable
  const auto jht_kind = ast::BuiltinType::JoinHashTable;
  if (!IsPointerToSpecificBuiltin(args[1]->GetType(), jht_kind)) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(jht_kind)->PointerTo());
    return;
  }

  // Third argument is a 64-bit unsigned hash value
  if (!args[2]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Uint64)) {
    ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Uint64));
    return;
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinJoinHashTableIterHasNext(ast::CallExpr *call) {
  if (!CheckArgCount(call, 4)) {
    return;
  }

  const auto &args = call->Arguments();

  // First argument is a pointer to a JoinHashTableIterator
  const auto jht_iterator_kind = ast::BuiltinType::JoinHashTableIterator;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), jht_iterator_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(jht_iterator_kind)->PointerTo());
    return;
  }

  // Second argument is a key equality function
  auto *const key_eq_type = args[1]->GetType()->SafeAs<ast::FunctionType>();
  if (key_eq_type == nullptr || key_eq_type->NumParams() != 3 ||
      !key_eq_type->ReturnType()->IsSpecificBuiltin(ast::BuiltinType::Bool) ||
      !key_eq_type->Params()[0].type_->IsPointerType() || !key_eq_type->Params()[1].type_->IsPointerType() ||
      !key_eq_type->Params()[2].type_->IsPointerType()) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadEqualityFunctionForJHTGetNext, args[1]->GetType(),
                               1);
    return;
  }

  // Third argument is an arbitrary pointer
  if (!args[2]->GetType()->IsPointerType()) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadPointerForJHTGetNext, args[2]->GetType(), 2);
    return;
  }

  // Fourth argument is an arbitrary pointer
  if (!args[3]->GetType()->IsPointerType()) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadPointerForJHTGetNext, args[3]->GetType(), 3);
    return;
  }

  // This call returns a bool
  call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
}

void Sema::CheckBuiltinJoinHashTableIterGetRow(execution::ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  const auto &args = call->Arguments();

  // The first argument is a pointer to a JoinHashTableIterator
  const auto jht_iterator_kind = ast::BuiltinType::JoinHashTableIterator;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), jht_iterator_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(jht_iterator_kind)->PointerTo());
    return;
  }

  // This call returns a byte pointer
  const auto byte_kind = ast::BuiltinType::Uint8;
  call->SetType(ast::BuiltinType::Get(GetContext(), byte_kind)->PointerTo());
}

void Sema::CheckBuiltinJoinHashTableIterClose(execution::ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  const auto &args = call->Arguments();

  // The first argument is a pointer to a JoinHashTableIterator
  const auto jht_iterator_kind = ast::BuiltinType::JoinHashTableIterator;
  if (!IsPointerToSpecificBuiltin(args[0]->GetType(), jht_iterator_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(jht_iterator_kind)->PointerTo());
    return;
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinExecutionContextCall(ast::CallExpr *call, UNUSED_ATTRIBUTE ast::Builtin builtin) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  const auto &call_args = call->Arguments();

  auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  if (!IsPointerToSpecificBuiltin(call_args[0]->GetType(), exec_ctx_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(exec_ctx_kind)->PointerTo());
    return;
  }

  auto mem_pool_kind = ast::BuiltinType::MemoryPool;
  call->SetType(GetBuiltinType(mem_pool_kind)->PointerTo());
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
    case ast::Builtin::ThreadStateContainerInit: {
      if (!CheckArgCount(call, 2)) {
        return;
      }

      // Second argument is a MemoryPool
      auto mem_pool_kind = ast::BuiltinType::MemoryPool;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), mem_pool_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(mem_pool_kind)->PointerTo());
        return;
      }
      break;
    }
    case ast::Builtin::ThreadStateContainerFree: {
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
      if (!call_args[2]->GetType()->IsFunctionType() || !call_args[3]->GetType()->IsFunctionType()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
      // Fifth argument must be a pointer to something or nil
      if (!call_args[4]->GetType()->IsPointerType() && !call_args[4]->GetType()->IsNilType()) {
        ReportIncorrectCallArg(call, 4, GetBuiltinType(ast::BuiltinType::Uint32));
        return;
      }
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
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }

  // All these calls return nil
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
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
      if (!call_args[2]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Int32));
        return;
      }
      // The fourth argument is a uint32_t array
      if (!call_args[3]->GetType()->IsArrayType()) {
        ReportIncorrectCallArg(call, 3, "Fourth argument should be a fixed length uint32 array");
        return;
      }
      auto *arr_type = call_args[3]->GetType()->SafeAs<ast::ArrayType>();
      auto uint32_t_kind = ast::BuiltinType::Uint32;
      if (!arr_type->ElementType()->IsSpecificBuiltin(uint32_t_kind) || !arr_type->HasKnownLength()) {
        ReportIncorrectCallArg(call, 3, "Fourth argument should be a fixed length uint32 array");
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::TableIterInitBind: {
      if (!CheckArgCount(call, 4)) {
        return;
      }
      // The second argument is the execution context
      auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
      if (!IsPointerToSpecificBuiltin(call_args[1]->GetType(), exec_ctx_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(exec_ctx_kind)->PointerTo());
        return;
      }
      // The third argument is the table name as a literal string
      if (!call_args[2]->IsStringLiteral()) {
        ReportIncorrectCallArg(call, 2, ast::StringType::Get(GetContext()));
        return;
      }
      // The fourth argument is a uint32_t array
      if (!call_args[3]->GetType()->IsArrayType()) {
        ReportIncorrectCallArg(call, 3, "Fourth argument should be a uint32 array");
        return;
      }
      auto *arr_type = call_args[3]->GetType()->SafeAs<ast::ArrayType>();
      auto uint32_t_kind = ast::BuiltinType::Uint32;
      if (!arr_type->ElementType()->IsSpecificBuiltin(uint32_t_kind)) {
        ReportIncorrectCallArg(call, 3, "Fourth argument should be a uint32 array");
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::TableIterAdvance: {
      // A single-arg builtin returning a boolean
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::TableIterGetPCI: {
      // A single-arg builtin return a pointer to the current PCI
      const auto pci_kind = ast::BuiltinType::ProjectedColumnsIterator;
      call->SetType(GetBuiltinType(pci_kind)->PointerTo());
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
  if (!CheckArgCount(call, 4)) {
    return;
  }

  const auto &call_args = call->Arguments();

  // First argument is table name as a string literal
  if (!call_args[0]->IsStringLiteral()) {
    ReportIncorrectCallArg(call, 0, ast::StringType::Get(GetContext()));
    return;
  }

  // Second argument is an opaque query state. For now, check it's a pointer.
  const auto void_kind = ast::BuiltinType::Nil;
  if (!call_args[1]->GetType()->IsPointerType()) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(void_kind)->PointerTo());
    return;
  }

  // Third argument is the thread state container
  const auto tls_kind = ast::BuiltinType::ThreadStateContainer;
  if (!IsPointerToSpecificBuiltin(call_args[2]->GetType(), tls_kind)) {
    ReportIncorrectCallArg(call, 2, GetBuiltinType(tls_kind)->PointerTo());
    return;
  }

  // Third argument is scanner function
  auto *scan_fn_type = call_args[3]->GetType()->SafeAs<ast::FunctionType>();
  if (scan_fn_type == nullptr) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadParallelScanFunction, call_args[3]->GetType());
    return;
  }
  // Check type
  const auto tvi_kind = ast::BuiltinType::TableVectorIterator;
  const auto &params = scan_fn_type->Params();
  if (params.size() != 3 || !params[0].type_->IsPointerType() || !params[1].type_->IsPointerType() ||
      !IsPointerToSpecificBuiltin(params[2].type_, tvi_kind)) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadParallelScanFunction, call_args[3]->GetType());
    return;
  }

  // Nil
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinPCICall(ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCountAtLeast(call, 1)) {
    return;
  }

  // The first argument must be a *PCI
  const auto pci_kind = ast::BuiltinType::ProjectedColumnsIterator;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), pci_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(pci_kind)->PointerTo());
    return;
  }

  switch (builtin) {
    case ast::Builtin::PCIIsFiltered:
    case ast::Builtin::PCIHasNext:
    case ast::Builtin::PCIHasNextFiltered:
    case ast::Builtin::PCIAdvance:
    case ast::Builtin::PCIAdvanceFiltered:
    case ast::Builtin::PCIReset:
    case ast::Builtin::PCIResetFiltered: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Bool));
      break;
    }
    case ast::Builtin::PCIMatch: {
      if (!CheckArgCount(call, 2)) {
        return;
      }
      // If the match argument is a SQL boolean, implicitly cast to native
      ast::Expr *match_arg = call->Arguments()[1];
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
    case ast::Builtin::PCIGetTinyInt:
    case ast::Builtin::PCIGetTinyIntNull:
    case ast::Builtin::PCIGetSmallInt:
    case ast::Builtin::PCIGetSmallIntNull:
    case ast::Builtin::PCIGetInt:
    case ast::Builtin::PCIGetIntNull:
    case ast::Builtin::PCIGetBigInt:
    case ast::Builtin::PCIGetBigIntNull: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Integer));
      break;
    }
    case ast::Builtin::PCIGetReal:
    case ast::Builtin::PCIGetRealNull:
    case ast::Builtin::PCIGetDouble:
    case ast::Builtin::PCIGetDoubleNull: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Real));
      break;
    }
    case ast::Builtin::PCIGetDate:
    case ast::Builtin::PCIGetDateNull: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Date));
      break;
    }
    case ast::Builtin::PCIGetVarlen:
    case ast::Builtin::PCIGetVarlenNull: {
      call->SetType(GetBuiltinType(ast::BuiltinType::StringVal));
      break;
    }
    default: {
      UNREACHABLE("Impossible PCI call");
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

  switch (builtin) {
    case ast::Builtin::FilterManagerInit:
    case ast::Builtin::FilterManagerFinalize:
    case ast::Builtin::FilterManagerFree: {
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::FilterManagerInsertFilter: {
      for (uint32_t arg_idx = 1; arg_idx < call->NumArgs(); arg_idx++) {
        // clang-format off
        auto *arg_type = call->Arguments()[arg_idx]->GetType()->SafeAs<ast::FunctionType>();
        if (arg_type == nullptr ||                                              // not a function
            !arg_type->ReturnType()->IsIntegerType() ||                        // doesn't return an integer
            arg_type->NumParams() != 1 ||                                      // isn't a single-arg func
            arg_type->Params()[0].type_->GetPointeeType() == nullptr ||          // first arg isn't a *PCI
            !arg_type->Params()[0].type_->GetPointeeType()->IsSpecificBuiltin(
                ast::BuiltinType::ProjectedColumnsIterator)) {
          // error
          GetErrorReporter()->Report(
              call->Position(), ErrorMessages::kIncorrectCallArgType,
              call->GetFuncName(),
              ast::BuiltinType::Get(GetContext(), fm_kind)->PointerTo(), arg_idx,
              call->Arguments()[arg_idx]->GetType());
          return;
        }
        // clang-format on
      }
      call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
      break;
    }
    case ast::Builtin::FilterManagerRunFilters: {
      const auto pci_kind = ast::BuiltinType::ProjectedColumnsIterator;
      if (!IsPointerToSpecificBuiltin(call->Arguments()[1]->GetType(), pci_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(pci_kind)->PointerTo());
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

void Sema::CheckMathTrigCall(ast::CallExpr *call, ast::Builtin builtin) {
  const auto real_kind = ast::BuiltinType::Real;

  const auto &call_args = call->Arguments();
  switch (builtin) {
    case ast::Builtin::ATan2: {
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
    case ast::Builtin::Cos:
    case ast::Builtin::Cot:
    case ast::Builtin::Sin:
    case ast::Builtin::Tan:
    case ast::Builtin::ACos:
    case ast::Builtin::ASin:
    case ast::Builtin::ATan: {
      if (!CheckArgCount(call, 1)) {
        return;
      }
      if (!call_args[0]->GetType()->IsSpecificBuiltin(real_kind)) {
        ReportIncorrectCallArg(call, 0, GetBuiltinType(real_kind));
        return;
      }
      break;
    }
    default: {
      UNREACHABLE("Impossible math trig function call");
    }
  }

  // Trig functions return real values
  call->SetType(GetBuiltinType(real_kind));
}

void Sema::CheckBuiltinSizeOfCall(ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  // This call returns an unsigned 32-bit value for the size of the type
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
      0, GetContext()->NodeFactory()->NewPointerType(call->Arguments()[0]->Position(), unary_op->Expression()));

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

  // Second argument must be a pointer to a MemoryPool
  const auto mem_kind = ast::BuiltinType::MemoryPool;
  if (!IsPointerToSpecificBuiltin(args[1]->GetType(), mem_kind)) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(mem_kind)->PointerTo());
    return;
  }

  // Second argument must be a function
  auto *const cmp_func_type = args[2]->GetType()->SafeAs<ast::FunctionType>();
  if (cmp_func_type == nullptr || cmp_func_type->NumParams() != 2 ||
      !cmp_func_type->ReturnType()->IsSpecificBuiltin(ast::BuiltinType::Int32) ||
      !cmp_func_type->Params()[0].type_->IsPointerType() || !cmp_func_type->Params()[1].type_->IsPointerType()) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kBadComparisonFunctionForSorter, args[2]->GetType());
    return;
  }

  // Third and last argument must be a 32-bit number representing the tuple size
  const auto uint_kind = ast::BuiltinType::Uint32;
  if (!args[3]->GetType()->IsSpecificBuiltin(uint_kind)) {
    ReportIncorrectCallArg(call, 3, GetBuiltinType(uint_kind));
    return;
  }

  // This call returns nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinSorterInsert(ast::CallExpr *call) {
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

      if (builtin == ast::Builtin::SorterSortTopKParallel) {
        if (!CheckArgCount(call, 4)) {
          return;
        }

        // Last argument must be the TopK value
        const auto uint64_kind = ast::BuiltinType::Uint64;
        if (!call_args[3]->GetType()->IsSpecificBuiltin(uint64_kind)) {
          ReportIncorrectCallArg(call, 3, GetBuiltinType(uint64_kind));
          return;
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

void Sema::CheckBuiltinOutputAlloc(execution::ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  // The first call argument must an execution context
  auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), exec_ctx_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(exec_ctx_kind)->PointerTo());
    return;
  }

  // Return a byte*
  ast::Type *ret_type = ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Uint8)->PointerTo();
  call->SetType(ret_type);
}

void Sema::CheckBuiltinOutputFinalize(execution::ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }

  // The first call argument must an execution context
  auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), exec_ctx_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(exec_ctx_kind)->PointerTo());
    return;
  }

  // Return nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
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
      if (!CheckArgCount(call, 5)) {
        return;
      }
      // The second argument is an execution context
      auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
      if (!IsPointerToSpecificBuiltin(call->Arguments()[1]->GetType(), exec_ctx_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(exec_ctx_kind)->PointerTo());
        return;
      }
      // The third argument is a table oid
      if (!call->Arguments()[2]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 2, GetBuiltinType(ast::BuiltinType::Int32));
        return;
      }
      // The fourth argument is an index oid
      if (!call->Arguments()[3]->IsIntegerLiteral()) {
        ReportIncorrectCallArg(call, 3, GetBuiltinType(ast::BuiltinType::Int32));
        return;
      }
      // The fifth argument is a uint32_t array
      if (!call->Arguments()[4]->GetType()->IsArrayType()) {
        ReportIncorrectCallArg(call, 4, "Fifth argument should be a fixed length uint32 array");
        return;
      }
      auto *arr_type = call->Arguments()[4]->GetType()->SafeAs<ast::ArrayType>();
      auto uint32_t_kind = ast::BuiltinType::Uint32;
      if (!arr_type->ElementType()->IsSpecificBuiltin(uint32_t_kind) || !arr_type->HasKnownLength()) {
        ReportIncorrectCallArg(call, 4, "Fifth argument should be a fixed length uint32 array");
      }
      break;
    }
    case ast::Builtin::IndexIteratorInitBind: {
      if (!CheckArgCount(call, 5)) {
        return;
      }
      // The second call argument must an execution context
      auto exec_ctx_kind = ast::BuiltinType::ExecutionContext;
      if (!IsPointerToSpecificBuiltin(call->Arguments()[1]->GetType(), exec_ctx_kind)) {
        ReportIncorrectCallArg(call, 1, GetBuiltinType(exec_ctx_kind)->PointerTo());
        return;
      }
      // The third argument must be the table's name
      if (!call->Arguments()[2]->GetType()->IsStringType()) {
        ReportIncorrectCallArg(call, 2, ast::StringType::Get(GetContext()));
        return;
      }
      // The fourth argument is the index's name
      if (!call->Arguments()[3]->GetType()->IsStringType()) {
        ReportIncorrectCallArg(call, 3, ast::StringType::Get(GetContext()));
        return;
      }
      // The fifth argument is a uint32_t array
      if (!call->Arguments()[4]->GetType()->IsArrayType()) {
        ReportIncorrectCallArg(call, 4, "Fifth argument should be a fixed length uint32 array");
        return;
      }
      auto *arr_type = call->Arguments()[4]->GetType()->SafeAs<ast::ArrayType>();
      auto uint32_t_kind = ast::BuiltinType::Uint32;
      if (!arr_type->ElementType()->IsSpecificBuiltin(uint32_t_kind) || !arr_type->HasKnownLength()) {
        ReportIncorrectCallArg(call, 4, "Fifth argument should be a fixed length uint32 array");
      }
      break;
    }
    default:
      UNREACHABLE("Unreachable index iterator in builtin");
  }

  // Return nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinIndexIteratorScanKey(execution::ast::CallExpr *call) {
  if (!CheckArgCount(call, 1)) {
    return;
  }
  // First argument must be a pointer to a IndexIterator
  auto index_kind = ast::BuiltinType::IndexIterator;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), index_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(index_kind)->PointerTo());
    return;
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

void Sema::CheckBuiltinIndexIteratorGet(execution::ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCount(call, 2)) {
    return;
  }
  // First argument must be a pointer to a IndexIterator
  auto index_kind = ast::BuiltinType::IndexIterator;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), index_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(index_kind)->PointerTo());
    return;
  }
  // Second argument must be an integer literal
  if (!call->Arguments()[1]->IsIntegerLiteral()) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Int32));
    return;
  }

  // Set return type
  switch (builtin) {
    case ast::Builtin::IndexIteratorGetTinyInt:
    case ast::Builtin::IndexIteratorGetSmallInt:
    case ast::Builtin::IndexIteratorGetInt:
    case ast::Builtin::IndexIteratorGetBigInt:
    case ast::Builtin::IndexIteratorGetTinyIntNull:
    case ast::Builtin::IndexIteratorGetSmallIntNull:
    case ast::Builtin::IndexIteratorGetIntNull:
    case ast::Builtin::IndexIteratorGetBigIntNull:
      call->SetType(ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Integer));
      break;
    case ast::Builtin::IndexIteratorGetDouble:
    case ast::Builtin::IndexIteratorGetReal:
    case ast::Builtin::IndexIteratorGetDoubleNull:
    case ast::Builtin::IndexIteratorGetRealNull:
      call->SetType(ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Real));
      break;
    default:
      UNREACHABLE("Impossible builtin!");
  }
}

void Sema::CheckBuiltinIndexIteratorSetKey(execution::ast::CallExpr *call, ast::Builtin builtin) {
  if (!CheckArgCount(call, 3)) {
    return;
  }
  // First argument must be a pointer to a IndexIterator
  auto index_kind = ast::BuiltinType::IndexIterator;
  if (!IsPointerToSpecificBuiltin(call->Arguments()[0]->GetType(), index_kind)) {
    ReportIncorrectCallArg(call, 0, GetBuiltinType(index_kind)->PointerTo());
    return;
  }
  // Second argument must be an integer literal
  if (!call->Arguments()[1]->IsIntegerLiteral()) {
    ReportIncorrectCallArg(call, 1, GetBuiltinType(ast::BuiltinType::Int32));
    return;
  }
  // The third argument depends on the builtin call
  ast::BuiltinType::Kind arg_kind;
  switch (builtin) {
    case ast::Builtin::IndexIteratorSetKeyTinyInt:
    case ast::Builtin::IndexIteratorSetKeySmallInt:
    case ast::Builtin::IndexIteratorSetKeyInt:
    case ast::Builtin::IndexIteratorSetKeyBigInt:
    case ast::Builtin::IndexIteratorSetKeyTinyIntNull:
    case ast::Builtin::IndexIteratorSetKeySmallIntNull:
    case ast::Builtin::IndexIteratorSetKeyIntNull:
    case ast::Builtin::IndexIteratorSetKeyBigIntNull:
      arg_kind = ast::BuiltinType::Integer;
      break;
    case ast::Builtin::IndexIteratorGetDouble:
    case ast::Builtin::IndexIteratorGetReal:
    case ast::Builtin::IndexIteratorGetDoubleNull:
    case ast::Builtin::IndexIteratorGetRealNull:
      arg_kind = ast::BuiltinType::Real;
      break;
    default:
      UNREACHABLE("Impossible builtin!");
  }
  if (GetBuiltinType(arg_kind) != call->Arguments()[2]->GetType()) {
    ReportIncorrectCallArg(call, 2, GetBuiltinType(arg_kind));
    return;
  }

  // Return nothing
  call->SetType(GetBuiltinType(ast::BuiltinType::Nil));
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
    case ast::Builtin::StringToSql:
    case ast::Builtin::VarlenToSql:
    case ast::Builtin::DateToSql:
    case ast::Builtin::SqlToBool: {
      CheckBuiltinSqlConversionCall(call, builtin);
      break;
    }
    case ast::Builtin::FilterEq:
    case ast::Builtin::FilterGe:
    case ast::Builtin::FilterGt:
    case ast::Builtin::FilterLt:
    case ast::Builtin::FilterNe:
    case ast::Builtin::FilterLe: {
      CheckBuiltinFilterCall(call);
      break;
    }
    case ast::Builtin::ExecutionContextGetMemoryPool: {
      CheckBuiltinExecutionContextCall(call, builtin);
      break;
    }
    case ast::Builtin::ThreadStateContainerInit:
    case ast::Builtin::ThreadStateContainerReset:
    case ast::Builtin::ThreadStateContainerIterate:
    case ast::Builtin::ThreadStateContainerFree: {
      CheckBuiltinThreadStateContainerCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterInit:
    case ast::Builtin::TableIterInitBind:
    case ast::Builtin::TableIterAdvance:
    case ast::Builtin::TableIterGetPCI:
    case ast::Builtin::TableIterClose: {
      CheckBuiltinTableIterCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterParallel: {
      CheckBuiltinTableIterParCall(call);
      break;
    }
    case ast::Builtin::PCIIsFiltered:
    case ast::Builtin::PCIHasNext:
    case ast::Builtin::PCIHasNextFiltered:
    case ast::Builtin::PCIAdvance:
    case ast::Builtin::PCIAdvanceFiltered:
    case ast::Builtin::PCIMatch:
    case ast::Builtin::PCIReset:
    case ast::Builtin::PCIResetFiltered:
    case ast::Builtin::PCIGetTinyInt:
    case ast::Builtin::PCIGetTinyIntNull:
    case ast::Builtin::PCIGetSmallInt:
    case ast::Builtin::PCIGetSmallIntNull:
    case ast::Builtin::PCIGetInt:
    case ast::Builtin::PCIGetIntNull:
    case ast::Builtin::PCIGetBigInt:
    case ast::Builtin::PCIGetBigIntNull:
    case ast::Builtin::PCIGetReal:
    case ast::Builtin::PCIGetRealNull:
    case ast::Builtin::PCIGetDouble:
    case ast::Builtin::PCIGetDoubleNull:
    case ast::Builtin::PCIGetDate:
    case ast::Builtin::PCIGetDateNull:
    case ast::Builtin::PCIGetVarlen:
    case ast::Builtin::PCIGetVarlenNull: {
      CheckBuiltinPCICall(call, builtin);
      break;
    }
    case ast::Builtin::Hash: {
      CheckBuiltinHashCall(call, builtin);
      break;
    }
    case ast::Builtin::FilterManagerInit:
    case ast::Builtin::FilterManagerInsertFilter:
    case ast::Builtin::FilterManagerFinalize:
    case ast::Builtin::FilterManagerRunFilters:
    case ast::Builtin::FilterManagerFree: {
      CheckBuiltinFilterManagerCall(call, builtin);
      break;
    }
    case ast::Builtin::AggHashTableInit:
    case ast::Builtin::AggHashTableInsert:
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
    case ast::Builtin::JoinHashTableIterInit: {
      CheckBuiltinJoinHashTableIterInit(call);
      break;
    }
    case ast::Builtin::JoinHashTableIterHasNext: {
      CheckBuiltinJoinHashTableIterHasNext(call);
      break;
    }
    case ast::Builtin::JoinHashTableIterGetRow: {
      CheckBuiltinJoinHashTableIterGetRow(call);
      break;
    }
    case ast::Builtin::JoinHashTableIterClose: {
      CheckBuiltinJoinHashTableIterClose(call);
      break;
    }
    case ast::Builtin::JoinHashTableBuild:
    case ast::Builtin::JoinHashTableBuildParallel: {
      CheckBuiltinJoinHashTableBuild(call, builtin);
      break;
    }
    case ast::Builtin::JoinHashTableFree: {
      CheckBuiltinJoinHashTableFree(call);
      break;
    }
    case ast::Builtin::SorterInit: {
      CheckBuiltinSorterInit(call);
      break;
    }
    case ast::Builtin::SorterInsert: {
      CheckBuiltinSorterInsert(call);
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
    case ast::Builtin::SorterIterGetRow:
    case ast::Builtin::SorterIterClose: {
      CheckBuiltinSorterIterCall(call, builtin);
      break;
    }
    case ast::Builtin::SizeOf: {
      CheckBuiltinSizeOfCall(call);
      break;
    }
    case ast::Builtin::OutputAlloc: {
      CheckBuiltinOutputAlloc(call);
      break;
    }
    case ast::Builtin::OutputFinalize: {
      CheckBuiltinOutputFinalize(call);
      break;
    }
    case ast::Builtin::IndexIteratorInit:
    case ast::Builtin::IndexIteratorInitBind: {
      CheckBuiltinIndexIteratorInit(call, builtin);
      break;
    }
    case ast::Builtin::IndexIteratorScanKey: {
      CheckBuiltinIndexIteratorScanKey(call);
      break;
    }
    case ast::Builtin::IndexIteratorAdvance: {
      CheckBuiltinIndexIteratorAdvance(call);
      break;
    }
    case ast::Builtin::IndexIteratorGetTinyInt:
    case ast::Builtin::IndexIteratorGetSmallInt:
    case ast::Builtin::IndexIteratorGetInt:
    case ast::Builtin::IndexIteratorGetBigInt:
    case ast::Builtin::IndexIteratorGetDouble:
    case ast::Builtin::IndexIteratorGetReal:
    case ast::Builtin::IndexIteratorGetTinyIntNull:
    case ast::Builtin::IndexIteratorGetSmallIntNull:
    case ast::Builtin::IndexIteratorGetIntNull:
    case ast::Builtin::IndexIteratorGetBigIntNull:
    case ast::Builtin::IndexIteratorGetDoubleNull:
    case ast::Builtin::IndexIteratorGetRealNull: {
      CheckBuiltinIndexIteratorGet(call, builtin);
      break;
    }
    case ast::Builtin::IndexIteratorSetKeyTinyInt:
    case ast::Builtin::IndexIteratorSetKeySmallInt:
    case ast::Builtin::IndexIteratorSetKeyInt:
    case ast::Builtin::IndexIteratorSetKeyBigInt:
    case ast::Builtin::IndexIteratorSetKeyDouble:
    case ast::Builtin::IndexIteratorSetKeyReal:
    case ast::Builtin::IndexIteratorSetKeyTinyIntNull:
    case ast::Builtin::IndexIteratorSetKeySmallIntNull:
    case ast::Builtin::IndexIteratorSetKeyIntNull:
    case ast::Builtin::IndexIteratorSetKeyBigIntNull:
    case ast::Builtin::IndexIteratorSetKeyDoubleNull:
    case ast::Builtin::IndexIteratorSetKeyRealNull: {
      CheckBuiltinIndexIteratorSetKey(call, builtin);
      break;
    }
    case ast::Builtin::IndexIteratorFree: {
      CheckBuiltinIndexIteratorFree(call);
      break;
    }
    case ast::Builtin::ACos:
    case ast::Builtin::ASin:
    case ast::Builtin::ATan:
    case ast::Builtin::ATan2:
    case ast::Builtin::Cos:
    case ast::Builtin::Cot:
    case ast::Builtin::Sin:
    case ast::Builtin::Tan: {
      CheckMathTrigCall(call, builtin);
      break;
    }
    default: {
      UNREACHABLE("Unhandled builtin!");
    }
  }
}

}  // namespace terrier::execution::sema
