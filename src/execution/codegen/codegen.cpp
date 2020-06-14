#include "execution/codegen/codegen.h"

#include "common/exception.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/builtins.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/codegen/executable_query_builder.h"
#include "spdlog/fmt/fmt.h"

namespace terrier::execution::codegen {

//===----------------------------------------------------------------------===//
//
// Scopes
//
//===----------------------------------------------------------------------===//

std::string CodeGen::Scope::GetFreshName(const std::string &name) {
  // Attempt insert.
  auto insert_result = names_.insert(std::make_pair(name, 1));
  if (insert_result.second) {
    return insert_result.first->getKey().str();
  }
  // Duplicate found. Find a new version that hasn't already been declared.
  uint64_t &id = insert_result.first->getValue();
  while (true) {
    auto next_name = name + std::to_string(id++);
    if (names_.find(next_name) == names_.end()) {
      return next_name;
    }
  }
}

//===----------------------------------------------------------------------===//
//
// Code Generator
//
//===----------------------------------------------------------------------===//

CodeGen::CodeGen(ast::Context *context) : context_(context), position_{0, 0}, num_cached_scopes_(0), scope_(nullptr) {
  for (auto &scope : scope_cache_) {
    scope = std::make_unique<Scope>(nullptr);
  }
  num_cached_scopes_ = kDefaultScopeCacheSize;
  EnterScope();
}

CodeGen::~CodeGen() { ExitScope(); }

ast::Expr *CodeGen::ConstBool(bool val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewBoolLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return expr;
}

ast::Expr *CodeGen::Const8(int8_t val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int8));
  return expr;
}

ast::Expr *CodeGen::Const16(int16_t val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int16));
  return expr;
}

ast::Expr *CodeGen::Const32(int32_t val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int32));
  return expr;
}

ast::Expr *CodeGen::Const64(int64_t val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewIntLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Int64));
  return expr;
}

ast::Expr *CodeGen::ConstDouble(double val) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewFloatLiteral(position_, val);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Float32));
  return expr;
}

ast::Expr *CodeGen::ConstString(std::string_view str) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewStringLiteral(position_, MakeIdentifier(str));
  expr->SetType(ast::StringType::Get(context_));
  return expr;
}

ast::VariableDecl *CodeGen::DeclareVar(ast::Identifier name, ast::Expr *type_repr, ast::Expr *init) {
  // Create a unique name for the variable
  ast::IdentifierExpr *var_name = MakeExpr(name);
  // Build and append the declaration
  return context_->GetNodeFactory()->NewVariableDecl(position_, var_name->Name(), type_repr, init);
}

ast::VariableDecl *CodeGen::DeclareVarNoInit(ast::Identifier name, ast::Expr *type_repr) {
  return DeclareVar(name, type_repr, nullptr);
}

ast::VariableDecl *CodeGen::DeclareVarNoInit(ast::Identifier name, ast::BuiltinType::Kind kind) {
  return DeclareVarNoInit(name, BuiltinType(kind));
}

ast::VariableDecl *CodeGen::DeclareVarWithInit(ast::Identifier name, ast::Expr *init) {
  return DeclareVar(name, nullptr, init);
}

ast::StructDecl *CodeGen::DeclareStruct(ast::Identifier name, util::RegionVector<ast::FieldDecl *> &&fields) const {
  auto type_repr = context_->GetNodeFactory()->NewStructType(position_, std::move(fields));
  return context_->GetNodeFactory()->NewStructDecl(position_, name, type_repr);
}

ast::Stmt *CodeGen::Assign(ast::Expr *dest, ast::Expr *value) {
  // TODO(pmenon): Check types?
  // Set the type of the destination
  dest->SetType(value->GetType());
  // Done.
  return context_->GetNodeFactory()->NewAssignmentStmt(position_, dest, value);
}

ast::Expr *CodeGen::BuiltinType(ast::BuiltinType::Kind builtin_kind) const {
  // Lookup the builtin type. We'll use it to construct an identifier.
  ast::BuiltinType *type = ast::BuiltinType::Get(context_, builtin_kind);
  // Build an identifier expression using the builtin's name
  ast::Expr *expr = MakeExpr(context_->GetIdentifier(type->GetTplName()));
  // Set the type to avoid double-checking the type
  expr->SetType(type);
  // Done
  return expr;
}

ast::Expr *CodeGen::BoolType() const { return BuiltinType(ast::BuiltinType::Bool); }

ast::Expr *CodeGen::Int8Type() const { return BuiltinType(ast::BuiltinType::Int8); }

ast::Expr *CodeGen::Int16Type() const { return BuiltinType(ast::BuiltinType::Int16); }

ast::Expr *CodeGen::Int32Type() const { return BuiltinType(ast::BuiltinType::Int32); }

ast::Expr *CodeGen::Int64Type() const { return BuiltinType(ast::BuiltinType::Int64); }

ast::Expr *CodeGen::Float32Type() const { return BuiltinType(ast::BuiltinType::Float32); }

ast::Expr *CodeGen::Float64Type() const { return BuiltinType(ast::BuiltinType::Float64); }

ast::Expr *CodeGen::PointerType(ast::Expr *base_type_repr) const {
  // Create the type representation
  auto *type_repr = context_->GetNodeFactory()->NewPointerType(position_, base_type_repr);
  // Set the actual TPL type
  if (base_type_repr->GetType() != nullptr) {
    type_repr->SetType(ast::PointerType::Get(base_type_repr->GetType()));
  }
  // Done
  return type_repr;
}

ast::Expr *CodeGen::PointerType(ast::Identifier type_name) const { return PointerType(MakeExpr(type_name)); }

ast::Expr *CodeGen::PointerType(ast::BuiltinType::Kind builtin) const { return PointerType(BuiltinType(builtin)); }

ast::Expr *CodeGen::TplType(sql::TypeId type) {
  switch (type) {
    case sql::TypeId::Boolean:
      return BuiltinType(ast::BuiltinType::Boolean);
    case sql::TypeId::TinyInt:
    case sql::TypeId::SmallInt:
    case sql::TypeId::Integer:
    case sql::TypeId::BigInt:
      return BuiltinType(ast::BuiltinType::Integer);
    case sql::TypeId::Date:
      return BuiltinType(ast::BuiltinType::Date);
    case sql::TypeId::Timestamp:
      return BuiltinType(ast::BuiltinType::Timestamp);
    case sql::TypeId::Double:
    case sql::TypeId::Float:
      return BuiltinType(ast::BuiltinType::Real);
    case sql::TypeId::Varchar:
      return BuiltinType(ast::BuiltinType::StringVal);
    default:
      UNREACHABLE("Cannot codegen unsupported type.");
  }
}

ast::Expr *CodeGen::AggregateType(planner::ExpressionType agg_type, TypeId ret_type) const {
  switch (agg_type) {
    case planner::ExpressionType::AGGREGATE_COUNT:
      return BuiltinType(ast::BuiltinType::Kind::CountAggregate);
    case planner::ExpressionType::AGGREGATE_AVG:
      return BuiltinType(ast::BuiltinType::AvgAggregate);
    case planner::ExpressionType::AGGREGATE_MIN:
      if (IsTypeIntegral(ret_type)) {
        return BuiltinType(ast::BuiltinType::IntegerMinAggregate);
      } else if (IsTypeFloatingPoint(ret_type)) {
        return BuiltinType(ast::BuiltinType::RealMinAggregate);
      } else if (ret_type == TypeId::Date) {
        return BuiltinType(ast::BuiltinType::DateMinAggregate);
      } else if (ret_type == TypeId::Varchar) {
        return BuiltinType(ast::BuiltinType::StringMinAggregate);
      } else {
        throw NotImplementedException(fmt::format("MIN() aggregates on type {}", TypeIdToString(ret_type)));
      }
    case planner::ExpressionType::AGGREGATE_MAX:
      if (IsTypeIntegral(ret_type)) {
        return BuiltinType(ast::BuiltinType::IntegerMaxAggregate);
      } else if (IsTypeFloatingPoint(ret_type)) {
        return BuiltinType(ast::BuiltinType::RealMaxAggregate);
      } else if (ret_type == TypeId::Date) {
        return BuiltinType(ast::BuiltinType::DateMaxAggregate);
      } else if (ret_type == TypeId::Varchar) {
        return BuiltinType(ast::BuiltinType::StringMaxAggregate);
      } else {
        throw NotImplementedException(fmt::format("MAX() aggregates on type {}", TypeIdToString(ret_type)));
      }
    case planner::ExpressionType::AGGREGATE_SUM:
      TPL_ASSERT(IsTypeNumeric(ret_type), "Only arithmetic types have sums.");
      if (IsTypeIntegral(ret_type)) {
        return BuiltinType(ast::BuiltinType::IntegerSumAggregate);
      }
      return BuiltinType(ast::BuiltinType::RealSumAggregate);
    default: {
      UNREACHABLE("AggregateType() should only be called with aggregates.");
    }
  }
}

ast::Expr *CodeGen::Nil() const {
  ast::Expr *expr = context_->GetNodeFactory()->NewNilLiteral(position_);
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return expr;
}

ast::Expr *CodeGen::AddressOf(ast::Expr *obj) const { return UnaryOp(parsing::Token::Type::AMPERSAND, obj); }

ast::Expr *CodeGen::AddressOf(ast::Identifier obj_name) const {
  return UnaryOp(parsing::Token::Type::AMPERSAND, MakeExpr(obj_name));
}

ast::Expr *CodeGen::SizeOf(ast::Identifier type_name) const {
  return CallBuiltin(ast::Builtin::SizeOf, {MakeExpr(type_name)});
}

ast::Expr *CodeGen::OffsetOf(ast::Identifier obj, ast::Identifier member) const {
  return CallBuiltin(ast::Builtin::OffsetOf, {MakeExpr(obj), MakeExpr(member)});
}

ast::Expr *CodeGen::PtrCast(ast::Expr *base, ast::Expr *arg) const {
  ast::Expr *ptr = context_->GetNodeFactory()->NewUnaryOpExpr(position_, parsing::Token::Type::STAR, base);
  return CallBuiltin(ast::Builtin::PtrCast, {ptr, arg});
}

ast::Expr *CodeGen::PtrCast(ast::Identifier base_name, ast::Expr *arg) const {
  return PtrCast(MakeExpr(base_name), arg);
}

ast::Expr *CodeGen::BinaryOp(parsing::Token::Type op, ast::Expr *left, ast::Expr *right) const {
  TPL_ASSERT(parsing::Token::IsBinaryOp(op), "Provided operation isn't binary");
  return context_->GetNodeFactory()->NewBinaryOpExpr(position_, op, left, right);
}

ast::Expr *CodeGen::Compare(parsing::Token::Type op, ast::Expr *left, ast::Expr *right) const {
  return context_->GetNodeFactory()->NewComparisonOpExpr(position_, op, left, right);
}

ast::Expr *CodeGen::IsNilPointer(ast::Expr *obj) const {
  return Compare(parsing::Token::Type::EQUAL_EQUAL, obj, Nil());
}

ast::Expr *CodeGen::UnaryOp(parsing::Token::Type op, ast::Expr *input) const {
  return context_->GetNodeFactory()->NewUnaryOpExpr(position_, op, input);
}

ast::Expr *CodeGen::AccessStructMember(ast::Expr *object, ast::Identifier member) {
  return context_->GetNodeFactory()->NewMemberExpr(position_, object, MakeExpr(member));
}

ast::Stmt *CodeGen::Return() { return Return(nullptr); }

ast::Stmt *CodeGen::Return(ast::Expr *ret) {
  ast::Stmt *stmt = context_->GetNodeFactory()->NewReturnStmt(position_, ret);
  NewLine();
  return stmt;
}

ast::Expr *CodeGen::Call(ast::Identifier func_name, std::initializer_list<ast::Expr *> args) const {
  util::RegionVector<ast::Expr *> call_args(args, context_->GetRegion());
  return context_->GetNodeFactory()->NewCallExpr(MakeExpr(func_name), std::move(call_args));
}

ast::Expr *CodeGen::Call(ast::Identifier func_name, const std::vector<ast::Expr *> &args) const {
  util::RegionVector<ast::Expr *> call_args(args.begin(), args.end(), context_->GetRegion());
  return context_->GetNodeFactory()->NewCallExpr(MakeExpr(func_name), std::move(call_args));
}

ast::Expr *CodeGen::CallBuiltin(ast::Builtin builtin, std::initializer_list<ast::Expr *> args) const {
  util::RegionVector<ast::Expr *> call_args(args, context_->GetRegion());
  ast::Expr *func = MakeExpr(context_->GetIdentifier(ast::Builtins::GetFunctionName(builtin)));
  ast::Expr *call = context_->GetNodeFactory()->NewBuiltinCallExpr(func, std::move(call_args));
  return call;
}

// This is copied from the overloaded function. But, we use initializer so often we keep it around.
ast::Expr *CodeGen::CallBuiltin(ast::Builtin builtin, const std::vector<ast::Expr *> &args) const {
  util::RegionVector<ast::Expr *> call_args(args.begin(), args.end(), context_->GetRegion());
  ast::Expr *func = MakeExpr(context_->GetIdentifier(ast::Builtins::GetFunctionName(builtin)));
  ast::Expr *call = context_->GetNodeFactory()->NewBuiltinCallExpr(func, std::move(call_args));
  return call;
}

ast::Expr *CodeGen::BoolToSql(bool b) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::BoolToSql, {ConstBool(b)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Boolean));
  return call;
}

ast::Expr *CodeGen::IntToSql(int64_t num) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::IntToSql, {Const64(num)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Integer));
  return call;
}

ast::Expr *CodeGen::FloatToSql(double num) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::FloatToSql, {ConstDouble(num)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Real));
  return call;
}

ast::Expr *CodeGen::DateToSql(Date date) const {
  int32_t year, month, day;
  date.ExtractComponents(&year, &month, &day);
  return DateToSql(year, month, day);
}

ast::Expr *CodeGen::DateToSql(int32_t year, int32_t month, int32_t day) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::DateToSql, {Const32(year), Const32(month), Const32(day)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Date));
  return call;
}

ast::Expr *CodeGen::StringToSql(std::string_view str) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::StringToSql, {ConstString(str)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::StringVal));
  return call;
}

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

ast::Expr *CodeGen::TableIterInit(ast::Expr *table_iter, std::string_view table_name) {
  ast::Expr *call = CallBuiltin(ast::Builtin::TableIterInit, {table_iter, ConstString(table_name)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::TableIterAdvance(ast::Expr *table_iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::TableIterAdvance, {table_iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::TableIterGetVPI(ast::Expr *table_iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::TableIterGetVPI, {table_iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::VectorProjectionIterator)->PointerTo());
  return call;
}

ast::Expr *CodeGen::TableIterClose(ast::Expr *table_iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::TableIterClose, {table_iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::IterateTableParallel(std::string_view table_name, ast::Expr *query_state, ast::Expr *tls,
                                         ast::Identifier worker_name) {
  ast::Expr *call =
      CallBuiltin(ast::Builtin::TableIterParallel, {ConstString(table_name), query_state, tls, MakeExpr(worker_name)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Vector Projection Iterator
// ---------------------------------------------------------

ast::Expr *CodeGen::VPIIsFiltered(ast::Expr *vpi) {
  ast::Expr *call = CallBuiltin(ast::Builtin::VPIIsFiltered, {vpi});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::VPIHasNext(ast::Expr *vpi, bool filtered) {
  ast::Builtin builtin = filtered ? ast::Builtin::VPIHasNextFiltered : ast::Builtin::VPIHasNext;
  ast::Expr *call = CallBuiltin(builtin, {vpi});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::VPIAdvance(ast::Expr *vpi, bool filtered) {
  ast::Builtin builtin = filtered ? ast::Builtin ::VPIAdvanceFiltered : ast::Builtin ::VPIAdvance;
  ast::Expr *call = CallBuiltin(builtin, {vpi});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::VPIMatch(ast::Expr *vpi, ast::Expr *cond) {
  ast::Expr *call = CallBuiltin(ast::Builtin::VPIMatch, {vpi, cond});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::VPIInit(ast::Expr *vpi, ast::Expr *vp, ast::Expr *tids) {
  ast::Expr *call = nullptr;
  if (tids != nullptr) {
    call = CallBuiltin(ast::Builtin::VPIInit, {vpi, vp, tids});
  } else {
    call = CallBuiltin(ast::Builtin::VPIInit, {vpi, vp});
  }
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::VPIGet(ast::Expr *vpi, sql::TypeId type_id, bool nullable, uint32_t idx) {
  ast::Builtin builtin;
  ast::BuiltinType::Kind ret_kind;
  switch (type_id) {
    case sql::TypeId::Boolean:
      builtin = ast::Builtin::VPIGetBool;
      ret_kind = ast::BuiltinType::Boolean;
      break;
    case sql::TypeId::TinyInt:
      builtin = ast::Builtin::VPIGetTinyInt;
      ret_kind = ast::BuiltinType::Integer;
      break;
    case sql::TypeId::SmallInt:
      builtin = ast::Builtin::VPIGetSmallInt;
      ret_kind = ast::BuiltinType::Integer;
      break;
    case sql::TypeId::Integer:
      builtin = ast::Builtin::VPIGetInt;
      ret_kind = ast::BuiltinType::Integer;
      break;
    case sql::TypeId::BigInt:
      builtin = ast::Builtin::VPIGetBigInt;
      ret_kind = ast::BuiltinType::Integer;
      break;
    case sql::TypeId::Float:
      builtin = ast::Builtin::VPIGetReal;
      ret_kind = ast::BuiltinType::Real;
      break;
    case sql::TypeId::Double:
      builtin = ast::Builtin::VPIGetDouble;
      ret_kind = ast::BuiltinType::Real;
      break;
    case sql::TypeId::Date:
      builtin = ast::Builtin::VPIGetDate;
      ret_kind = ast::BuiltinType::Date;
      break;
    case sql::TypeId::Varchar:
      builtin = ast::Builtin::VPIGetString;
      ret_kind = ast::BuiltinType::StringVal;
      break;
    default:
      throw NotImplementedException(
          fmt::format("CodeGen: Reading type {} from VPI not supported.", TypeIdToString(type_id)));
  }
  ast::Expr *call = CallBuiltin(builtin, {vpi, Const32(idx)});
  call->SetType(ast::BuiltinType::Get(context_, ret_kind));
  return call;
}

ast::Expr *CodeGen::VPIFilter(ast::Expr *vp, planner::ExpressionType comp_type, uint32_t col_idx, ast::Expr *filter_val,
                              ast::Expr *tids) {
  // Call @FilterComp(vpi, col_idx, col_type, filter_val)
  ast::Builtin builtin;
  switch (comp_type) {
    case planner::ExpressionType::COMPARE_EQUAL:
      builtin = ast::Builtin::VectorFilterEqual;
      break;
    case planner::ExpressionType::COMPARE_NOT_EQUAL:
      builtin = ast::Builtin::VectorFilterNotEqual;
      break;
    case planner::ExpressionType::COMPARE_LESS_THAN:
      builtin = ast::Builtin::VectorFilterLessThan;
      break;
    case planner::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::VectorFilterLessThanEqual;
      break;
    case planner::ExpressionType::COMPARE_GREATER_THAN:
      builtin = ast::Builtin::VectorFilterGreaterThan;
      break;
    case planner::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::VectorFilterGreaterThanEqual;
      break;
    default:
      throw NotImplementedException(fmt::format("CodeGen: Vector filter type {} from VPI not supported.",
                                                planner::ExpressionTypeToString(comp_type, true)));
  }
  ast::Expr *call = CallBuiltin(builtin, {vp, Const32(col_idx), filter_val, tids});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

ast::Expr *CodeGen::FilterManagerInit(ast::Expr *filter_manager) {
  ast::Expr *call = CallBuiltin(ast::Builtin::FilterManagerInit, {filter_manager});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::FilterManagerFree(ast::Expr *filter_manager) {
  ast::Expr *call = CallBuiltin(ast::Builtin::FilterManagerFree, {filter_manager});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::FilterManagerInsert(ast::Expr *filter_manager,
                                        const std::vector<ast::Identifier> &clause_fn_names) {
  std::vector<ast::Expr *> params(1 + clause_fn_names.size());
  params[0] = filter_manager;
  for (uint32_t i = 0; i < clause_fn_names.size(); i++) {
    params[i + 1] = MakeExpr(clause_fn_names[i]);
  }
  ast::Expr *call = CallBuiltin(ast::Builtin::FilterManagerInsertFilter, params);
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::FilterManagerRunFilters(ast::Expr *filter_manager, ast::Expr *vpi) {
  ast::Expr *call = CallBuiltin(ast::Builtin::FilterManagerRunFilters, {filter_manager, vpi});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::ExecCtxGetMemoryPool(ast::Expr *exec_ctx) {
  ast::Expr *call = CallBuiltin(ast::Builtin::ExecutionContextGetMemoryPool, {exec_ctx});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::MemoryPool)->PointerTo());
  return call;
}

ast::Expr *CodeGen::ExecCtxGetTLS(ast::Expr *exec_ctx) {
  ast::Expr *call = CallBuiltin(ast::Builtin::ExecutionContextGetTLS, {exec_ctx});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::ThreadStateContainer)->PointerTo());
  return call;
}

ast::Expr *CodeGen::TLSAccessCurrentThreadState(ast::Expr *tls, ast::Identifier state_type_name) {
  ast::Expr *call = CallBuiltin(ast::Builtin::ThreadStateContainerGetState, {tls});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  return PtrCast(state_type_name, call);
}

ast::Expr *CodeGen::TLSIterate(ast::Expr *tls, ast::Expr *context, ast::Identifier func) {
  ast::Expr *call = CallBuiltin(ast::Builtin::ThreadStateContainerIterate, {tls, context, MakeExpr(func)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::TLSReset(ast::Expr *tls, ast::Identifier tls_state_name, ast::Identifier init_fn,
                             ast::Identifier tear_down_fn, ast::Expr *context) {
  ast::Expr *call = CallBuiltin(ast::Builtin::ThreadStateContainerReset,
                                {tls, SizeOf(tls_state_name), MakeExpr(init_fn), MakeExpr(tear_down_fn), context});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::TLSClear(ast::Expr *tls) {
  ast::Expr *call = CallBuiltin(ast::Builtin::ThreadStateContainerClear, {tls});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Hash
// ---------------------------------------------------------

ast::Expr *CodeGen::Hash(const std::vector<ast::Expr *> &values) {
  ast::Expr *call = CallBuiltin(ast::Builtin::Hash, values);
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint64));
  return call;
}

// ---------------------------------------------------------
// Joins
// ---------------------------------------------------------

ast::Expr *CodeGen::JoinHashTableInit(ast::Expr *join_hash_table, ast::Expr *mem_pool,
                                      ast::Identifier build_row_type_name) {
  ast::Expr *call =
      CallBuiltin(ast::Builtin::JoinHashTableInit, {join_hash_table, mem_pool, SizeOf(build_row_type_name)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::JoinHashTableInsert(ast::Expr *join_hash_table, ast::Expr *hash_val,
                                        ast::Identifier tuple_type_name) {
  ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableInsert, {join_hash_table, hash_val});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return PtrCast(tuple_type_name, call);
}

ast::Expr *CodeGen::JoinHashTableBuild(ast::Expr *join_hash_table) {
  ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableBuild, {join_hash_table});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::JoinHashTableBuildParallel(ast::Expr *join_hash_table, ast::Expr *thread_state_container,
                                               ast::Expr *offset) {
  ast::Expr *call =
      CallBuiltin(ast::Builtin::JoinHashTableBuildParallel, {join_hash_table, thread_state_container, offset});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::JoinHashTableLookup(ast::Expr *join_hash_table, ast::Expr *entry_iter, ast::Expr *hash_val) {
  ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableLookup, {join_hash_table, entry_iter, hash_val});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::JoinHashTableFree(ast::Expr *join_hash_table) {
  ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableFree, {join_hash_table});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::HTEntryIterHasNext(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::HashTableEntryIterHasNext, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::HTEntryIterGetRow(ast::Expr *iter, ast::Identifier row_type) {
  ast::Expr *call = CallBuiltin(ast::Builtin::HashTableEntryIterGetRow, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  return PtrCast(row_type, call);
}

// ---------------------------------------------------------
// Hash aggregations
// ---------------------------------------------------------

ast::Expr *CodeGen::AggHashTableInit(ast::Expr *agg_ht, ast::Expr *mem_pool, ast::Identifier agg_payload_type) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableInit, {agg_ht, mem_pool, SizeOf(agg_payload_type)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::AggHashTableLookup(ast::Expr *agg_ht, ast::Expr *hash_val, ast::Identifier key_check,
                                       ast::Expr *input, ast::Identifier agg_payload_type) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableLookup, {agg_ht, hash_val, MakeExpr(key_check), input});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  return PtrCast(agg_payload_type, call);
}

ast::Expr *CodeGen::AggHashTableInsert(ast::Expr *agg_ht, ast::Expr *hash_val, bool partitioned,
                                       ast::Identifier agg_payload_type) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableInsert, {agg_ht, hash_val, ConstBool(partitioned)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  return PtrCast(agg_payload_type, call);
}

ast::Expr *CodeGen::AggHashTableLinkEntry(ast::Expr *agg_ht, ast::Expr *entry) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableLinkEntry, {agg_ht, entry});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::AggHashTableMovePartitions(ast::Expr *agg_ht, ast::Expr *tls, ast::Expr *tl_agg_ht_offset,
                                               ast::Identifier merge_partitions_fn_name) {
  std::initializer_list<ast::Expr *> args = {agg_ht, tls, tl_agg_ht_offset, MakeExpr(merge_partitions_fn_name)};
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableMovePartitions, args);
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::AggHashTableParallelScan(ast::Expr *agg_ht, ast::Expr *query_state,
                                             ast::Expr *thread_state_container, ast::Identifier worker_fn) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableParallelPartitionedScan,
                                {agg_ht, query_state, thread_state_container, MakeExpr(worker_fn)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::AggHashTableFree(ast::Expr *agg_ht) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableFree, {agg_ht});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Aggregation Hash Table Overflow Iterator
// ---------------------------------------------------------

ast::Expr *CodeGen::AggPartitionIteratorHasNext(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggPartIterHasNext, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::AggPartitionIteratorNext(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggPartIterNext, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::AggPartitionIteratorGetHash(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggPartIterGetHash, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint64));
  return call;
}

ast::Expr *CodeGen::AggPartitionIteratorGetRow(ast::Expr *iter, ast::Identifier agg_payload_type) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggPartIterGetRow, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  return PtrCast(agg_payload_type, call);
}

ast::Expr *CodeGen::AggPartitionIteratorGetRowEntry(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggPartIterGetRowEntry, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::HashTableEntry)->PointerTo());
  return call;
}

ast::Expr *CodeGen::AggHashTableIteratorInit(ast::Expr *iter, ast::Expr *agg_ht) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableIterInit, {iter, agg_ht});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::AggHashTableIteratorHasNext(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableIterHasNext, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::AggHashTableIteratorNext(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableIterNext, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::AggHashTableIteratorGetRow(ast::Expr *iter, ast::Identifier agg_payload_type) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableIterGetRow, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  return PtrCast(agg_payload_type, call);
}

ast::Expr *CodeGen::AggHashTableIteratorClose(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableIterClose, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Aggregators
// ---------------------------------------------------------

ast::Expr *CodeGen::AggregatorInit(ast::Expr *agg) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggInit, {agg});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::AggregatorAdvance(ast::Expr *agg, ast::Expr *val) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggAdvance, {agg, val});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::AggregatorMerge(ast::Expr *agg1, ast::Expr *agg2) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggMerge, {agg1, agg2});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::AggregatorResult(ast::Expr *agg) { return CallBuiltin(ast::Builtin::AggResult, {agg}); }

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

ast::Expr *CodeGen::SorterInit(ast::Expr *sorter, ast::Expr *mem_pool, ast::Identifier cmp_func_name,
                               ast::Identifier sort_row_type_name) {
  ast::Expr *call =
      CallBuiltin(ast::Builtin::SorterInit, {sorter, mem_pool, MakeExpr(cmp_func_name), SizeOf(sort_row_type_name)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterInsert(ast::Expr *sorter, ast::Identifier sort_row_type_name) {
  // @sorterInsert(sorter)
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterInsert, {sorter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  // @ptrCast(sort_row_type, @sorterInsert())
  return PtrCast(sort_row_type_name, call);
}

ast::Expr *CodeGen::SorterInsertTopK(ast::Expr *sorter, ast::Identifier sort_row_type_name, uint64_t top_k) {
  // @sorterInsertTopK(sorter)
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterInsertTopK, {sorter, Const64(top_k)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  // @ptrCast(sort_row_type, @sorterInsertTopK())
  return PtrCast(sort_row_type_name, call);
}

ast::Expr *CodeGen::SorterInsertTopKFinish(ast::Expr *sorter, uint64_t top_k) {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterInsertTopKFinish, {sorter, Const64(top_k)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterSort(ast::Expr *sorter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterSort, {sorter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SortParallel(ast::Expr *sorter, ast::Expr *tls, ast::Expr *offset) {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterSortParallel, {sorter, tls, offset});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SortTopKParallel(ast::Expr *sorter, ast::Expr *tls, ast::Expr *offset, std::size_t top_k) {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterSortTopKParallel, {sorter, tls, offset, Const64(top_k)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterFree(ast::Expr *sorter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterFree, {sorter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterIterInit(ast::Expr *iter, ast::Expr *sorter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterInit, {iter, sorter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterIterHasNext(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterHasNext, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::SorterIterNext(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterNext, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterIterSkipRows(ast::Expr *iter, uint32_t n) {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterSkipRows, {iter, Const64(n)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::SorterIterGetRow(ast::Expr *iter, ast::Identifier row_type_name) {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterGetRow, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  return PtrCast(row_type_name, call);
}

ast::Expr *CodeGen::SorterIterClose(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::SorterIterClose, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// SQL functions
// ---------------------------------------------------------

ast::Expr *CodeGen::Like(ast::Expr *str, ast::Expr *pattern) {
  ast::Expr *call = CallBuiltin(ast::Builtin::Like, {str, pattern});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::NotLike(ast::Expr *str, ast::Expr *pattern) {
  return UnaryOp(parsing::Token::Type::BANG, Like(str, pattern));
}

// ---------------------------------------------------------
// CSV
// ---------------------------------------------------------

ast::Expr *CodeGen::CSVReaderInit(ast::Expr *reader, std::string_view file_name) {
  ast::Expr *call = CallBuiltin(ast::Builtin::CSVReaderInit, {reader, ConstString(file_name)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::CSVReaderAdvance(ast::Expr *reader) {
  ast::Expr *call = CallBuiltin(ast::Builtin::CSVReaderAdvance, {reader});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::CSVReaderGetField(ast::Expr *reader, uint32_t field_index, ast::Expr *result) {
  ast::Expr *call = CallBuiltin(ast::Builtin::CSVReaderGetField, {reader, Const32(field_index), result});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::CSVReaderGetRecordNumber(ast::Expr *reader) {
  ast::Expr *call = CallBuiltin(ast::Builtin::CSVReaderGetRecordNumber, {reader});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint32));
  return call;
}

ast::Expr *CodeGen::CSVReaderClose(ast::Expr *reader) {
  ast::Expr *call = CallBuiltin(ast::Builtin::CSVReaderClose, {reader});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Extras
// ---------------------------------------------------------

ast::Identifier CodeGen::MakeFreshIdentifier(const std::string &str) {
  return context_->GetIdentifier(scope_->GetFreshName(str));
}

ast::Identifier CodeGen::MakeIdentifier(std::string_view str) const {
  return context_->GetIdentifier({str.data(), str.length()});
}

ast::IdentifierExpr *CodeGen::MakeExpr(ast::Identifier ident) const {
  return context_->GetNodeFactory()->NewIdentifierExpr(position_, ident);
}

ast::Stmt *CodeGen::MakeStmt(ast::Expr *expr) const { return context_->GetNodeFactory()->NewExpressionStmt(expr); }

ast::BlockStmt *CodeGen::MakeEmptyBlock() const {
  return context_->GetNodeFactory()->NewBlockStmt(position_, position_, {{}, context_->GetRegion()});
}

util::RegionVector<ast::FieldDecl *> CodeGen::MakeEmptyFieldList() const {
  return util::RegionVector<ast::FieldDecl *>(context_->GetRegion());
}

util::RegionVector<ast::FieldDecl *> CodeGen::MakeFieldList(std::initializer_list<ast::FieldDecl *> fields) const {
  return util::RegionVector<ast::FieldDecl *>(fields, context_->GetRegion());
}

ast::FieldDecl *CodeGen::MakeField(ast::Identifier name, ast::Expr *type) const {
  return context_->GetNodeFactory()->NewFieldDecl(position_, name, type);
}

ast::AstNodeFactory *CodeGen::GetFactory() { return context_->GetNodeFactory(); }

void CodeGen::EnterScope() {
  if (num_cached_scopes_ == 0) {
    scope_ = new Scope(scope_);
  } else {
    auto scope = scope_cache_[--num_cached_scopes_].release();
    scope->Init(scope_);
    scope_ = scope;
  }
}

void CodeGen::ExitScope() {
  Scope *scope = scope_;
  scope_ = scope->Previous();

  if (num_cached_scopes_ < kDefaultScopeCacheSize) {
    scope_cache_[num_cached_scopes_++].reset(scope);
  } else {
    delete scope;
  }
}

}  // namespace terrier::execution::codegen
