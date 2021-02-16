#include "execution/compiler/codegen.h"

#include "common/error/exception.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/builtins.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/compiler/executable_query_builder.h"
#include "spdlog/fmt/fmt.h"
#include "storage/index/index_defs.h"

namespace noisepage::execution::compiler {

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

CodeGen::CodeGen(ast::Context *context, catalog::CatalogAccessor *accessor)
    : context_(context),
      position_{0, 0},
      num_cached_scopes_(0),
      scope_(nullptr),
      accessor_(accessor),
      pipeline_operating_units_(std::make_unique<selfdriving::PipelineOperatingUnits>()) {
  for (auto &scope : scope_cache_) {
    scope = std::make_unique<Scope>(nullptr);
  }
  num_cached_scopes_ = DEFAULT_SCOPE_CACHE_SIZE;
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
  expr->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Float64));
  return expr;
}

ast::Expr *CodeGen::ConstString(std::string_view str) const {
  ast::Expr *expr = context_->GetNodeFactory()->NewStringLiteral(position_, MakeIdentifier(str));
  expr->SetType(ast::StringType::Get(context_));
  return expr;
}

ast::Expr *CodeGen::ConstNull(type::TypeId type) const {
  ast::Expr *dummy_expr;
  // initSqlNull(&expr) produces a NULL of expr's type.
  switch (type) {
    case type::TypeId::BOOLEAN:
      dummy_expr = BoolToSql(false);
      break;
    case type::TypeId::TINYINT:   // fallthrough
    case type::TypeId::SMALLINT:  // fallthrough
    case type::TypeId::INTEGER:   // fallthrough
    case type::TypeId::BIGINT:
      dummy_expr = IntToSql(0);
      break;
    case type::TypeId::DATE:
      dummy_expr = DateToSql(0, 0, 0);
      break;
    case type::TypeId::TIMESTAMP:
      dummy_expr = TimestampToSql(0);
      break;
    case type::TypeId::VARCHAR:
      dummy_expr = StringToSql("");
      break;
    case type::TypeId::REAL:
      dummy_expr = FloatToSql(0.0);
      break;
    case type::TypeId::VARBINARY:
    default:
      UNREACHABLE("Unsupported NULL type!");
  }
  return CallBuiltin(ast::Builtin::InitSqlNull, {PointerType(dummy_expr)});
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

ast::Expr *CodeGen::ArrayType(uint64_t num_elems, ast::BuiltinType::Kind kind) {
  return GetFactory()->NewArrayType(position_, Const64(num_elems), BuiltinType(kind));
}

ast::Expr *CodeGen::ArrayAccess(ast::Identifier arr, uint64_t idx) {
  return GetFactory()->NewIndexExpr(position_, MakeExpr(arr), Const64(idx));
}

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
    case sql::TypeId::Varbinary:
      return BuiltinType(ast::BuiltinType::StringVal);
    default:
      UNREACHABLE("Cannot codegen unsupported type.");
  }
}

ast::Expr *CodeGen::AggregateType(parser::ExpressionType agg_type, sql::TypeId ret_type) const {
  switch (agg_type) {
    case parser::ExpressionType::AGGREGATE_COUNT:
      return BuiltinType(ast::BuiltinType::Kind::CountAggregate);
    case parser::ExpressionType::AGGREGATE_AVG:
      return BuiltinType(ast::BuiltinType::AvgAggregate);
    case parser::ExpressionType::AGGREGATE_MIN:
      if (IsTypeIntegral(ret_type)) {
        return BuiltinType(ast::BuiltinType::IntegerMinAggregate);
      } else if (IsTypeFloatingPoint(ret_type)) {
        return BuiltinType(ast::BuiltinType::RealMinAggregate);
      } else if (ret_type == sql::TypeId::Date) {
        return BuiltinType(ast::BuiltinType::DateMinAggregate);
      } else if (ret_type == sql::TypeId::Varchar) {
        return BuiltinType(ast::BuiltinType::StringMinAggregate);
      } else {
        throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("MIN() aggregates on type {}", TypeIdToString(ret_type)));
      }
    case parser::ExpressionType::AGGREGATE_MAX:
      if (IsTypeIntegral(ret_type)) {
        return BuiltinType(ast::BuiltinType::IntegerMaxAggregate);
      } else if (IsTypeFloatingPoint(ret_type)) {
        return BuiltinType(ast::BuiltinType::RealMaxAggregate);
      } else if (ret_type == sql::TypeId::Date) {
        return BuiltinType(ast::BuiltinType::DateMaxAggregate);
      } else if (ret_type == sql::TypeId::Varchar) {
        return BuiltinType(ast::BuiltinType::StringMaxAggregate);
      } else {
        throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("MAX() aggregates on type {}", TypeIdToString(ret_type)));
      }
    case parser::ExpressionType::AGGREGATE_SUM:
      NOISEPAGE_ASSERT(IsTypeNumeric(ret_type), "Only arithmetic types have sums.");
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
  NOISEPAGE_ASSERT(parsing::Token::IsBinaryOp(op), "Provided operation isn't binary");
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

ast::Expr *CodeGen::DateToSql(sql::Date date) const {
  int32_t year, month, day;
  date.ExtractComponents(&year, &month, &day);
  return DateToSql(year, month, day);
}

ast::Expr *CodeGen::DateToSql(int32_t year, int32_t month, int32_t day) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::DateToSql, {Const32(year), Const32(month), Const32(day)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Date));
  return call;
}

ast::Expr *CodeGen::TimestampToSql(sql::Timestamp timestamp) const {
  int32_t year, month, day, hour, min, sec, millisec, microsec;
  timestamp.ExtractComponents(&year, &month, &day, &hour, &min, &sec, &millisec, &microsec);
  ast::Expr *call = CallBuiltin(ast::Builtin::TimestampToSqlYMDHMSMU,
                                {Const32(year), Const32(month), Const32(day), Const32(hour), Const32(min), Const32(sec),
                                 Const32(millisec), Const32(microsec)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Timestamp));
  return call;
}

ast::Expr *CodeGen::TimestampToSql(uint64_t julian_usec) const {
  ast::Expr *usec_expr = Const64(julian_usec);
  ast::Expr *call = CallBuiltin(ast::Builtin::TimestampToSql, {usec_expr});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Timestamp));
  return call;
}

ast::Expr *CodeGen::StringToSql(std::string_view str) const {
  ast::Expr *call = CallBuiltin(ast::Builtin::StringToSql, {ConstString(str)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::StringVal));
  return call;
}

ast::Expr *CodeGen::IndexIteratorInit(ast::Identifier iter, ast::Expr *exec_ctx_var, uint32_t num_attrs,
                                      uint32_t table_oid, uint32_t index_oid, ast::Identifier col_oids) {
  // @indexIteratorInit(&iter, table_oid, index_oid, execCtx)
  ast::Expr *iter_ptr = AddressOf(iter);
  ast::Expr *num_attrs_expr = Const32(static_cast<int32_t>(num_attrs));
  ast::Expr *table_oid_expr = Const32(static_cast<int32_t>(table_oid));
  ast::Expr *index_oid_expr = Const32(static_cast<int32_t>(index_oid));
  ast::Expr *col_oids_expr = MakeExpr(col_oids);
  std::vector<ast::Expr *> args{iter_ptr, exec_ctx_var, num_attrs_expr, table_oid_expr, index_oid_expr, col_oids_expr};
  return CallBuiltin(ast::Builtin::IndexIteratorInit, args);
}

ast::Expr *CodeGen::IndexIteratorScan(ast::Identifier iter, planner::IndexScanType scan_type, uint32_t limit) {
  // @indexIteratorScanKey(&iter)
  ast::Builtin builtin;
  bool asc_scan = false;
  bool use_limit = false;
  storage::index::ScanType asc_type;
  switch (scan_type) {
    case planner::IndexScanType::Exact:
      builtin = ast::Builtin::IndexIteratorScanKey;
      break;
    case planner::IndexScanType::AscendingClosed:
    case planner::IndexScanType::AscendingOpenHigh:
    case planner::IndexScanType::AscendingOpenLow:
    case planner::IndexScanType::AscendingOpenBoth:
      asc_scan = true;
      use_limit = true;
      builtin = ast::Builtin::IndexIteratorScanAscending;
      if (scan_type == planner::IndexScanType::AscendingClosed)
        asc_type = storage::index::ScanType::Closed;
      else if (scan_type == planner::IndexScanType::AscendingOpenHigh)
        asc_type = storage::index::ScanType::OpenHigh;
      else if (scan_type == planner::IndexScanType::AscendingOpenLow)
        asc_type = storage::index::ScanType::OpenLow;
      else if (scan_type == planner::IndexScanType::AscendingOpenBoth)
        asc_type = storage::index::ScanType::OpenBoth;
      break;
    case planner::IndexScanType::Descending:
      builtin = ast::Builtin::IndexIteratorScanDescending;
      break;
    case planner::IndexScanType::DescendingLimit:
      use_limit = true;
      builtin = ast::Builtin::IndexIteratorScanLimitDescending;
      break;
    default:
      UNREACHABLE("Unknown scan type");
  }

  if (!use_limit && !asc_scan) return CallBuiltin(builtin, {AddressOf(iter)});

  ast::Expr *iter_ptr = AddressOf(iter);
  std::vector<ast::Expr *> args{iter_ptr};

  if (asc_scan) args.push_back(Const64(static_cast<int64_t>(asc_type)));
  if (use_limit) args.push_back(Const32(limit));

  return CallBuiltin(builtin, args);
}

ast::Expr *CodeGen::PRGet(ast::Expr *pr, type::TypeId type, bool nullable, uint32_t attr_idx) {
  // @indexIteratorGetTypeNull(&iter, attr_idx)
  ast::Builtin builtin;
  switch (type) {
    case type::TypeId::BOOLEAN:
      builtin = nullable ? ast::Builtin::PRGetBoolNull : ast::Builtin::PRGetBool;
      break;
    case type::TypeId::TINYINT:
      builtin = nullable ? ast::Builtin::PRGetTinyIntNull : ast::Builtin::PRGetTinyInt;
      break;
    case type::TypeId::SMALLINT:
      builtin = nullable ? ast::Builtin::PRGetSmallIntNull : ast::Builtin::PRGetSmallInt;
      break;
    case type::TypeId::INTEGER:
      builtin = nullable ? ast::Builtin::PRGetIntNull : ast::Builtin::PRGetInt;
      break;
    case type::TypeId::BIGINT:
      builtin = nullable ? ast::Builtin::PRGetBigIntNull : ast::Builtin::PRGetBigInt;
      break;
    case type::TypeId::REAL:
      builtin = nullable ? ast::Builtin::PRGetDoubleNull : ast::Builtin::PRGetDouble;
      break;
    case type::TypeId::DATE:
      builtin = nullable ? ast::Builtin::PRGetDateNull : ast::Builtin::PRGetDate;
      break;
    case type::TypeId::TIMESTAMP:
      builtin = nullable ? ast::Builtin::PRGetTimestampNull : ast::Builtin::PRGetTimestamp;
      break;
    case type::TypeId::VARCHAR:
      builtin = nullable ? ast::Builtin::PRGetVarlenNull : ast::Builtin::PRGetVarlen;
      break;
    default:
      // TODO(amlatyr): Support other types.
      UNREACHABLE("Unsupported index get type!");
  }
  ast::Expr *idx_expr = GetFactory()->NewIntLiteral(position_, attr_idx);
  return CallBuiltin(builtin, {pr, idx_expr});
}

ast::Expr *CodeGen::PRSet(ast::Expr *pr, type::TypeId type, bool nullable, uint32_t attr_idx, ast::Expr *val,
                          bool own) {
  ast::Builtin builtin;
  switch (type) {
    case type::TypeId::BOOLEAN:
      builtin = nullable ? ast::Builtin::PRSetBoolNull : ast::Builtin::PRSetBool;
      break;
    case type::TypeId::TINYINT:
      builtin = nullable ? ast::Builtin::PRSetTinyIntNull : ast::Builtin::PRSetTinyInt;
      break;
    case type::TypeId::SMALLINT:
      builtin = nullable ? ast::Builtin::PRSetSmallIntNull : ast::Builtin::PRSetSmallInt;
      break;
    case type::TypeId::INTEGER:
      builtin = nullable ? ast::Builtin::PRSetIntNull : ast::Builtin::PRSetInt;
      break;
    case type::TypeId::BIGINT:
      builtin = nullable ? ast::Builtin::PRSetBigIntNull : ast::Builtin::PRSetBigInt;
      break;
    case type::TypeId::REAL:
      builtin = nullable ? ast::Builtin::PRSetDoubleNull : ast::Builtin::PRSetDouble;
      break;
    case type::TypeId::DATE:
      builtin = nullable ? ast::Builtin::PRSetDateNull : ast::Builtin::PRSetDate;
      break;
    case type::TypeId::TIMESTAMP:
      builtin = nullable ? ast::Builtin::PRSetTimestampNull : ast::Builtin::PRSetTimestamp;
      break;
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY:
      builtin = nullable ? ast::Builtin::PRSetVarlenNull : ast::Builtin::PRSetVarlen;
      break;
    default:
      UNREACHABLE("Unsupported index set type!");
  }
  ast::Expr *idx_expr = GetFactory()->NewIntLiteral(position_, attr_idx);
  if (builtin == ast::Builtin::PRSetVarlenNull || builtin == ast::Builtin::PRSetVarlen) {
    return CallBuiltin(builtin, {pr, idx_expr, val, ConstBool(own)});
  }
  return CallBuiltin(builtin, {pr, idx_expr, val});
}

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

catalog::CatalogAccessor *CodeGen::GetCatalogAccessor() const { return accessor_; }

ast::Expr *CodeGen::TableIterInit(ast::Expr *table_iter, ast::Expr *exec_ctx, catalog::table_oid_t table_oid,
                                  ast::Identifier col_oids) {
  ast::Expr *call = CallBuiltin(ast::Builtin::TableIterInit,
                                {table_iter, exec_ctx, Const32(table_oid.UnderlyingValue()), MakeExpr(col_oids)});
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

ast::Expr *CodeGen::IterateTableParallel(catalog::table_oid_t table_oid, ast::Identifier col_oids,
                                         ast::Expr *query_state, ast::Expr *exec_ctx, ast::Identifier worker_name) {
  ast::Expr *call = CallBuiltin(
      ast::Builtin::TableIterParallel,
      {Const32(table_oid.UnderlyingValue()), MakeExpr(col_oids), query_state, exec_ctx, MakeExpr(worker_name)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::AbortTxn(ast::Expr *exec_ctx) { return CallBuiltin(ast::Builtin::AbortTxn, {exec_ctx}); }

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
      builtin = nullable ? ast::Builtin::VPIGetBoolNull : ast::Builtin::VPIGetBool;
      ret_kind = ast::BuiltinType::Boolean;
      break;
    case sql::TypeId::TinyInt:
      builtin = nullable ? ast::Builtin::VPIGetTinyIntNull : ast::Builtin::VPIGetTinyInt;
      ret_kind = ast::BuiltinType::Integer;
      break;
    case sql::TypeId::SmallInt:
      builtin = nullable ? ast::Builtin::VPIGetSmallIntNull : ast::Builtin::VPIGetSmallInt;
      ret_kind = ast::BuiltinType::Integer;
      break;
    case sql::TypeId::Integer:
      builtin = nullable ? ast::Builtin::VPIGetIntNull : ast::Builtin::VPIGetInt;
      ret_kind = ast::BuiltinType::Integer;
      break;
    case sql::TypeId::BigInt:
      builtin = nullable ? ast::Builtin::VPIGetBigIntNull : ast::Builtin::VPIGetBigInt;
      ret_kind = ast::BuiltinType::Integer;
      break;
    case sql::TypeId::Float:
      builtin = nullable ? ast::Builtin::VPIGetRealNull : ast::Builtin::VPIGetReal;
      ret_kind = ast::BuiltinType::Real;
      break;
    case sql::TypeId::Double:
      builtin = nullable ? ast::Builtin::VPIGetDoubleNull : ast::Builtin::VPIGetDouble;
      ret_kind = ast::BuiltinType::Real;
      break;
    case sql::TypeId::Date:
      builtin = nullable ? ast::Builtin::VPIGetDateNull : ast::Builtin::VPIGetDate;
      ret_kind = ast::BuiltinType::Date;
      break;
    case sql::TypeId::Timestamp:
      builtin = nullable ? ast::Builtin::VPIGetTimestampNull : ast::Builtin::VPIGetTimestamp;
      ret_kind = ast::BuiltinType::Timestamp;
      break;
    case sql::TypeId::Varchar:
    case sql::TypeId::Varbinary:
      builtin = nullable ? ast::Builtin::VPIGetStringNull : ast::Builtin::VPIGetString;
      ret_kind = ast::BuiltinType::StringVal;
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(
          fmt::format("CodeGen: Reading type {} from VPI not supported.", TypeIdToString(type_id)));
  }
  ast::Expr *call = CallBuiltin(builtin, {vpi, Const32(idx)});
  call->SetType(ast::BuiltinType::Get(context_, ret_kind));
  return call;
}

ast::Expr *CodeGen::VPIFilter(ast::Expr *exec_ctx, ast::Expr *vp, parser::ExpressionType comp_type, uint32_t col_idx,
                              ast::Expr *filter_val, ast::Expr *tids) {
  // Call @FilterComp(execCtx, vpi, col_idx, col_type, filter_val)
  ast::Builtin builtin;
  switch (comp_type) {
    case parser::ExpressionType::COMPARE_EQUAL:
      builtin = ast::Builtin::VectorFilterEqual;
      break;
    case parser::ExpressionType::COMPARE_NOT_EQUAL:
      builtin = ast::Builtin::VectorFilterNotEqual;
      break;
    case parser::ExpressionType::COMPARE_LESS_THAN:
      builtin = ast::Builtin::VectorFilterLessThan;
      break;
    case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::VectorFilterLessThanEqual;
      break;
    case parser::ExpressionType::COMPARE_GREATER_THAN:
      builtin = ast::Builtin::VectorFilterGreaterThan;
      break;
    case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::VectorFilterGreaterThanEqual;
      break;
    case parser::ExpressionType::COMPARE_LIKE:
      builtin = ast::Builtin::VectorFilterLike;
      break;
    case parser::ExpressionType::COMPARE_NOT_LIKE:
      builtin = ast::Builtin::VectorFilterNotLike;
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("CodeGen: Vector filter type {} from VPI not supported.",
                                                  parser::ExpressionTypeToString(comp_type, true)));
  }
  ast::Expr *call = CallBuiltin(builtin, {exec_ctx, vp, Const32(col_idx), filter_val, tids});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

ast::Expr *CodeGen::FilterManagerInit(ast::Expr *filter_manager, ast::Expr *exec_ctx) {
  ast::Expr *call = CallBuiltin(ast::Builtin::FilterManagerInit, {filter_manager, exec_ctx});
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

ast::Expr *CodeGen::FilterManagerRunFilters(ast::Expr *filter_manager, ast::Expr *vpi, ast::Expr *exec_ctx) {
  ast::Expr *call = CallBuiltin(ast::Builtin::FilterManagerRunFilters, {filter_manager, vpi, exec_ctx});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::ExecCtxRegisterHook(ast::Expr *exec_ctx, uint32_t hook_idx, ast::Identifier hook) {
  ast::Expr *call =
      CallBuiltin(ast::Builtin::ExecutionContextRegisterHook, {exec_ctx, Const32(hook_idx), MakeExpr(hook)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::ExecCtxClearHooks(ast::Expr *exec_ctx) {
  ast::Expr *call = CallBuiltin(ast::Builtin::ExecutionContextClearHooks, {exec_ctx});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::ExecCtxInitHooks(ast::Expr *exec_ctx, uint32_t num_hooks) {
  ast::Expr *call = CallBuiltin(ast::Builtin::ExecutionContextInitHooks, {exec_ctx, Const32(num_hooks)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::ExecCtxAddRowsAffected(ast::Expr *exec_ctx, int64_t num_rows_affected) {
  ast::Expr *call = CallBuiltin(ast::Builtin::ExecutionContextAddRowsAffected, {exec_ctx, Const64(num_rows_affected)});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::ExecOUFeatureVectorRecordFeature(
    ast::Expr *ouvec, pipeline_id_t pipeline_id, feature_id_t feature_id,
    selfdriving::ExecutionOperatingUnitFeatureAttribute feature_attribute,
    selfdriving::ExecutionOperatingUnitFeatureUpdateMode mode, ast::Expr *value) {
  ast::Expr *call =
      CallBuiltin(ast::Builtin::ExecOUFeatureVectorRecordFeature,
                  {ouvec, Const32(pipeline_id.UnderlyingValue()), Const32(feature_id.UnderlyingValue()),
                   Const32(static_cast<int32_t>(feature_attribute)), Const32(static_cast<int32_t>(mode)), value});
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

ast::Expr *CodeGen::JoinHashTableInit(ast::Expr *join_hash_table, ast::Expr *exec_ctx,
                                      ast::Identifier build_row_type_name) {
  ast::Expr *call =
      CallBuiltin(ast::Builtin::JoinHashTableInit, {join_hash_table, exec_ctx, SizeOf(build_row_type_name)});
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

ast::Expr *CodeGen::JoinHTIteratorInit(ast::Expr *iter, ast::Expr *ht) {
  ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableIterInit, {iter, ht});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::JoinHTIteratorHasNext(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableIterHasNext, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Bool));
  return call;
}

ast::Expr *CodeGen::JoinHTIteratorNext(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableIterNext, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

ast::Expr *CodeGen::JoinHTIteratorGetRow(ast::Expr *iter, ast::Identifier payload_type) {
  ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableIterGetRow, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Uint8)->PointerTo());
  return PtrCast(payload_type, call);
}

ast::Expr *CodeGen::JoinHTIteratorFree(ast::Expr *iter) {
  ast::Expr *call = CallBuiltin(ast::Builtin::JoinHashTableIterFree, {iter});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Hash aggregations
// ---------------------------------------------------------

ast::Expr *CodeGen::AggHashTableInit(ast::Expr *agg_ht, ast::Expr *exec_ctx, ast::Identifier agg_payload_type) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggHashTableInit, {agg_ht, exec_ctx, SizeOf(agg_payload_type)});
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

ast::Expr *CodeGen::AggregatorFree(ast::Expr *agg) {
  ast::Expr *call = CallBuiltin(ast::Builtin::AggFree, {agg});
  call->SetType(ast::BuiltinType::Get(context_, ast::BuiltinType::Nil));
  return call;
}

// ---------------------------------------------------------
// Sorters
// ---------------------------------------------------------

ast::Expr *CodeGen::SorterInit(ast::Expr *sorter, ast::Expr *exec_ctx, ast::Identifier cmp_func_name,
                               ast::Identifier sort_row_type_name) {
  ast::Expr *call =
      CallBuiltin(ast::Builtin::SorterInit, {sorter, exec_ctx, MakeExpr(cmp_func_name), SizeOf(sort_row_type_name)});
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

ast::Expr *CodeGen::StorageInterfaceInit(ast::Identifier si, ast::Expr *exec_ctx, uint32_t table_oid,
                                         ast::Identifier col_oids, bool need_indexes) {
  ast::Expr *si_ptr = AddressOf(si);
  ast::Expr *table_oid_expr = Const64(static_cast<int64_t>(table_oid));
  ast::Expr *col_oids_expr = MakeExpr(col_oids);
  ast::Expr *need_indexes_expr = ConstBool(need_indexes);

  std::vector<ast::Expr *> args{si_ptr, exec_ctx, table_oid_expr, col_oids_expr, need_indexes_expr};
  return CallBuiltin(ast::Builtin::StorageInterfaceInit, args);
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

  if (num_cached_scopes_ < DEFAULT_SCOPE_CACHE_SIZE) {
    scope_cache_[num_cached_scopes_++].reset(scope);
  } else {
    delete scope;
  }
}

}  // namespace noisepage::execution::compiler
