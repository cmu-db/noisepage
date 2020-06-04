#include "execution/compiler/codegen.h"

#include <string>
#include <utility>
#include <vector>

#include "brain/operating_unit.h"
#include "date/date.h"
#include "execution/sql/value.h"
#include "parser/expression/constant_value_expression.h"
#include "util/time_util.h"

namespace terrier::execution::compiler {

CodeGen::CodeGen(exec::ExecutionContext *exec_ctx)
    : region_(std::make_unique<util::Region>("QueryRegion")),
      error_reporter_(region_.get()),
      ast_ctx_(std::make_unique<ast::Context>(region_.get(), &error_reporter_)),
      factory_(region_.get()),
      exec_ctx_(exec_ctx),
      pipeline_operating_units_(std::make_unique<brain::PipelineOperatingUnits>()),
      state_struct_{Context()->GetIdentifier("State")},
      state_var_{Context()->GetIdentifier("state")},
      exec_ctx_var_(Context()->GetIdentifier("execCtx")),
      main_fn_(Context()->GetIdentifier("main")),
      setup_fn_(Context()->GetIdentifier("setupFn")),
      teardown_fn_(Context()->GetIdentifier("teardownFn")) {}

ast::BlockStmt *CodeGen::EmptyBlock() {
  util::RegionVector<ast::Stmt *> stmts(Region());
  return Factory()->NewBlockStmt(DUMMY_POS, DUMMY_POS, std::move(stmts));
}

ast::Decl *CodeGen::MakeStruct(ast::Identifier struct_name, util::RegionVector<ast::FieldDecl *> &&fields) {
  ast::StructTypeRepr *struct_type = Factory()->NewStructType(DUMMY_POS, std::move(fields));
  return Factory()->NewStructDecl(DUMMY_POS, struct_name, struct_type);
}

util::RegionVector<ast::FieldDecl *> CodeGen::MainParams() {
  // Exec Context Parameter
  ast::Expr *exec_ctx_type = PointerType(BuiltinType(ast::BuiltinType::Kind::ExecutionContext));
  ast::FieldDecl *param = MakeField(exec_ctx_var_, exec_ctx_type);
  return {{param}, Region()};
}

util::RegionVector<ast::FieldDecl *> CodeGen::ExecParams() {
  // State parameter
  ast::Expr *state_type = PointerType(GetStateType());
  ast::FieldDecl *state_param = MakeField(state_var_, state_type);

  // Exec Context Parameter
  ast::Expr *exec_ctx_type = PointerType(BuiltinType(ast::BuiltinType::Kind::ExecutionContext));
  ast::FieldDecl *exec_ctx_param = MakeField(exec_ctx_var_, exec_ctx_type);

  // Function parameter
  return {{state_param, exec_ctx_param}, Region()};
}

ast::Stmt *CodeGen::ExecCall(ast::Identifier fn_name) {
  ast::Expr *func = MakeExpr(fn_name);
  ast::Expr *state_arg = PointerTo(state_var_);
  ast::Expr *exec_ctx_arg = MakeExpr(exec_ctx_var_);
  util::RegionVector<ast::Expr *> params{{state_arg, exec_ctx_arg}, Region()};
  return MakeStmt(Factory()->NewCallExpr(func, std::move(params)));
}

ast::Expr *CodeGen::GetStateMemberPtr(ast::Identifier ident) { return PointerTo(MemberExpr(state_var_, ident)); }

ast::Identifier CodeGen::NewIdentifier(const std::string &prefix) {
  // TODO(Amadou/Wan): John notes that there could be an extra string allocation and deallocation for the id count.
  //  An explicit string formatting call could avoid this.
  // Use the custom allocator because the id will outlive the std::string.
  std::string id = prefix + std::to_string(id_count_++);
  auto *id_str = Region()->AllocateArray<char>(id.size() + 1);
  std::memcpy(id_str, id.c_str(), id.size() + 1);
  return Context()->GetIdentifier(id_str);
}

ast::Expr *CodeGen::ZeroArgCall(ast::Builtin builtin) {
  ast::Expr *fun = BuiltinFunction(builtin);
  util::RegionVector<ast::Expr *> args{{}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr *CodeGen::OneArgCall(ast::Builtin builtin, ast::Expr *arg) {
  ast::Expr *fun = BuiltinFunction(builtin);
  util::RegionVector<ast::Expr *> args{{arg}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr *CodeGen::OneArgCall(ast::Builtin builtin, ast::Identifier ident, bool take_ptr) {
  ast::Expr *arg;
  if (take_ptr) {
    arg = PointerTo(ident);
  } else {
    arg = MakeExpr(ident);
  }
  return OneArgCall(builtin, arg);
}

ast::Expr *CodeGen::OneArgStateCall(ast::Builtin builtin, ast::Identifier ident) {
  ast::Expr *arg = GetStateMemberPtr(ident);
  return OneArgCall(builtin, arg);
}

ast::Expr *CodeGen::PtrCast(ast::Identifier base, ast::Expr *arg) { return PtrCast(MakeExpr(base), arg); }

ast::Expr *CodeGen::PtrCast(ast::Expr *base, ast::Expr *arg) {
  ast::Expr *fun = BuiltinFunction(ast::Builtin::PtrCast);
  ast::Expr *ptr = Factory()->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::STAR, base);
  util::RegionVector<ast::Expr *> cast_args{{ptr, arg}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(cast_args));
}

ast::Expr *CodeGen::TableIterInit(ast::Identifier tvi, uint32_t table_oid, ast::Identifier col_oids) {
  ast::Expr *fun = BuiltinFunction(ast::Builtin::TableIterInit);
  ast::Expr *tvi_ptr = PointerTo(tvi);
  ast::Expr *exec_ctx_expr = MakeExpr(exec_ctx_var_);
  ast::Expr *table_oid_expr = IntLiteral(static_cast<int64_t>(table_oid));
  ast::Expr *col_oids_expr = MakeExpr(col_oids);

  util::RegionVector<ast::Expr *> args{{tvi_ptr, exec_ctx_expr, table_oid_expr, col_oids_expr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr *CodeGen::PCIGet(ast::Identifier pci, type::TypeId type, bool nullable, uint32_t idx) {
  ast::Builtin builtin;
  ast::Type *ast_type;
  switch (type) {
    case type::TypeId::BOOLEAN:
      builtin = nullable ? ast::Builtin::PCIGetBoolNull : ast::Builtin::PCIGetBool;
      // TODO(WAN): pmenon points out that this should be Boolean, throughout codegen.cpp,
      // but that we will hold off on the change until codegen v2
      ast_type = ast::BuiltinType::Get(Context(), ast::BuiltinType::Bool);
      break;
    case type::TypeId::TINYINT:
      builtin = nullable ? ast::Builtin::PCIGetTinyIntNull : ast::Builtin::PCIGetTinyInt;
      ast_type = ast::BuiltinType::Get(Context(), ast::BuiltinType::Integer);
      break;
    case type::TypeId::SMALLINT:
      builtin = nullable ? ast::Builtin::PCIGetSmallIntNull : ast::Builtin::PCIGetSmallInt;
      ast_type = ast::BuiltinType::Get(Context(), ast::BuiltinType::Integer);
      break;
    case type::TypeId::INTEGER:
      builtin = nullable ? ast::Builtin::PCIGetIntNull : ast::Builtin::PCIGetInt;
      ast_type = ast::BuiltinType::Get(Context(), ast::BuiltinType::Integer);
      break;
    case type::TypeId::BIGINT:
      builtin = nullable ? ast::Builtin::PCIGetBigIntNull : ast::Builtin::PCIGetBigInt;
      ast_type = ast::BuiltinType::Get(Context(), ast::BuiltinType::Integer);
      break;
    case type::TypeId::DECIMAL:
      builtin = nullable ? ast::Builtin ::PCIGetDoubleNull : ast::Builtin::PCIGetDouble;
      ast_type = ast::BuiltinType::Get(Context(), ast::BuiltinType::Real);
      break;
    case type::TypeId::DATE:
      builtin = nullable ? ast::Builtin ::PCIGetDateNull : ast::Builtin::PCIGetDate;
      ast_type = ast::BuiltinType::Get(Context(), ast::BuiltinType::Date);
      break;
    case type::TypeId::TIMESTAMP:
      builtin = nullable ? ast::Builtin ::PCIGetTimestampNull : ast::Builtin::PCIGetTimestamp;
      ast_type = ast::BuiltinType::Get(Context(), ast::BuiltinType::Timestamp);
      break;
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY:
      builtin = nullable ? ast::Builtin ::PCIGetVarlenNull : ast::Builtin::PCIGetVarlen;
      ast_type = ast::BuiltinType::Get(Context(), ast::BuiltinType::StringVal);
      break;
    default:
      UNREACHABLE("Cannot @pciGetType unsupported type");
  }
  ast::Expr *fun = BuiltinFunction(builtin);
  ast::Expr *pci_expr = MakeExpr(pci);
  ast::Expr *idx_expr = Factory()->NewIntLiteral(DUMMY_POS, idx);
  util::RegionVector<ast::Expr *> args{{pci_expr, idx_expr}, Region()};
  ast::Expr *ret = Factory()->NewBuiltinCallExpr(fun, std::move(args));
  ret->SetType(ast_type);
  return ret;
}

ast::Expr *CodeGen::PCIFilter(ast::Identifier pci, parser::ExpressionType comp_type, uint32_t col_idx,
                              type::TypeId col_type, ast::Expr *filter_val) {
  // Call @FilterComp(pci, col_idx, col_type, filter_val)
  ast::Builtin builtin;
  switch (comp_type) {
    case parser::ExpressionType::COMPARE_EQUAL:
      builtin = ast::Builtin::FilterEq;
      break;
    case parser::ExpressionType::COMPARE_NOT_EQUAL:
      builtin = ast::Builtin::FilterNe;
      break;
    case parser::ExpressionType::COMPARE_LESS_THAN:
      builtin = ast::Builtin::FilterLt;
      break;
    case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::FilterLe;
      break;
    case parser::ExpressionType::COMPARE_GREATER_THAN:
      builtin = ast::Builtin::FilterGt;
      break;
    case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      builtin = ast::Builtin::FilterGe;
      break;
    default:
      UNREACHABLE("Impossible filter comparison!");
  }
  ast::Expr *fun = BuiltinFunction(builtin);
  ast::Expr *pci_expr = MakeExpr(pci);
  ast::Expr *idx_expr = IntLiteral(col_idx);
  ast::Expr *type_expr = IntLiteral(static_cast<int8_t>(col_type));
  util::RegionVector<ast::Expr *> args{{pci_expr, idx_expr, type_expr, filter_val}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr *CodeGen::ExecCtxGetMem() {
  return OneArgCall(ast::Builtin::ExecutionContextGetMemoryPool, exec_ctx_var_, false);
}

ast::Expr *CodeGen::SizeOf(ast::Identifier type_name) { return OneArgCall(ast::Builtin::SizeOf, type_name, false); }

ast::Expr *CodeGen::HTInitCall(ast::Builtin builtin, ast::Identifier object, ast::Identifier struct_type) {
  // Init Function
  ast::Expr *fun = BuiltinFunction(builtin);
  // The object to initialize
  ast::Expr *obj_ptr = GetStateMemberPtr(object);
  // Then get @execCtxGetMem(execCtx)
  ast::Expr *get_mem_call = ExecCtxGetMem();
  // Then get @sizeof(Struct)
  ast::Expr *sizeof_call = SizeOf(struct_type);
  // Finally make the init call
  util::RegionVector<ast::Expr *> args{{obj_ptr, get_mem_call, sizeof_call}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr *CodeGen::IndexIteratorInit(ast::Identifier iter, uint32_t num_attrs, uint32_t table_oid, uint32_t index_oid,
                                      ast::Identifier col_oids) {
  // @indexIteratorInit(&iter, table_oid, index_oid, execCtx)
  ast::Expr *fun = BuiltinFunction(ast::Builtin::IndexIteratorInit);
  ast::Expr *iter_ptr = PointerTo(iter);
  ast::Expr *exec_ctx_expr = MakeExpr(exec_ctx_var_);
  ast::Expr *num_attrs_expr = IntLiteral(static_cast<int32_t>(num_attrs));
  ast::Expr *table_oid_expr = IntLiteral(static_cast<int32_t>(table_oid));
  ast::Expr *index_oid_expr = IntLiteral(static_cast<int32_t>(index_oid));
  ast::Expr *col_oids_expr = MakeExpr(col_oids);
  util::RegionVector<ast::Expr *> args{
      {iter_ptr, exec_ctx_expr, num_attrs_expr, table_oid_expr, index_oid_expr, col_oids_expr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
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

  if (!use_limit && !asc_scan) return OneArgCall(builtin, iter, true);

  ast::Expr *fun = BuiltinFunction(builtin);
  ast::Expr *iter_ptr = PointerTo(iter);
  util::RegionVector<ast::Expr *> args({iter_ptr}, Region());

  if (asc_scan) args.push_back(IntLiteral(static_cast<int64_t>(asc_type)));
  if (use_limit) args.push_back(IntLiteral(limit));

  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
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
    case type::TypeId::DECIMAL:
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
  ast::Expr *fun = BuiltinFunction(builtin);
  ast::Expr *idx_expr = Factory()->NewIntLiteral(DUMMY_POS, attr_idx);
  util::RegionVector<ast::Expr *> args{{pr, idx_expr}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
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
    case type::TypeId::DECIMAL:
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
  ast::Expr *fun = BuiltinFunction(builtin);
  ast::Expr *idx_expr = Factory()->NewIntLiteral(DUMMY_POS, attr_idx);
  util::RegionVector<ast::Expr *> args{{pr, idx_expr, val}, Region()};
  if ((type == type::TypeId::VARCHAR) || (type == type::TypeId::VARBINARY)) {
    args.emplace_back(BoolLiteral(own));
  }
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr *CodeGen::PeekValue(const parser::ConstantValueExpression &const_val) {
  if (const_val.IsNull()) {
    // NullToSql(&expr) produces a NULL of expr's type.
    ast::Expr *dummy_expr;
    switch (const_val.GetReturnValueType()) {
      case type::TypeId::BOOLEAN:
        dummy_expr = BoolLiteral(false);
        break;
      case type::TypeId::TINYINT:  /* fall-through */
      case type::TypeId::SMALLINT: /* fall-through */
      case type::TypeId::INTEGER:  /* fall-through */
      case type::TypeId::BIGINT:   /* fall-through */
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
      case type::TypeId::DECIMAL:
        dummy_expr = FloatToSql(0.0);
        break;
      case type::TypeId::VARBINARY:
      default:
        UNREACHABLE("Unsupported NULL type!");
    }
    return OneArgCall(ast::Builtin::NullToSql, PointerTo(dummy_expr));
  }

  switch (const_val.GetReturnValueType()) {
    case type::TypeId::BOOLEAN: {
      return BoolLiteral(const_val.Peek<bool>());
    }
    case type::TypeId::TINYINT:
    case type::TypeId::SMALLINT:
    case type::TypeId::INTEGER:
    case type::TypeId::BIGINT: {
      return IntToSql(const_val.Peek<int64_t>());
    }
    case type::TypeId::DATE: {
      const auto val = const_val.Peek<execution::sql::Date>().ToNative();
      auto ymd = terrier::util::TimeConvertor::YMDFromDate(static_cast<type::date_t>(val));
      auto year = static_cast<int32_t>(ymd.year());
      auto month = static_cast<uint32_t>(ymd.month());
      auto day = static_cast<uint32_t>(ymd.day());
      return DateToSql(year, month, day);
    }
    case type::TypeId::TIMESTAMP: {
      const auto val = const_val.Peek<execution::sql::Timestamp>().ToNative();
      auto julian_usec = terrier::util::TimeConvertor::ExtractJulianMicroseconds(static_cast<type::timestamp_t>(val));
      return TimestampToSql(julian_usec);
    }
    case type::TypeId::DECIMAL: {
      return FloatToSql(const_val.Peek<double>());
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY: {
      return StringToSql(const_val.Peek<std::string_view>());
    }
    default:
      // TODO(Amadou): Add support for these types.
      UNREACHABLE("Should not peek on given type!");
  }
}

ast::Expr *CodeGen::TplType(type::TypeId type) {
  switch (type) {
    case type::TypeId::BOOLEAN:
      return BuiltinType(ast::BuiltinType::Kind::Boolean);
    case type::TypeId::TINYINT:
    case type::TypeId::SMALLINT:
    case type::TypeId::INTEGER:
    case type::TypeId::BIGINT:
      return BuiltinType(ast::BuiltinType::Kind::Integer);
    case type::TypeId::DATE:
      return BuiltinType(ast::BuiltinType::Kind::Date);
    case type::TypeId::TIMESTAMP:
      return BuiltinType(ast::BuiltinType::Kind::Timestamp);
    case type::TypeId::DECIMAL:
      return BuiltinType(ast::BuiltinType::Kind::Real);
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY:
      return BuiltinType(ast::BuiltinType::Kind::StringVal);
    default:
      UNREACHABLE("Cannot codegen unsupported type.");
  }
}

ast::Expr *CodeGen::BuiltinFunction(ast::Builtin builtin) {
  ast::Identifier fun = Context()->GetBuiltinFunction(builtin);
  return MakeExpr(fun);
}

ast::Expr *CodeGen::BuiltinType(ast::BuiltinType::Kind kind) {
  ast::Identifier typ = Context()->GetBuiltinType(kind);
  return MakeExpr(typ);
}

ast::Expr *CodeGen::PointerType(ast::Expr *base_expr) { return Factory()->NewPointerType(DUMMY_POS, base_expr); }

ast::Expr *CodeGen::PointerType(ast::Identifier base_type) {
  ast::Expr *base_expr = MakeExpr(base_type);
  return PointerTo(base_expr);
}

ast::Expr *CodeGen::ArrayType(uint64_t num_elems, ast::BuiltinType::Kind kind) {
  return Factory()->NewArrayType(DUMMY_POS, IntLiteral(num_elems), BuiltinType(kind));
}

ast::Expr *CodeGen::ArrayAccess(ast::Identifier arr, uint64_t idx) {
  return Factory()->NewIndexExpr(DUMMY_POS, MakeExpr(arr), IntLiteral(idx));
}

#define AGGTYPE(AggName, terrier_type)                        \
  switch (terrier_type) {                                     \
    case type::TypeId::TINYINT:                               \
    case type::TypeId::SMALLINT:                              \
    case type::TypeId::INTEGER:                               \
    case type::TypeId::BIGINT:                                \
      return BuiltinType(ast::BuiltinType::Integer##AggName); \
    case type::TypeId::DECIMAL:                               \
      return BuiltinType(ast::BuiltinType::Real##AggName);    \
    default:                                                  \
      UNREACHABLE("Unsupported aggregate type");              \
  }

ast::Expr *CodeGen::AggregateType(parser::ExpressionType agg_type, type::TypeId ret_type) {
  switch (agg_type) {
    case parser::ExpressionType::AGGREGATE_COUNT:
      return BuiltinType(ast::BuiltinType::Kind::CountAggregate);
    case parser::ExpressionType::AGGREGATE_AVG:
      AGGTYPE(AvgAggregate, ret_type);
    case parser::ExpressionType::AGGREGATE_MIN:
      AGGTYPE(MinAggregate, ret_type);
    case parser::ExpressionType::AGGREGATE_MAX:
      AGGTYPE(MaxAggregate, ret_type);
    case parser::ExpressionType::AGGREGATE_SUM:
      AGGTYPE(SumAggregate, ret_type);
    default:
      UNREACHABLE("AggregateType() should only be called with aggregates");
  }
}

ast::Stmt *CodeGen::DeclareVariable(ast::Identifier name, ast::Expr *type, ast::Expr *init) {
  ast::Decl *decl = Factory()->NewVariableDecl(DUMMY_POS, name, type, init);
  return Factory()->NewDeclStmt(decl);
}

ast::Stmt *CodeGen::MakeStmt(ast::Expr *expr) { return Factory()->NewExpressionStmt(expr); }

ast::Stmt *CodeGen::Assign(ast::Expr *lhs, ast::Expr *rhs) { return Factory()->NewAssignmentStmt(DUMMY_POS, lhs, rhs); }

ast::Expr *CodeGen::MakeExpr(ast::Identifier ident) { return Factory()->NewIdentifierExpr(DUMMY_POS, ident); }

ast::Expr *CodeGen::PointerTo(ast::Identifier ident) { return PointerTo(MakeExpr(ident)); }

ast::Expr *CodeGen::PointerTo(ast::Expr *base) {
  return Factory()->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::AMPERSAND, base);
}

ast::Expr *CodeGen::MemberExpr(ast::Identifier lhs, ast::Identifier rhs) {
  ast::Expr *object = MakeExpr(lhs);
  ast::Expr *member = MakeExpr(rhs);
  return Factory()->NewMemberExpr(DUMMY_POS, object, member);
}

ast::Expr *CodeGen::IsSqlNull(ast::Expr *expr) { return OneArgCall(ast::Builtin::IsSqlNull, expr); }

ast::Expr *CodeGen::IsSqlNotNull(ast::Expr *expr) { return OneArgCall(ast::Builtin::IsSqlNotNull, expr); }

ast::Expr *CodeGen::NullToSql(ast::Expr *expr) { return OneArgCall(ast::Builtin::NullToSql, expr); }

ast::Expr *CodeGen::IntToSql(int64_t num) {
  ast::Expr *int_lit = IntLiteral(num);
  return OneArgCall(ast::Builtin::IntToSql, int_lit);
}

ast::Expr *CodeGen::FloatToSql(double num) {
  ast::Expr *float_lit = FloatLiteral(num);
  return OneArgCall(ast::Builtin::FloatToSql, float_lit);
}

ast::Expr *CodeGen::DateToSql(int32_t year, uint32_t month, uint32_t day) {
  ast::Expr *fun = BuiltinFunction(ast::Builtin::DateToSql);
  ast::Expr *year_lit = IntLiteral(year);
  ast::Expr *month_lit = IntLiteral(month);
  ast::Expr *day_lit = IntLiteral(day);
  util::RegionVector<ast::Expr *> args{{year_lit, month_lit, day_lit}, Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr *CodeGen::TimestampToSql(uint64_t julian_usec) {
  ast::Expr *usec_expr = IntLiteral(julian_usec);
  return OneArgCall(ast::Builtin::TimestampToSql, usec_expr);
}

ast::Expr *CodeGen::StringToSql(std::string_view str) {
  ast::Identifier str_ident = Context()->GetIdentifier({str.data(), str.length()});
  ast::Expr *str_lit = Factory()->NewStringLiteral(DUMMY_POS, str_ident);
  return OneArgCall(ast::Builtin::StringToSql, str_lit);
}

ast::Expr *CodeGen::StorageInterfaceInit(ast::Identifier si, uint32_t table_oid, ast::Identifier col_oids,
                                         bool need_indexes) {
  ast::Expr *fun = BuiltinFunction(ast::Builtin::StorageInterfaceInit);
  ast::Expr *si_ptr = PointerTo(si);
  ast::Expr *table_oid_expr = IntLiteral(static_cast<int64_t>(table_oid));
  ast::Expr *exec_ctx_expr = MakeExpr(exec_ctx_var_);
  ast::Expr *col_oids_expr = MakeExpr(col_oids);
  ast::Expr *need_indexes_expr = BoolLiteral(need_indexes);

  util::RegionVector<ast::Expr *> args{{si_ptr, exec_ctx_expr, table_oid_expr, col_oids_expr, need_indexes_expr},
                                       Region()};
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}

ast::Expr *CodeGen::BuiltinCall(ast::Builtin builtin, std::vector<ast::Expr *> &&params) {
  ast::Expr *fun = BuiltinFunction(builtin);
  util::RegionVector<ast::Expr *> args{{}, Region()};
  for (auto &expr : params) {
    args.emplace_back(expr);
  }
  return Factory()->NewBuiltinCallExpr(fun, std::move(args));
}
}  // namespace terrier::execution::compiler
