#include "execution/compiler/codegen.h"

#include <string>
#include <utility>
#include "execution/compiler/code_context.h"
#include "execution/compiler/function_builder.h"
#include "type/transient_value_peeker.h"

namespace tpl::compiler {

CodeGen::CodeGen(Query* query)
: query_(query)
, state_struct_{Context()->GetIdentifier("State")}
, state_var_{Context()->GetIdentifier("state")}
, exec_ctx_var_(Context()->GetIdentifier("execCtx"))
, main_fn_(Context()->GetIdentifier("main"))
, setup_fn_(Context()->GetIdentifier("setupFn"))
, teardown_fn_(Context()->GetIdentifier("teardownFn"))
{}

ast::BlockStmt *CodeGen::EmptyBlock() {
  util::RegionVector<ast::Stmt *> stmts(Region());
  return Factory()->NewBlockStmt(DUMMY_POS, DUMMY_POS, std::move(stmts));
}

ast::Decl* CodeGen::MakeStruct(ast::Identifier struct_name, util::RegionVector<ast::FieldDecl*> && fields) {
  ast::StructTypeRepr * struct_type = Factory()->NewStructType(DUMMY_POS, std::move(fields));
  return Factory()->NewStructDecl(DUMMY_POS, struct_name, struct_type);
}

util::RegionVector<ast::FieldDecl *> CodeGen::MainParams() {
  // Exec Context Parameter
  ast::Expr *exec_ctx_type = PointerType(BuiltinType(ast::BuiltinType::Kind::ExecutionContext));
  ast::FieldDecl * param = MakeField(exec_ctx_var_, exec_ctx_type);
  return {{param}, Region()};
}

util::RegionVector<ast::FieldDecl *> CodeGen::ExecParams() {
  // State parameter
  ast::Expr * state_type = PointerType(GetStateType());
  ast::FieldDecl * state_param = MakeField(state_var_, state_type);

  // Exec Context Parameter
  ast::Expr *exec_ctx_type = PointerType(BuiltinType(ast::BuiltinType::Kind::ExecutionContext));
  ast::FieldDecl * exec_ctx_param = MakeField(exec_ctx_var_, exec_ctx_type);

  // Function parameter
  return {{state_param, exec_ctx_param}, Region()};
}

ast::Expr* CodeGen::GetStateMemberPtr(ast::Identifier ident) {
  return PointerTo(MemberExpr(state_var_, ident));
}

ast::Identifier CodeGen::NewIdentifier() {
  return NewIdentifier("id");
}

ast::Identifier CodeGen::NewIdentifier(const std::string &prefix) {
  // Use the custom allocator because the id will outlive the std::string.
  std::string id = prefix + std::to_string(id_count_++);
  auto *id_str = Region()->AllocateArray<char>(id.size() + 1);
  std::memcpy(id_str, id.c_str(), id.size() + 1);
  return Context()->GetIdentifier(id_str);

}

ast::Expr* CodeGen::OneArgCall(ast::Builtin builtin, ast::Expr* arg) {
  ast::Expr * fun = BuiltinFunction(builtin);
  util::RegionVector<ast::Expr*> args{{arg}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::OneArgCall(ast::Builtin builtin, ast::Identifier ident, bool take_ptr) {
  ast::Expr * arg;
  if (take_ptr) {
    arg = PointerTo(ident);
  } else {
    arg = MakeExpr(ident);
  }
  return OneArgCall(builtin, arg);
}



ast::Expr* CodeGen::OneArgStateCall(tpl::ast::Builtin builtin, tpl::ast::Identifier ident) {
  ast::Expr * arg = GetStateMemberPtr(ident);
  OneArgCall(builtin, arg);
}


ast::Expr* CodeGen::PtrCast(ast::Identifier base, ast::Expr* arg) {
  ast::Expr* fun = BuiltinFunction(ast::Builtin::PtrCast);
  ast::Expr* ptr = Factory()->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::STAR, MakeExpr(base));
  util::RegionVector<ast::Expr*> cast_args{{ptr, arg}, Region()};
  Factory()->NewCallExpr(fun, std::move(cast_args));
}

ast::Expr* CodeGen::OutputAlloc() {
  return OneArgCall(ast::Builtin::OutputAlloc, exec_ctx_var_, false);
}

ast::Expr* CodeGen::OutputAdvance() {
  return OneArgCall(ast::Builtin::OutputAdvance, exec_ctx_var_, false);
}

ast::Expr* CodeGen::OutputFinalize() {
  return OneArgCall(ast::Builtin::OutputFinalize, exec_ctx_var_, false);
}

ast::Expr* CodeGen::TableIterInit(ast::Identifier tvi, const std::string & table_name) {
  ast::Expr * fun = BuiltinFunction(ast::Builtin::TableIterInit);
  ast::Expr * tvi_ptr = PointerTo(tvi);
  ast::Expr * table_name_expr = Factory()->NewStringLiteral(DUMMY_POS, Context()->GetIdentifier(table_name));
  ast::Expr * exec_ctx_expr = MakeExpr(exec_ctx_var_);

  util::RegionVector<ast::Expr*> args{{tvi_ptr, table_name_expr, exec_ctx_expr}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::TableIterAdvance(ast::Identifier tvi) {
  return OneArgCall(ast::Builtin::TableIterAdvance, tvi, true);
}

ast::Expr* CodeGen::TableIterGetPCI(tpl::ast::Identifier tvi) {
  return OneArgCall(ast::Builtin::TableIterGetPCI, tvi, true);
}


ast::Expr* CodeGen::TableIterClose(tpl::ast::Identifier tvi) {
  return OneArgCall(ast::Builtin::TableIterClose, tvi, true);
}

ast::Expr* CodeGen::PCIHasNext(tpl::ast::Identifier pci) {
  return OneArgCall(ast::Builtin::PCIHasNext, pci, false);
}


ast::Expr* CodeGen::PCIAdvance(tpl::ast::Identifier pci) {
  return OneArgCall(ast::Builtin::PCIAdvance, pci, false);
}


ast::Expr* CodeGen::PCIGet(tpl::ast::Identifier pci, terrier::type::TypeId type, uint32_t idx) {
  ast::Builtin builtin;
  switch (type) {
    case terrier::type::TypeId::INTEGER:
      builtin = ast::Builtin::PCIGetInt;
      break;
    case terrier::type::TypeId::SMALLINT:
      builtin = ast::Builtin::PCIGetSmallInt;
      break;
    case terrier::type::TypeId::BIGINT:
      builtin = ast::Builtin::PCIGetBigInt;
      break;
    default:
      // TODO: Support other types.
      builtin = ast::Builtin::PCIGetInt;
  }
  ast::Expr * fun = BuiltinFunction(builtin);
  ast::Expr * pci_expr = MakeExpr(pci);
  ast::Expr * idx_expr = Factory()->NewIntLiteral(DUMMY_POS, idx);
  util::RegionVector<ast::Expr*> args{{pci_expr, idx_expr}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::Hash(tpl::util::RegionVector<tpl::ast::Expr *> &&args) {
  ast::Expr * fun = BuiltinFunction(ast::Builtin::Hash);
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::ExecCtxGetMem() {
  return OneArgCall(ast::Builtin::ExecutionContextGetMemoryPool, exec_ctx_var_, false);
}

ast::Expr* CodeGen::SizeOf(ast::Identifier type_name) {
  return OneArgCall(ast::Builtin::SizeOf, type_name, false);
}

ast::Expr* CodeGen::InitCall(ast::Builtin builtin, tpl::ast::Identifier object, tpl::ast::Identifier struct_type) {
  // Init Function
  ast::Expr * fun = BuiltinFunction(builtin);
  ast::Expr* obj_ptr = GetStateMemberPtr(object);
  // Then get @execCtxGetMem(execCtx)
  ast::Expr* get_mem_call = ExecCtxGetMem();
  // Then get @sizeof(Struct)
  ast::Expr* sizeof_call = SizeOf(struct_type);
  // Finally make the init call
  util::RegionVector<ast::Expr*> args{{obj_ptr, get_mem_call, sizeof_call}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::AggHashTableInit(ast::Identifier ht, ast::Identifier payload_struct) {
  // @aggHTInit(&state.agg_hash_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  return InitCall(ast::Builtin::AggHashTableInit, ht, payload_struct);
}

ast::Expr* CodeGen::AggHashTableLookup(ast::Identifier ht, ast::Identifier hash_val, ast::Identifier key_check, ast::Identifier values) {
  // @aggHTLookup((&state.agg_ht, agg_hash_val, keyCheck, &agg_values)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::AggHashTableLookup);
  ast::Expr* agg_ht = GetStateMemberPtr(ht);
  ast::Expr* hash_val_expr = MakeExpr(hash_val);
  ast::Expr* key_check_expr = MakeExpr(key_check);
  ast::Expr* agg_values_ptr = PointerTo(values);
  util::RegionVector<ast::Expr*> args{{agg_ht, hash_val_expr, key_check_expr, agg_values_ptr}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::AggHashTableFree(ast::Identifier ht) {
  // @aggHTFree(&state.agg_ht)
  return OneArgStateCall(ast::Builtin::AggHashTableFree, ht);
}

ast::Expr* CodeGen::AggHashTableIterInit(ast::Identifier iter, ast::Identifier ht) {
  // @aggHTIterInit(&agg_iter, &state.agg_table)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::AggHashTableIterInit);
  ast::Expr* iter_ptr = PointerTo(iter);
  ast::Expr* agg_ht = GetStateMemberPtr(ht);
  util::RegionVector<ast::Expr*> args{{iter_ptr, agg_ht}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::AggHashTableIterHasNext(ast::Identifier iter) {
  // @aggHTIterHasNext(&agg_iter)
  return OneArgCall(ast::Builtin::AggHashTableIterHasNext, iter, true);
}

ast::Expr* CodeGen::AggHashTableIterNext(ast::Identifier iter) {
  // @aggHTIterNext(&agg_iter)
  return OneArgCall(ast::Builtin::AggHashTableIterNext, iter, true);
}

ast::Expr* CodeGen::AggHashTableIterGetRow(ast::Identifier iter) {
  // @aggHTIterGetRow(&agg_iter)
  return OneArgCall(ast::Builtin::AggHashTableIterGetRow, iter, true);
}

ast::Expr* CodeGen::AggHashTableIterClose(ast::Identifier iter) {
  // @aggHTIterClose(&agg_iter)
  return OneArgCall(ast::Builtin::AggHashTableIterClose, iter, true);
}

ast::Expr* CodeGen::AggInit(ast::Expr* agg) {
  // @aggInit(agg)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::AggInit);
  util::RegionVector<ast::Expr*> args{{agg}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::AggAdvance(ast::Expr* agg, ast::Expr* val) {
  // @aggAdvance(agg, val)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::AggAdvance);
  util::RegionVector<ast::Expr*> args{{agg, val}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::AggResult(ast::Expr* agg) {
  // @aggResult(agg)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::AggResult);
  util::RegionVector<ast::Expr*> args{{agg}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::JoinHashTableInit(ast::Identifier ht, ast::Identifier build_struct) {
  // @joinHTInit(&state.join_hash_table, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
  return InitCall(ast::Builtin::JoinHashTableInit, ht, build_struct);
}

ast::Expr* CodeGen::JoinHashTableInsert(tpl::ast::Identifier ht, tpl::ast::Identifier hash_val) {
  // @joinHTInsert(&state.join_table, hash_val)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::JoinHashTableInsert);
  ast::Expr* join_ht = GetStateMemberPtr(ht);
  ast::Expr* hash_val_expr = MakeExpr(hash_val);
  util::RegionVector<ast::Expr*> args{{join_ht, hash_val_expr}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::JoinHashTableIterInit(tpl::ast::Identifier iter,
                                          tpl::ast::Identifier ht,
                                          tpl::ast::Identifier hash_val) {
  // Call @joinHTIterInit(&iter, &state.ht, hash_val)
  ast::Expr * fun = BuiltinFunction(ast::Builtin::JoinHashTableIterInit);
  ast::Expr * iter_ptr = PointerTo(iter);
  ast::Expr* join_ht = GetStateMemberPtr(ht);
  ast::Expr* hash_val_expr = MakeExpr(hash_val);
  util::RegionVector<ast::Expr*> args{{iter_ptr, join_ht, hash_val_expr}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::JoinHashTableIterHasNext(tpl::ast::Identifier iter,
                                             tpl::ast::Identifier key_check,
                                             tpl::ast::Identifier probe_row,
                                             bool is_probe_ptr) {
  ast::Expr * fun = BuiltinFunction(ast::Builtin::JoinHashTableIterHasNext);
  ast::Expr * iter_ptr = PointerTo(iter);
  ast::Expr * key_check_fn = MakeExpr(key_check);
  ast::Expr* probe_row_ptr;
  if (is_probe_ptr) {
    probe_row_ptr = MakeExpr(probe_row);
  } else {
    probe_row_ptr = PointerTo(probe_row);
  }
  util::RegionVector<ast::Expr*> args{{iter_ptr, key_check_fn, probe_row_ptr}, Region()};
  return Factory()->NewCallExpr(fun, std::move(args));
}

ast::Expr* CodeGen::JoinHashTableIterGetRow(ast::Identifier iter) {
  // @joinHTIterHasNext(&iter)
  return OneArgCall(ast::Builtin::JoinHashTableIterGetRow, iter, true);
}

ast::Expr* CodeGen::JoinHashTableIterClose(tpl::ast::Identifier iter) {
  // @joinHTIterClose(&iter)
  return OneArgCall(ast::Builtin::JoinHashTableIterClose, iter, true);
}

ast::Expr* CodeGen::JoinHashTableBuild(tpl::ast::Identifier ht) {
  // @joinHTIterBuild&state.ht)
  return OneArgStateCall(ast::Builtin::JoinHashTableBuild, ht);
}

ast::Expr* CodeGen::JoinHashTableFree(tpl::ast::Identifier ht) {
  // @joinHTIterBuild&state.ht)
  return OneArgStateCall(ast::Builtin::JoinHashTableFree, ht);
}


ast::Expr *CodeGen::PeekValue(const terrier::type::TransientValue &transient_val) {
  switch (transient_val.Type()) {
    case terrier::type::TypeId::TINYINT: {
      auto val = terrier::type::TransientValuePeeker::PeekTinyInt(transient_val);
      return Factory()->NewIntLiteral(DUMMY_POS, val);
    }
    case terrier::type::TypeId::SMALLINT: {
      auto val = terrier::type::TransientValuePeeker::PeekSmallInt(transient_val);
      return Factory()->NewIntLiteral(DUMMY_POS, val);
    }
    case terrier::type::TypeId::INTEGER: {
      auto val = terrier::type::TransientValuePeeker::PeekInteger(transient_val);
      return Factory()->NewIntLiteral(DUMMY_POS, static_cast<i32>(val));
    }
    case terrier::type::TypeId::BIGINT: {
      // TODO(WAN): the factory's IntLiteral only goes to i32
      auto val = terrier::type::TransientValuePeeker::PeekBigInt(transient_val);
      return Factory()->NewIntLiteral(DUMMY_POS, static_cast<i32>(val));
    }
    case terrier::type::TypeId::BOOLEAN: {
      auto val = terrier::type::TransientValuePeeker::PeekBoolean(transient_val);
      return Factory()->NewBoolLiteral(DUMMY_POS, val);
    }
    case terrier::type::TypeId::DATE:
    case terrier::type::TypeId::TIMESTAMP:
    case terrier::type::TypeId::DECIMAL:
    case terrier::type::TypeId::VARCHAR:
    case terrier::type::TypeId::VARBINARY:
    default:
      // TODO(WAN): error out
      return nullptr;
  }
}

ast::Expr *CodeGen::TplType(terrier::type::TypeId type) {
  switch (type) {
    case terrier::type::TypeId::TINYINT:
    case terrier::type::TypeId::SMALLINT:
    case terrier::type::TypeId::INTEGER:
    case terrier::type::TypeId::BIGINT: {
      return BuiltinType(ast::BuiltinType::Kind::Integer);
    }
    case terrier::type::TypeId::BOOLEAN: {
      return BuiltinType(ast::BuiltinType::Kind::Boolean);
    }
    case terrier::type::TypeId::DATE:
    case terrier::type::TypeId::TIMESTAMP:
    case terrier::type::TypeId::DECIMAL:
    case terrier::type::TypeId::VARCHAR:
    case terrier::type::TypeId::VARBINARY:
    default:
      // TODO(WAN): error out
      return nullptr;
  }
}

ast::Expr* CodeGen::BuiltinFunction(ast::Builtin builtin) {
  ast::Identifier fun = Context()->GetBuiltinFunction(builtin);
  return MakeExpr(fun);
}

ast::Expr* CodeGen::BuiltinType(tpl::ast::BuiltinType::Kind kind) {
  ast::Identifier typ = Context()->GetBuiltinType(kind);
  return MakeExpr(typ);
}

ast::Expr* CodeGen::PointerType(ast::Expr* base_expr) {
  return Factory()->NewPointerType(DUMMY_POS, base_expr);
}

ast::Expr* CodeGen::PointerType(tpl::ast::Identifier base_type) {
  ast::Expr* base_expr = MakeExpr(base_type);
  return PointerTo(base_expr);
}

ast::Expr* CodeGen::AggregateType(terrier::parser::ExpressionType type) {
  switch (type) {
    case terrier::parser::ExpressionType::AGGREGATE_COUNT:
      return BuiltinType(ast::BuiltinType::Kind::CountAggregate);
    case terrier::parser::ExpressionType::AGGREGATE_COUNT_STAR:
      return BuiltinType(ast::BuiltinType::Kind::CountStarAggregate);
    case terrier::parser::ExpressionType::AGGREGATE_MIN:
      return BuiltinType(ast::BuiltinType::Kind::IntegerMinAggregate);
    case terrier::parser::ExpressionType::AGGREGATE_AVG:
      return BuiltinType(ast::BuiltinType::Kind::IntegerAvgAggregate);
    case terrier::parser::ExpressionType::AGGREGATE_MAX:
      return BuiltinType(ast::BuiltinType::Kind::IntegerMaxAggregate);
    case terrier::parser::ExpressionType::AGGREGATE_SUM:
      return BuiltinType(ast::BuiltinType::Kind::IntegerSumAggregate);
    default:
      UNREACHABLE("AggregateType() should only be called with aggregates");
  }
}

ast::Stmt* CodeGen::DeclareVariable(ast::Identifier name, ast::Expr *typ, ast::Expr *init) {
  ast::Decl *decl = Factory()->NewVariableDecl(DUMMY_POS, name, typ, init);
  return Factory()->NewDeclStmt(decl);
}

ast::Stmt* CodeGen::MakeStmt(ast::Expr* expr) {
  return Factory()->NewExpressionStmt(expr);
}

ast::Stmt* CodeGen::Assign(tpl::ast::Expr *lhs, tpl::ast::Expr *rhs) {
  return Factory()->NewAssignmentStmt(DUMMY_POS, lhs, rhs);
}

ast::Expr* CodeGen::MakeExpr(ast::Identifier ident) {
  return Factory()->NewIdentifierExpr(DUMMY_POS, ident);
}

ast::Expr* CodeGen::PointerTo(ast::Identifier ident) {
  return PointerTo(MakeExpr(ident));
}

ast::Expr* CodeGen::PointerTo(ast::Expr* base) {
  return Factory()->NewUnaryOpExpr(DUMMY_POS, parsing::Token::Type::AMPERSAND, base);
}

ast::Expr* CodeGen::MemberExpr(ast::Identifier lhs, ast::Identifier rhs) {
  ast::Expr* object = MakeExpr(lhs);
  ast::Expr* member = MakeExpr(rhs);
  return Factory()->NewMemberExpr(DUMMY_POS, object, member);
}

ast::Expr* CodeGen::IntToSql(i32 num) {
  ast::Expr* int_lit = IntLiteral(num);
  OneArgCall(ast::Builtin::IntToSql, int_lit);
}

}  // namespace tpl::compiler
