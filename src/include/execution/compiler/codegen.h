#pragma once

#include <string>
#include "execution/ast/ast.h"
#include "execution/ast/type.h"
#include "execution/ast/context.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/util/common.h"
#include "execution/util/region.h"
#include "execution/util/macros.h"
#include "type/transient_value.h"
#include "type/type_id.h"
#include "execution/compiler/function_builder.h"
#include "parser/expression_defs.h"
#include "execution/compiler/query.h"

namespace tpl::compiler {

class FunctionBuilder;

/**
 * Bundles convenience methods needed by other classes during code generation.
 */
class CodeGen {
 public:
  /**
   * Constructor
   * @param ctx code context to use
   */
  explicit CodeGen(Query* query);

  /**
   * @return region used for allocation
   */
  util::Region *Region() {
    return query_->GetRegion();
  }


  /**
   * @return the ast node factory
   */
  ast::AstNodeFactory * Factory() {
    return query_->GetFactory();
  }

  /**
   * @return the ast context
   */
  ast::Context * Context() {
    return query_->GetAstContext();
  }

  /**
   * @return the state's identifier
   */
  ast::Identifier GetStateVar() {
    return state_var_;
  }

  /**
   * @return the state's type
   */
  ast::Identifier GetStateType() {
    return state_struct_;
  }

  /**
   * @return the exec ctx's identifier
   */
  ast::Identifier GetExecCtxVar() {
    return exec_ctx_var_;
  }

  /**
   * @return the main function identifier
   */
  ast::Identifier GetMainFn() {
    return main_fn_;
  }

  /**
   * @return the setup function identifier
   */
  ast::Identifier GetSetupFn() {
    return setup_fn_;
  }

  /**
   * @return the teardown function identifier
   */
  ast::Identifier GetTeardownFn() {
    return teardown_fn_;
  }


  /**
   * @return the list of parameters of the main function
   */
  util::RegionVector<ast::FieldDecl*> MainParams();

  /**
   * @return the list of parameters of functions call by main (setup, teardown, pipeline_i)
   */
  util::RegionVector<ast::FieldDecl*> ExecParams();

  /**
   * Return a pointer to a state member
   * @param ident identifier of the member
   * @return the expression &state.ident
   */
  ast::Expr* GetStateMemberPtr(ast::Identifier ident);


  /**
   * Creates a field declaration
   * @param field_name name of field
   * @param field_type type of the field
   * @return the field declaration {field_name : field_type}
   */
  ast::FieldDecl* MakeField(ast::Identifier field_name, ast::Expr* field_type) {
    return Factory()->NewFieldDecl(DUMMY_POS, field_name, field_type);
  }

  /**
   * Declares a struct
   * @param struct_name name of the struct
   * @param fields fields of the struct
   * @return the struct declaration.
   */
  ast::Decl* MakeStruct(ast::Identifier struct_name, util::RegionVector<ast::FieldDecl*> && fields);

  /**
   * Prevent copy and move
   */
  DISALLOW_COPY_AND_MOVE(CodeGen);

  /**
   * @param transient_val the transient value to read
   * @return the value stored in transient_val
   */
  ast::Expr *PeekValue(const terrier::type::TransientValue &transient_val);

  /**
   * Convert from terrier type to TPL expr type
   */
  ast::Expr *TplType(terrier::type::TypeId type);

  /**
   * @param builtin builtin function to get
   * @return an IdentifierExpr around the builtin function.
   */
  ast::Expr* BuiltinFunction(ast::Builtin builtin);

  /**
   * @param kind the type to get
   * @return An IdentifierExpr around the type
   */
  ast::Expr* BuiltinType(ast::BuiltinType::Kind kind);

  /**
   * Return the tpl type corresponding to an aggregate.
   */
  ast::Expr* AggregateType(terrier::parser::ExpressionType type);


  /**
   * Return a pointer type with the given base type (i.e *base_type)
   */
  ast::Expr* PointerType(ast::Identifier base_type);

  /**
   * Return a pointer type with the given base type (i.e *base_expr)
   */
  ast::Expr* PointerType(ast::Expr* base_expr);

  /**
   * Declares variable
   * @param name name of the variable
   * @param typ type of the variable (nullptr if inferred)
   * @param init initial value of the variable (nullptr for no initial value)
   * @return the stmt containing the declaration
   */
  ast::Stmt* DeclareVariable(ast::Identifier name, ast::Expr* typ, ast::Expr* init);


  /**
   * Convert an expression to stmt
   * @param expr expression to convert
   * @return the statement wrapping around the expression
   */

  ast::Stmt* MakeStmt(ast::Expr* expr);

  /**
   * Generate code for lhs = rhs;
   */
  ast::Stmt* Assign(ast::Expr* lhs, ast::Expr* rhs);

  /**
   * Turn an identifier into a expression
   */
  ast::Expr* MakeExpr(ast::Identifier ident);

  /**
   * Return the expression &ident.
   */
  ast::Expr* PointerTo(ast::Identifier ident);

  /**
   * Return the expression &expr.
   */
  ast::Expr* PointerTo(ast::Expr* base);

  /**
   * Return lhs.rhs
   */
  ast::Expr* MemberExpr(ast::Identifier lhs, ast::Identifier rhs);


  /**
   * Convert a raw in to a sql int
   */
  ast::Expr* IntToSql(i32 num);

  /**
   * Return the integer literal representing num
   */
  ast::Expr* IntLiteral(i32 num) {
    return Factory()->NewIntLiteral(DUMMY_POS, num);
  }

  /**
   * Return the float literal representing num
   */
  ast::Expr* FloatLiteral(float_t num) {
    return Factory()->NewFloatLiteral(DUMMY_POS, num);
  }

  /**
   * Return boolean literal with the given value.
   */
  ast::Expr* BoolLiteral(bool value) {
    return Factory()->NewBoolLiteral(DUMMY_POS, value);
  }

  /**
   * Return nil literal.
   */
  ast::Expr* NilLiteral() {
    return Factory()->NewNilLiteral(DUMMY_POS);
  }

  /**
   * Compares lhs to rhs
   * @param comp_type type of the comparison
   * @param lhs left hand side of the comparison
   * @param rhs right hand side of the comparison
   * @return the generated comparison expression.
   */
  ast::Expr* Compare(parsing::Token::Type comp_type, ast::Expr* lhs, ast::Expr* rhs) {
    return Factory()->NewComparisonOpExpr(DUMMY_POS, comp_type, lhs, rhs);
  }

  /**
   * Perform the operation lhs op rhs
   * @param op_type type of the operator
   * @param lhs left hand side of the operation
   * @param rhs right hand side of the operation
   * @return the generated operation.
   */
  ast::Expr* BinaryOp(parsing::Token::Type op_type, ast::Expr* lhs, ast::Expr* rhs) {
    return Factory()->NewBinaryOpExpr(DUMMY_POS, op_type, lhs, rhs);
  }

  /**
   * Perform the operation op(operand)
   * @param op_type type of the operator
   * @param operand operand of the operation
   * @return the generated operation.
   */
  ast::Expr* UnaryOp(parsing::Token::Type op_type, ast::Expr* operand) {
    return Factory()->NewUnaryOpExpr(DUMMY_POS, op_type, operand);
  }

  /**
   * Generates: return expr;
   */
  ast::Stmt* ReturnStmt(ast::Expr* expr) {
    return Factory()->NewReturnStmt(DUMMY_POS, expr);
  }

  /**
   * @return a new identifier
   */
  ast::Identifier NewIdentifier();

  /**
   * @param prefix the prefix of the identifier
   * @return a unique identifier beginning with the given prefix
   */
  ast::Identifier NewIdentifier(const std::string & prefix);

  /**
   * @return an empty BlockStmt
   */
  ast::BlockStmt *EmptyBlock();


  /**
   * @param fn function to call
   * @param args arguments
   * @return a new call stmt
   */
  ast::Stmt *Call(ast::FunctionDecl *fn, util::RegionVector<ast::Expr *> &&args);


  /////////////////////////////////////////////////////////////////
  /// Builtin functions
  /////////////////////////////////////////////////////////////////

  /**
   * @param base base type of the pointer
   * @param arg argument to cast
   * @return the builtin call ptrCast(*base, arg)
   */
  ast::Expr *PtrCast(ast::Identifier base, ast::Expr* arg);

  /**
   * Call outputAlloc(execCtx)
   */
  ast::Expr *OutputAlloc();

  /**
   * Call outputAdvance(execCtx)
   */
  ast::Expr *OutputAdvance();

  /**
   * Call outputFinalize(execCtx)
   */
  ast::Expr *OutputFinalize();


  /**
   * TODO: Pass in OIDs instead of table names
   * @param tvi the iterator to initialize
   * @param table_name name of the table
   * @return the builtin call tableIterInit(&tvi, "table_name", execCtx)
   */
  ast::Expr *TableIterInit(ast::Identifier tvi, const std::string& table_name);

  /**
   * Call tableIterAdvance(&tvi)
   */
  ast::Expr *TableIterAdvance(ast::Identifier tvi);

  /**
   * Call tableIterGetPCI(&tvi)
   */
  ast::Expr *TableIterGetPCI(ast::Identifier tvi);

  /**
   * Call tableIterClose(&tvi)
   */
  ast::Expr *TableIterClose(ast::Identifier tvi);

  /**
   * Call pciHasNext(pci)
   */
  ast::Expr *PCIHasNext(ast::Identifier pci);

  /**
   * Call pciAdvance(pci)
   */
  ast::Expr *PCIAdvance(ast::Identifier pci);

  /**
   * Call pciGetType(pci, idx)
   */
  ast::Expr *PCIGet(ast::Identifier pci, terrier::type::TypeId type, uint32_t idx);

  /**
   * Call hash(arg1, ..., argN)
   */
  ast::Expr *Hash(util::RegionVector<ast::Expr*> && args);

  /**
   * Call execCtxGetMem(execCtx)
   */
  ast::Expr* ExecCtxGetMem();

  /**
   * Call sizeOf(type)
   */
  ast::Expr* SizeOf(ast::Identifier type_name);

  /**
   * Call aggHTInit(&state.ht, execCtxGetMem(execCtx), sizeOf(payload_struct))
   */
  ast::Expr *AggHashTableInit(ast::Identifier ht, ast::Identifier payload_struct);

  /**
   * Call aggHTLookup(&state.ht, hash_val, keyCheck, &values)
   */
  ast::Expr *AggHashTableLookup(ast::Identifier ht, ast::Identifier hash_val, ast::Identifier key_check, ast::Identifier values);

  /**
   * Call aggHTFree(&state.ht)
   */
  ast::Expr *AggHashTableFree(ast::Identifier ht);

  /**
   * Call aggHTIterInit(&iter, &state.ht)
   */
  ast::Expr *AggHashTableIterInit(ast::Identifier iter, ast::Identifier ht);

  /**
   * Call aggHTIterHasNext(&iter)
   */
  ast::Expr *AggHashTableIterHasNext(ast::Identifier iter);

  /**
   * Call aggHTIterNext(&iter)
   */
  ast::Expr *AggHashTableIterNext(ast::Identifier iter);

  /**
   * Call aggHTIterGetRow(&iter)
   */
  ast::Expr *AggHashTableIterGetRow(ast::Identifier iter);

  /**
   * Call aggHTIterClose(&iter)
   */
  ast::Expr *AggHashTableIterClose(ast::Identifier iter);

  /**
   * Call aggInit(agg)
   */
  ast::Expr *AggInit(ast::Expr* agg);

  /**
   * Call aggAdvance(agg, val)
   */
  ast::Expr *AggAdvance(ast::Expr* agg, ast::Expr* val);

  /**
   * Call aggResult(agg)
   */
  ast::Expr *AggResult(ast::Expr* agg);

  /**
   * Call joinHTInit(&ht, execCtxGetMem(execCtx), &build_struct)
   */
  ast::Expr* JoinHashTableInit(ast::Identifier ht, ast::Identifier build_struct);

  /**
   * Call joinHTInsert(&ht, hash_val)
   */
  ast::Expr* JoinHashTableInsert(ast::Identifier ht, ast::Identifier hash_val);

  /**
   * Call joinHTIterInit(&iter, &state.ht, hash_val)
   */
  ast::Expr* JoinHashTableIterInit(ast::Identifier iter, ast::Identifier ht, ast::Identifier hash_val);

  /**
   * joinHTIterHasNext(&iter, checkKey, state, probe_row) if is_probe_ptr is true.
   * Otherwise calls joinHTIterHasNext(&iter, checkKey, state, &probe_row).
   * This is needed because the previous operator in the pipeline may return a pointer type.
   * We don't want to pass a double pointer into the keyCheck function because it will complicate the logic.
   */
  ast::Expr* JoinHashTableIterHasNext(ast::Identifier iter, ast::Identifier check_key, ast::Identifier probe_row, bool is_probe_ptr);

  /**
   * Call joinHTIterGetRow(&iter)
   */
  ast::Expr* JoinHashTableIterGetRow(ast::Identifier iter);

  /**
   * Call joinHTIterClose(&iter)
   */
  ast::Expr* JoinHashTableIterClose(ast::Identifier iter);

  /**
   * Call joinHTIBuild(&state.ht)
   */
  ast::Expr* JoinHashTableBuild(ast::Identifier ht);

  /**
   * Call joinHTFree(&state.ht)
   */
  ast::Expr* JoinHashTableFree(ast::Identifier iter);
 private:
  /**
   * Many functions take a pointer to an identifier as their argument.
   * This function helps avoid code repetition
   * @param builtin builtin function to call
   * @param ident argument to the function
   * @param take_ptr whether to take the pointer to the identifier (for example, pci calls will set this to false)
   * @return
   */
  ast::Expr* OneArgCall(ast::Builtin builtin, ast::Identifier ident, bool take_ptr = true);

  // Same as above, but the argument is part of the state object.
  // This used by join build, sorter sort as well as all free methods.
  ast::Expr* OneArgStateCall(ast::Builtin builtin, ast::Identifier ident);

  // Same as above, but the argurment expression is directly passed in.
  ast::Expr* OneArgCall(ast::Builtin builtin, ast::Expr* arg);


  /**
   * Sorter, JoinHT and AggHT are all initialized the same way.
   * The function dedups the code.
   */
  ast::Expr *InitCall(ast::Builtin builtin, ast::Identifier object, ast::Identifier struct_type);



  u64 id_count_{0};
  Query * query_;

  // Identifiers that are always needed
  ast::Identifier state_struct_;
  ast::Identifier state_var_;
  ast::Identifier exec_ctx_var_;
  ast::Identifier main_fn_;
  ast::Identifier setup_fn_;
  ast::Identifier teardown_fn_;
};

}  // namespace tpl::compiler
