#pragma once

#include <string>
#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/util/region.h"
#include "parser/expression_defs.h"
#include "type/transient_value.h"
#include "type/type_id.h"
#include "type/transient_value_peeker.h"
#include "execution/sema/error_reporter.h"
#include "catalog/catalog_accessor.h"

namespace terrier::execution::compiler {

class FunctionBuilder;

/**
 * Bundles convenience methods needed by other classes during code generation.
 * Ideally no other class should directly use the AST node factory (except FunctionBuilder).
 * If you realize that you are generating the same code over and over again, or creating complex logic,
 * chances this class can help you simplify things. If not, add helper methods here to keep the translation code
 * clean and debuggable.
 */
class CodeGen {
 public:
  /**
   * Constructor
   * @param ctx code context to use
   */
  explicit CodeGen(catalog::CatalogAccessor *accessor);

  /**
   * @return region used for allocation
   */
  util::Region *Region() { return &region_; }

  /**
   * @return the ast node factory
   */
  ast::AstNodeFactory *Factory() { return &factory_; }

  /**
   * @return the ast context
   */
  ast::Context *Context() { return &ast_ctx_; }

  /**
   * @return the catalog accessor
   */
  catalog::CatalogAccessor *Accessor() { return accessor_; }

  /**
   * @return the error reporter
   */
  sema::ErrorReporter * Reporter() {
    return &error_reporter_;
  }

  /**
   * @return the state's identifier
   */
  ast::Identifier GetStateVar() { return state_var_; }

  /**
   * @return the state's type
   */
  ast::Identifier GetStateType() { return state_struct_; }

  /**
   * @return the exec ctx's identifier
   */
  ast::Identifier GetExecCtxVar() { return exec_ctx_var_; }

  /**
   * Creates the File node for the query
   * @param top_level_decls the list of top level declarations
   * @return the File node
   */
  ast::File *Compile(util::RegionVector<ast::Decl *> &&top_level_decls) {
    return Factory()->NewFile(DUMMY_POS, std::move(top_level_decls));
  }

  /**
   * @return the main function identifier
   */
  ast::Identifier GetMainFn() { return main_fn_; }

  /**
   * @return the setup function identifier
   */
  ast::Identifier GetSetupFn() { return setup_fn_; }

  /**
   * @return the teardown function identifier
   */
  ast::Identifier GetTeardownFn() { return teardown_fn_; }

  /**
   * @return the list of parameters of the main function
   */
  util::RegionVector<ast::FieldDecl *> MainParams();

  /**
   * @return the list of parameters of functions call by main (setup, teardown, pipeline_i)
   */
  util::RegionVector<ast::FieldDecl *> ExecParams();

  /**
   * Calls one of functions called by main
   * @return the fn_name(state, execCtx) call.
   */
  ast::Stmt *ExecCall(ast::Identifier fn_name);

  /**
   * Return a pointer to a state member
   * @param ident identifier of the member
   * @return the expression &state.ident
   */
  ast::Expr *GetStateMemberPtr(ast::Identifier ident);

  /**
   * Creates a field declaration
   * @param field_name name of field
   * @param field_type type of the field
   * @return the field declaration {field_name : field_type}
   */
  ast::FieldDecl *MakeField(ast::Identifier field_name, ast::Expr *field_type) {
    return Factory()->NewFieldDecl(DUMMY_POS, field_name, field_type);
  }

  /**
   * Declares a struct
   * @param struct_name name of the struct
   * @param fields fields of the struct
   * @return the struct declaration.
   */
  ast::Decl *MakeStruct(ast::Identifier struct_name, util::RegionVector<ast::FieldDecl *> &&fields);

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
  ast::Expr *BuiltinFunction(ast::Builtin builtin);

  /**
   * @param kind the type to get
   * @return An IdentifierExpr around the type
   */
  ast::Expr *BuiltinType(ast::BuiltinType::Kind kind);

  /**
   * Return the tpl type corresponding to an aggregate.
   */
  ast::Expr *AggregateType(terrier::parser::ExpressionType agg_type, terrier::type::TypeId ret_type);

  /**
   * Return a pointer type with the given base type (i.e *base_type)
   */
  ast::Expr *PointerType(ast::Identifier base_type);

  /**
   * Return a pointer type with the given base type (i.e *base_expr)
   */
  ast::Expr *PointerType(ast::Expr *base_expr);

  /**
   * Return the type represented by [num_elems]kind;
   */
  ast::Expr *ArrayType(uint64_t num_elems, ast::BuiltinType::Kind kind);

  /**
   * Return the expression arr[idx]
   */
  ast::Expr *ArrayAccess(ast::Identifier arr, uint64_t idx);

  /**
   * Declares variable
   * @param name name of the variable
   * @param typ type of the variable (nullptr if inferred)
   * @param init initial value of the variable (nullptr for no initial value)
   * @return the stmt containing the declaration
   */
  ast::Stmt *DeclareVariable(ast::Identifier name, ast::Expr *typ, ast::Expr *init);

  /**
   * Convert an expression to stmt
   * @param expr expression to convert
   * @return the statement wrapping around the expression
   */

  ast::Stmt *MakeStmt(ast::Expr *expr);

  /**
   * Generate code for lhs = rhs;
   */
  ast::Stmt *Assign(ast::Expr *lhs, ast::Expr *rhs);

  /**
   * Turn an identifier into a expression
   */
  ast::Expr *MakeExpr(ast::Identifier ident);

  /**
   * Return the expression &ident.
   */
  ast::Expr *PointerTo(ast::Identifier ident);

  /**
   * Return the expression &expr.
   */
  ast::Expr *PointerTo(ast::Expr *base);

  /**
   * Return lhs.rhs
   */
  ast::Expr *MemberExpr(ast::Identifier lhs, ast::Identifier rhs);

  /**
   * Convert a raw int to a sql int
   */
  ast::Expr *IntToSql(int64_t num);

  /**
   * Convert a raw float to a sql float
   */
  ast::Expr *FloatToSql(double num);

  /**
   * Convert to sql date
   */
  ast::Expr *DateToSql(int16_t year, uint8_t month, uint8_t day);

  /**
   * Convert to sql string
   */
  ast::Expr *StringToSql(std::string_view str);

  /**
   * Return the integer literal representing num
   */
  ast::Expr *IntLiteral(int64_t num) { return Factory()->NewIntLiteral(DUMMY_POS, num); }

  /**
   * Return the float literal representing num
   */
  ast::Expr *FloatLiteral(double num) { return Factory()->NewFloatLiteral(DUMMY_POS, num); }

  /**
   * Return boolean literal with the given value.
   */
  ast::Expr *BoolLiteral(bool value) { return Factory()->NewBoolLiteral(DUMMY_POS, value); }

  /**
   * Return nil literal.
   */
  ast::Expr *NilLiteral() { return Factory()->NewNilLiteral(DUMMY_POS); }

  /**
   * Compares lhs to rhs
   * @param comp_type type of the comparison
   * @param lhs left hand side of the comparison
   * @param rhs right hand side of the comparison
   * @return the generated comparison expression.
   */
  ast::Expr *Compare(parsing::Token::Type comp_type, ast::Expr *lhs, ast::Expr *rhs) {
    return Factory()->NewComparisonOpExpr(DUMMY_POS, comp_type, lhs, rhs);
  }

  /**
   * Perform the operation lhs op rhs
   * @param op_type type of the operator
   * @param lhs left hand side of the operation
   * @param rhs right hand side of the operation
   * @return the generated operation.
   */
  ast::Expr *BinaryOp(parsing::Token::Type op_type, ast::Expr *lhs, ast::Expr *rhs) {
    return Factory()->NewBinaryOpExpr(DUMMY_POS, op_type, lhs, rhs);
  }

  /**
   * Perform the operation op(operand)
   * @param op_type type of the operator
   * @param operand operand of the operation
   * @return the generated operation.
   */
  ast::Expr *UnaryOp(parsing::Token::Type op_type, ast::Expr *operand) {
    return Factory()->NewUnaryOpExpr(DUMMY_POS, op_type, operand);
  }

  /**
   * Generates: return expr;
   */
  ast::Stmt *ReturnStmt(ast::Expr *expr) { return Factory()->NewReturnStmt(DUMMY_POS, expr); }

  /**
   * @return a new identifier
   */
  ast::Identifier NewIdentifier();

  /**
   * @param prefix the prefix of the identifier
   * @return a unique identifier beginning with the given prefix
   */
  ast::Identifier NewIdentifier(const std::string &prefix);

  /**
   * @return an empty BlockStmt
   */
  ast::BlockStmt *EmptyBlock();

  /////////////////////////////////////////////////////////////////
  /// Builtin functions
  /// Implement one function for each builtin. It makes
  /// the translators easier to write.
  /////////////////////////////////////////////////////////////////

  /**
   * Note: This is a bit more convenient that the expression based implementation
   * @param base base type of the pointer
   * @param arg argument to cast
   * @return the builtin call ptrCast(*base, arg)
   */
  ast::Expr *PtrCast(ast::Identifier base, ast::Expr *arg);

  /**
   * @param base base type of the pointer
   * @param arg argument to cast
   * @return the builtin call ptrCase(*base, arg)
   */
  ast::Expr *PtrCast(ast::Expr *base, ast::Expr *arg);

  /**
   * Call outputAlloc(execCtx)
   */
  ast::Expr *OutputAlloc();

  /**
   * Call outputFinalize(execCtx)
   */
  ast::Expr *OutputFinalize();

  /**
   * Call tableIterInit(&tvi, execCtx, table_oid, col_oids)
   */
  ast::Expr *TableIterInit(ast::Identifier tvi, uint32_t table_oid, ast::Identifier col_oids);

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
   * Call tableIterReset(&tvi)
   */
  ast::Expr *TableIterReset(ast::Identifier tvi);

  /**
   * Call pciHasNext(pci) or pciHasNextFiltered(pci)
   */
  ast::Expr *PCIHasNext(ast::Identifier pci, bool filtered);

  /**
   * Call pciAdvance(pci) or pciAdvanceFiltered(pci)
   */
  ast::Expr *PCIAdvance(ast::Identifier pci, bool filtered);

  /**
   * Call pciGetTypeNullable(pci, idx)
   */
  ast::Expr *PCIGet(ast::Identifier pci, terrier::type::TypeId type, bool nullable, uint32_t idx);

  /**
   * Call pciSetPosition(pci, idx) or pciSetPositionFiltered(pci, idx)
   */
  // ast::Expr *PCISetPosition(ast::Identifier pci, uint32_t idx, bool filtered);

  /**
   * Call filterCompType(pci, col_idx, col_type, filter_val)
   */
  ast::Expr *PCIFilter(ast::Identifier pci, terrier::parser::ExpressionType comp_type, uint32_t col_idx,
                       terrier::type::TypeId col_type, ast::Expr *filter_val);

  /**
   * Call hash(arg1, ..., argN)
   */
  ast::Expr *Hash(util::RegionVector<ast::Expr *> &&args);

  /**
   * Call execCtxGetMem(execCtx)
   */
  ast::Expr *ExecCtxGetMem();

  /**
   * Call sizeOf(type)
   */
  ast::Expr *SizeOf(ast::Identifier type_name);

  /**
   * Call aggHTInit(&state.ht, execCtxGetMem(execCtx), sizeOf(payload_struct))
   */
  ast::Expr *AggHashTableInit(ast::Identifier ht, ast::Identifier payload_struct);

  /**
   * Call aggHTLookup(&state.ht, hash_val, keyCheck, &values)
   */
  ast::Expr *AggHashTableLookup(ast::Identifier ht, ast::Identifier hash_val, ast::Identifier key_check,
                                ast::Identifier values);

  /**
   * Call aggHTInsert(&state.ht, hash_val)
   */
  ast::Expr *AggHashTableInsert(ast::Identifier ht, ast::Identifier hash_val);

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
  ast::Expr *AggInit(ast::Expr *agg);

  /**
   * Call aggAdvance(agg, val)
   */
  ast::Expr *AggAdvance(ast::Expr *agg, ast::Expr *val);

  /**
   * Call aggResult(agg)
   */
  ast::Expr *AggResult(ast::Expr *agg);

  /**
   * Call joinHTInit(&ht, execCtxGetMem(execCtx), &build_struct)
   */
  ast::Expr *JoinHashTableInit(ast::Identifier ht, ast::Identifier build_struct);

  /**
   * Call joinHTInsert(&ht, hash_val)
   */
  ast::Expr *JoinHashTableInsert(ast::Identifier ht, ast::Identifier hash_val);

  /**
   * Call joinHTIterInit(&iter, &state.ht, hash_val)
   */
  ast::Expr *JoinHashTableIterInit(ast::Identifier iter, ast::Identifier ht, ast::Identifier hash_val);

  /**
   * joinHTIterHasNext(&iter, checkKey, state, probe_row) if is_probe_ptr is true.
   * Otherwise calls joinHTIterHasNext(&iter, checkKey, state, &probe_row).
   * This is needed because the previous operator in the pipeline may return a pointer type.
   * We don't want to pass a double pointer into the keyCheck function because it will complicate the logic.
   */
  ast::Expr *JoinHashTableIterHasNext(ast::Identifier iter, ast::Identifier check_key, ast::Identifier probe_row,
                                      bool is_probe_ptr);

  /**
   * Call joinHTIterGetRow(&iter)
   */
  ast::Expr *JoinHashTableIterGetRow(ast::Identifier iter);

  /**
   * Call joinHTIterClose(&iter)
   */
  ast::Expr *JoinHashTableIterClose(ast::Identifier iter);

  /**
   * Call joinHTIBuild(&state.ht)
   */
  ast::Expr *JoinHashTableBuild(ast::Identifier ht);

  /**
   * Call joinHTFree(&state.ht)
   */
  ast::Expr *JoinHashTableFree(ast::Identifier iter);

  /**
   * Call sorterInit(&state.sorter, execCtxGetMem(execCtx), comp_fn, sizeOf(sorter_struct))
   */
  ast::Expr *SorterInit(ast::Identifier sorter, ast::Identifier comp_fn, ast::Identifier sorter_struct);

  /**
   * Call sorterInsert(&state.sorter)
   */
  ast::Expr *SorterInsert(ast::Identifier sorter);

  /**
   * Call sorterSort(&state.sorter)
   */
  ast::Expr *SorterSort(ast::Identifier sorter);

  /**
   * Call sorterFree(&state.sorter)
   */
  ast::Expr *SorterFree(ast::Identifier sorter);

  /**
   * Call sorterIterInit(&iter, &state.sorter)
   */
  ast::Expr *SorterIterInit(ast::Identifier iter, ast::Identifier sorter);

  /**
   * Call sorterIterHasNext(&iter)
   */
  ast::Expr *SorterIterHasNext(ast::Identifier iter);

  /**
   * Call sorterIterNext(&iter)
   */
  ast::Expr *SorterIterNext(ast::Identifier iter);

  /**
   * Call sorterIterGet(&iter)
   */
  ast::Expr *SorterIterGetRow(ast::Identifier iter);

  /**
   * Call sorterIterClose(&iter)
   */
  ast::Expr *SorterIterClose(ast::Identifier iter);

  /**
   * Call indexIteratorInit(&iter, execCtx, table_oid, index_oid, col_oids)
   */
  ast::Expr *IndexIteratorInit(ast::Identifier iter, uint32_t table_oid, uint32_t index_oid, ast::Identifier col_oids);

  /**
   * Call IndexIteratorScanKey(&iter)
   */
  ast::Expr *IndexIteratorScanKey(ast::Identifier iter);

  /**
   * Call IndexIteratorGetIndexPR(&iter)
   */
  ast::Expr *IndexIteratorGetIndexPR(ast::Identifier iter);

  /**
   * Call IndexIteratorGetTablePR(&iter)
   */
  ast::Expr *IndexIteratorGetTablePR(ast::Identifier iter);

  /**
   * Call IndexIteratorAdvance(&iter)
   */
  ast::Expr *IndexIteratorAdvance(ast::Identifier iter);

  /**
   * Call IndexIteratorFree(&iter)
   */
  ast::Expr *IndexIteratorFree(ast::Identifier iter);

  /**
   * Call PrGet(&iter, attr_idx)
   */
  ast::Expr *PRGet(ast::Expr* iter_ptr, terrier::type::TypeId type, bool nullable, uint32_t attr_idx);
  ast::Expr *PRGet(ast::Identifier iter, terrier::type::TypeId type, bool nullable, uint32_t attr_idx);

  /**
   * Call PrSet(&iter, attr_idx, val)
   */
  ast::Expr *PRSet(ast::Expr* iter_ptr, terrier::type::TypeId type, bool nullable, uint32_t attr_idx, ast::Expr *val);
  ast::Expr *PRSet(ast::Identifier iter, terrier::type::TypeId type, bool nullable, uint32_t attr_idx, ast::Expr *val);

 private:
  /**
   * Many functions take a pointer to an identifier as their argument.
   * This function helps avoid code repetition
   * @param builtin builtin function to call
   * @param ident argument to the function
   * @param take_ptr whether to take the pointer to the identifier (for example, pci calls will set this to false)
   * @return  Calls the builtin function with the given identifier as its argument
   */
  ast::Expr *OneArgCall(ast::Builtin builtin, ast::Identifier ident, bool take_ptr = true);

  // Same as above, but the argument is part of the state object.
  // This used by join build & free, sorter sort & free, and aggregator free methods.
  ast::Expr *OneArgStateCall(ast::Builtin builtin, ast::Identifier ident);

  // Same as above, but the argument expression is directly passed in.
  ast::Expr *OneArgCall(ast::Builtin builtin, ast::Expr *arg);

  /**
   * JoinHT and AggHT are initialized the same way.
   * The function dedups the code.
   */
  ast::Expr *InitCall(ast::Builtin builtin, ast::Identifier object, ast::Identifier struct_type);

  uint64_t id_count_{0};

  // Helper objects
  util::Region region_;
  sema::ErrorReporter error_reporter_;
  ast::Context ast_ctx_;
  ast::AstNodeFactory factory_;
  catalog::CatalogAccessor * accessor_;

  // Identifiers that are always needed
  ast::Identifier state_struct_;
  ast::Identifier state_var_;
  ast::Identifier exec_ctx_var_;
  ast::Identifier main_fn_;
  ast::Identifier setup_fn_;
  ast::Identifier teardown_fn_;
};

}  // namespace terrier::execution::compiler
