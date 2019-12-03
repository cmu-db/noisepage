#pragma once

#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_accessor.h"
#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/sema/error_reporter.h"
#include "execution/util/region.h"
#include "parser/expression_defs.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "type/transient_value.h"
#include "type/transient_value_peeker.h"
#include "type/type_id.h"

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
   * TODO(Amadou): This implicitly ties this object to a transaction. May not be what we want.
   * @param accessor The catalog accessor
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
  sema::ErrorReporter *Reporter() { return &error_reporter_; }

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
  /// Convenience builtin functions
  /// Only add the complex ones or the oft called ones here.
  /// The other builtins can be handled by the translators.
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
   * Call tableIterInit(&tvi, execCtx, table_oid, col_oids)
   */
  ast::Expr *TableIterInit(ast::Identifier tvi, uint32_t table_oid, ast::Identifier col_oids);

  /**
   * Call pciGetTypeNullable(pci, idx)
   */
  ast::Expr *PCIGet(ast::Identifier pci, terrier::type::TypeId type, bool nullable, uint32_t idx);

  /**
   * Call filterCompType(pci, col_idx, col_type, filter_val)
   */
  ast::Expr *PCIFilter(ast::Identifier pci, terrier::parser::ExpressionType comp_type, uint32_t col_idx,
                       terrier::type::TypeId col_type, ast::Expr *filter_val);

  /**
   * Call execCtxGetMem(execCtx)
   */
  ast::Expr *ExecCtxGetMem();

  /**
   * Call sizeOf(type)
   */
  ast::Expr *SizeOf(ast::Identifier type_name);

  /**
   * Call indexIteratorInit(&iter, execCtx, table_oid, index_oid, col_oids)
   */
  ast::Expr *IndexIteratorInit(ast::Identifier iter, uint32_t table_oid, uint32_t index_oid, ast::Identifier col_oids);

  /**
   * Call IndexIteratorScanType(&iter[, limit])
   */
  ast::Expr *IndexIteratorScan(ast::Identifier iter, planner::IndexScanType scan_type, uint32_t limit);

  /**
   * Call PrGet(&iter, attr_idx)
   */
  ast::Expr *PRGet(ast::Expr *iter_ptr, terrier::type::TypeId type, bool nullable, uint32_t attr_idx);

  /**
   * Same as above, but use an identifier instead of a raw expression.
   */
  ast::Expr *PRGet(ast::Identifier iter, terrier::type::TypeId type, bool nullable, uint32_t attr_idx);

  /**
   * Call PrSet(&iter, attr_idx, val)
   */
  ast::Expr *PRSet(ast::Expr *iter_ptr, terrier::type::TypeId type, bool nullable, uint32_t attr_idx, ast::Expr *val);

  /**
   * Same as above, but use an identifier instead of a raw expression.
   */
  ast::Expr *PRSet(ast::Identifier iter, terrier::type::TypeId type, bool nullable, uint32_t attr_idx, ast::Expr *val);

  /**
   * Call storageInterfaceInit(&storage_interface, execCtx, table_oid, col_oids, need_indexes)
   */
  ast::Expr *StorageInterfaceInit(ast::Identifier si, uint32_t table_oid, ast::Identifier col_oids, bool need_indexes);

  /**
   * Make a generic builtin call with the given arguments.
   */
  ast::Expr *BuiltinCall(ast::Builtin builtin, std::vector<ast::Expr *> &&params);

  /**
   * This is for functions that take one identifier or a pointer to an identifier as their argument.
   * @param builtin builtin function to call
   * @param ident argument to the function
   * @param take_ptr whether to take the pointer to the identifier (for example, pci calls will set this to false)
   * @return  Calls the builtin function with the given identifier as its argument
   */
  ast::Expr *OneArgCall(ast::Builtin builtin, ast::Identifier ident, bool take_ptr = true);

  /**
   * JoinHT and AggHT are initialized the same way.
   * The function dedups the code.
   */
  ast::Expr *HTInitCall(ast::Builtin builtin, ast::Identifier object, ast::Identifier struct_type);

  /**
   * This is for function this take one state argument.
   */
  ast::Expr *OneArgStateCall(ast::Builtin builtin, ast::Identifier ident);

 private:
  // Same as above, but the argument expression is directly passed in.
  ast::Expr *OneArgCall(ast::Builtin builtin, ast::Expr *arg);

  uint64_t id_count_{0};

  // Helper objects
  util::Region region_;
  sema::ErrorReporter error_reporter_;
  ast::Context ast_ctx_;
  ast::AstNodeFactory factory_;
  catalog::CatalogAccessor *accessor_;

  // Identifiers that are always needed
  ast::Identifier state_struct_;
  ast::Identifier state_var_;
  ast::Identifier exec_ctx_var_;
  ast::Identifier main_fn_;
  ast::Identifier setup_fn_;
  ast::Identifier teardown_fn_;
};

}  // namespace terrier::execution::compiler
