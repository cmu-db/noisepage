#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "brain/operating_unit.h"
#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/sema/error_reporter.h"
#include "execution/util/region.h"
#include "parser/expression_defs.h"
#include "planner/plannodes/plan_node_defs.h"
#include "type/type_id.h"

namespace terrier::parser {
class ConstantValueExpression;
}  // namespace terrier::parser

namespace terrier::catalog {
class CatalogAccessor;
}  // namespace terrier::catalog

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
   * @param exec_ctx The execution context
   */
  explicit CodeGen(exec::ExecutionContext *exec_ctx);

  /**
   * Prevent copy and move
   */
  DISALLOW_COPY_AND_MOVE(CodeGen);

  /**
   * @return region used for allocation
   */
  util::Region *Region() { return region_.get(); }

  /**
   * @return release ownership of the region used for allocation
   */
  std::unique_ptr<util::Region> ReleaseRegion() { return std::move(region_); }

  /**
   * @return the ast node factory
   */
  ast::AstNodeFactory *Factory() { return &factory_; }

  /**
   * @return the ast context
   */
  ast::Context *Context() { return ast_ctx_.get(); }

  /**
   * @return release ownership of the ast context
   */
  std::unique_ptr<ast::Context> ReleaseContext() { return std::move(ast_ctx_); }

  /**
   * @return the catalog accessor
   */
  catalog::CatalogAccessor *Accessor();

  /**
   * @return the execution context
   */
  exec::ExecutionContext *ExecCtx() { return exec_ctx_; }

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
   * @return PipelineOperatingUnits instance
   */
  brain::PipelineOperatingUnits *GetPipelineOperatingUnits() { return pipeline_operating_units_.get(); }

  /**
   * @return release ownership of the PipelineOperatingUnits instance
   */
  std::unique_ptr<brain::PipelineOperatingUnits> ReleasePipelineOperatingUnits() {
    return std::move(pipeline_operating_units_);
  }

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
   * The main function has the signature: (execCtx: *ExecutionContext) -> int32
   * @return the list of parameters of the main function
   */
  util::RegionVector<ast::FieldDecl *> MainParams();

  /**
   * Functions called by main have the signature: (state: *State, execCtx: *ExecutionContext) -> nil
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
   * @param const_val the CVE to read
   * @return the value stored in transient_val
   */
  ast::Expr *PeekValue(const parser::ConstantValueExpression &const_val);

  /**
   * Convert from terrier type to TPL expr type
   * @param type The terrier type
   * @return The corresponding tpl type
   */
  ast::Expr *TplType(terrier::type::TypeId type);

  /**
   * @param builtin builtin function to get
   * @return an IdentifierExpr around the builtin function.
   */
  ast::Expr *BuiltinFunction(ast::Builtin builtin);

  /**
   * @param kind The kind of the builtin type.
   * @return An IdentifierExpr corresponding to the type.
   */
  ast::Expr *BuiltinType(ast::BuiltinType::Kind kind);

  /**
   * @return the tpl type corresponding to an aggregate.
   */
  ast::Expr *AggregateType(terrier::parser::ExpressionType agg_type, terrier::type::TypeId ret_type);

  /**
   * @return a pointer type with the given base type (i.e *base_type)
   */
  ast::Expr *PointerType(ast::Identifier base_type);

  /**
   * @return a pointer type with the given base type (i.e *base_expr)
   */
  ast::Expr *PointerType(ast::Expr *base_expr);

  /**
   * @return the type represented by [num_elems]kind;
   */
  ast::Expr *ArrayType(uint64_t num_elems, ast::BuiltinType::Kind kind);

  /**
   *
   * @return the expression arr[idx]
   */
  ast::Expr *ArrayAccess(ast::Identifier arr, uint64_t idx);

  /**
   * Declares variable
   * @param name name of the variable
   * @param type type of the variable (nullptr if inferred)
   * @param init initial value of the variable (nullptr for no initial value)
   * @return the stmt containing the declaration
   */
  ast::Stmt *DeclareVariable(ast::Identifier name, ast::Expr *type, ast::Expr *init);

  /**
   * Convert an expression to statement
   * @param expr expression to convert
   * @return the statement wrapping around the expression
   */
  ast::Stmt *MakeStmt(ast::Expr *expr);

  /**
   * Generate code for lhs = rhs;
   * @param lhs Left hand side of the assignment
   * @param rhs Right hand side of the assigment.
   * @return The assignment statement.
   */
  ast::Stmt *Assign(ast::Expr *lhs, ast::Expr *rhs);

  /**
   * Turn an identifier into a expression.
   * For example if ident="x". This will return a tpl expression corresponding to just "x".
   * @param ident The identifier to convert.
   * @return The expression around the given identifier.
   */
  ast::Expr *MakeExpr(ast::Identifier ident);

  /**
   * Return the pointer to the given identifier.
   * For example if ident="x". This will return a tpl expression corresponding "&x".
   * @param ident The identifier to point to.
   * @return The pointer to the identifer.
   */
  ast::Expr *PointerTo(ast::Identifier ident);

  /**
   * Takes a pointer to the given expression.
   * @param base The expression to point to
   * @return The pointer to the expression.
   */
  ast::Expr *PointerTo(ast::Expr *base);

  /**
   * Constructs a membership expression: lhs.rhs
   * @param lhs The left hand side of the expression.
   * @param rhs The right hand side of the expression.
   * @return The membership expression.
   */
  ast::Expr *MemberExpr(ast::Identifier lhs, ast::Identifier rhs);

  /**
   * @param expr The expression to be checked.
   * @return The generated null check.
   */
  ast::Expr *IsSqlNull(ast::Expr *expr);

  /**
   * @param expr The expression to be checked.
   * @return The generated not null check.
   */
  ast::Expr *IsSqlNotNull(ast::Expr *expr);

  /**
   * @param expr An expression whose type is the type of NULL to create.
   * @return The generated NULL.
   */
  ast::Expr *NullToSql(ast::Expr *expr);
  /**
   * @param num The number to convert to a sql Integer.
   * @return The generated sql Integer
   */
  ast::Expr *IntToSql(int64_t num);

  /**
   * @param num The number to convert to a sql Real.
   * @return The generated sql Real.
   */
  ast::Expr *FloatToSql(double num);

  /**
   * Create a date value.
   * @param year The year of the date.
   * @param month The month of the date.
   * @param day The day of the date
   * @return The generated sql Date.
   */
  ast::Expr *DateToSql(int32_t year, uint32_t month, uint32_t day);

  /**
   * Create a timestamp value.
   * @param julian_usec The number of microseconds in Julian time.
   * @return The generated sql Timestamp.
   */
  ast::Expr *TimestampToSql(uint64_t julian_usec);

  /**
   * Convert a raw string to a sql StringVal.
   * @param str The string to convert to a sql StringVal.
   * @return The generate sql StringVal
   */
  ast::Expr *StringToSql(std::string_view str);

  /**
   * @return The integer literal representing num
   */
  ast::Expr *IntLiteral(int64_t num) { return Factory()->NewIntLiteral(DUMMY_POS, num); }

  /**
   * @return The float literal representing num
   */
  ast::Expr *FloatLiteral(double num) { return Factory()->NewFloatLiteral(DUMMY_POS, num); }

  /**
   * @return The boolean literal with the given value.
   */
  ast::Expr *BoolLiteral(bool value) { return Factory()->NewBoolLiteral(DUMMY_POS, value); }

  /**
   * @return The nil literal.
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
   * Generates: return expr.
   * @param expr The expression to return
   * @return The generated return statement.
   */
  ast::Stmt *ReturnStmt(ast::Expr *expr) { return Factory()->NewReturnStmt(DUMMY_POS, expr); }

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
   * @param tvi The identifier of table vector iterator
   * @param table_oid The oid of the table to iterate through
   * @param col_oids The identifier of the array of column oids to read.
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *TableIterInit(ast::Identifier tvi, uint32_t table_oid, ast::Identifier col_oids);

  /**
   * Call pciGetTypeNullable(pci, idx)
   * @param pci The identifier of the projected columns iterator
   * @param type The type of the column being accessed.
   * @param nullable Whether the column being accessed is nullable.
   * @param idx Index of the column being accessed.
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *PCIGet(ast::Identifier pci, terrier::type::TypeId type, bool nullable, uint32_t idx);

  /**
   * Call filterCompType(pci, col_idx, col_type, filter_val)
   * @param pci The identifier of the projected columns iterator
   * @param comp_type The type of comparison being performed.
   * @param col_idx Index of the column being filtered.
   * @param col_type The type of the column being filtered.
   * @param filter_val The value to filter byc
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *PCIFilter(ast::Identifier pci, terrier::parser::ExpressionType comp_type, uint32_t col_idx,
                       terrier::type::TypeId col_type, ast::Expr *filter_val);

  /**
   * Call execCtxGetMem(execCtx)
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *ExecCtxGetMem();

  /**
   * Call sizeOf(type)
   * @param type_name The type name of argument to sizeOf.
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *SizeOf(ast::Identifier type_name);

  /**
   * Call indexIteratorInit(&iter, execCtx, table_oid, index_oid, col_oids)
   * @param iter The identifier of the index iterator.
   * @param num_attrs Number of attributes
   * @param table_oid The oid of the index's table.
   * @param index_oid The oid the index.
   * @param col_oids The identifier of the array of column oids to read.
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *IndexIteratorInit(ast::Identifier iter, uint32_t num_attrs, uint32_t table_oid, uint32_t index_oid,
                               ast::Identifier col_oids);

  /**
   * Call IndexIteratorScanType(&iter[, limit])
   * @param iter The identifier of the index iterator.
   * @param scan_type The type of scan to perform.
   * @param limit The limit of the scan in case of limited scans.
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *IndexIteratorScan(ast::Identifier iter, planner::IndexScanType scan_type, uint32_t limit);

  /**
   * Call PrGet(pr, attr_idx)
   * @param pr The projected row being accessed.
   * @param type The type of the column being accessed.
   * @param nullable Whether the column being accessed is nullable.
   * @param attr_idx Index of the column being accessed.
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *PRGet(ast::Expr *pr, terrier::type::TypeId type, bool nullable, uint32_t attr_idx);

  /**
   * Call PrSet(pr, attr_idx, val)
   * @param pr The projected row being accessed.
   * @param type The type of the column being accessed.
   * @param nullable Whether the column being accessed is nullable.
   * @param attr_idx Index of the column being accessed.
   * @param val The value to set the column to.
   * @param own When inserting varchars, whether the VarlenEntry should own its content.
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *PRSet(ast::Expr *pr, terrier::type::TypeId type, bool nullable, uint32_t attr_idx, ast::Expr *val,
                   bool own = false);

  /**
   * Call storageInterfaceInit(&storage_interface, execCtx, table_oid, col_oids, need_indexes)
   * @param si The storage interface to initialize
   * @param table_oid The oid of the table being accessed.
   * @param col_oids The identifier of the array of column oids to access.
   * @param need_indexes Whether the storage interface will need to use indexes
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *StorageInterfaceInit(ast::Identifier si, uint32_t table_oid, ast::Identifier col_oids, bool need_indexes);

  /**
   * Make a generic builtin call with the given arguments.
   * @param builtin builtin function to call
   * @param params parameters of the builtin function
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *BuiltinCall(ast::Builtin builtin, std::vector<ast::Expr *> &&params);

  /**
   * This is for functions that take one identifier or a pointer to an identifier as their argument.
   * @param builtin builtin function to call
   * @param ident argument to the function
   * @param take_ptr whether to take the pointer to the identifier (for example, pci calls will set this to false)
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *OneArgCall(ast::Builtin builtin, ast::Identifier ident, bool take_ptr = true);

  /**
   * JoinHT and AggHT are initialized the same way.
   * The function dedups the code.
   * @param builtin builtin function to call
   * @param object hash table to initialize.
   * @param struct_type identifier of the build struct.
   * @return The expression corresponding to the builtin call initializing the given hash table.
   */
  ast::Expr *HTInitCall(ast::Builtin builtin, ast::Identifier object, ast::Identifier struct_type);

  /**
   * This is for function this take one state argument.
   * @param builtin builtin function to call
   * @param ident identifier of the state element
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *OneArgStateCall(ast::Builtin builtin, ast::Identifier ident);

  /**
   * Generic way to call functions that take in one argument
   * @param builtin builtin function to call
   * @param arg argument to the function
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *OneArgCall(ast::Builtin builtin, ast::Expr *arg);

  /**
   * Generic way to call functions that take in no arguments
   * @param builtin builtin function to call
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *ZeroArgCall(ast::Builtin builtin);

 private:
  // Counter for the identifiers. Allows the creation of unique names.
  uint64_t id_count_{0};

  // Helper objects
  std::unique_ptr<util::Region> region_;
  sema::ErrorReporter error_reporter_;
  std::unique_ptr<ast::Context> ast_ctx_;
  ast::AstNodeFactory factory_;
  exec::ExecutionContext *exec_ctx_;
  std::unique_ptr<brain::PipelineOperatingUnits> pipeline_operating_units_;

  // Identifiers that are always needed
  // Identifier of the state struct
  ast::Identifier state_struct_;
  // Identifier of the state variable
  ast::Identifier state_var_;
  // Identifier of the execution context variable
  ast::Identifier exec_ctx_var_;
  /**
   * Identifier of the main function.
   * Signature: (execCtx: *ExecutionContext) -> int32
   */
  ast::Identifier main_fn_;
  /**
   * Identifier of the setup function
   * Signature: (state: *State, execCtx: *ExecutionContext) -> nil
   */
  ast::Identifier setup_fn_;
  /**
   * Identifier of the teardown function
   * Signature: (state: *State, execCtx: *ExecutionContext) -> nil
   */
  ast::Identifier teardown_fn_;
};

}  // namespace terrier::execution::compiler
