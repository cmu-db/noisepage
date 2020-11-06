#pragma once

#include <llvm/ADT/StringMap.h>

#include <array>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "brain/operating_unit.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/builtins.h"
#include "execution/ast/identifier.h"
#include "execution/ast/type.h"
#include "execution/sql/runtime_types.h"
#include "execution/sql/sql.h"
#include "parser/expression_defs.h"
#include "planner/plannodes/plan_node_defs.h"

namespace noisepage::catalog {
class CatalogAccessor;
}  // namespace noisepage::catalog

namespace noisepage::execution::compiler {

/**
 * Bundles convenience methods needed by other classes during code generation.
 */
class CodeGen {
  friend class If;
  friend class FunctionBuilder;
  friend class Loop;

  // The default number of cached scopes to keep around.
  static constexpr uint32_t DEFAULT_SCOPE_CACHE_SIZE = 4;

  /**
   * Scope object.
   */
  class Scope {
   public:
    // Create scope.
    explicit Scope(Scope *previous) : previous_(nullptr) { Init(previous); }

    // Initialize this scope.
    void Init(Scope *previous) {
      previous_ = previous;
      names_.clear();
    }

    // Get a fresh name in this scope.
    std::string GetFreshName(const std::string &name);

    // Return the previous scope.
    Scope *Previous() { return previous_; }

   private:
    // The previous scope_;
    Scope *previous_;
    // Map of name/identifier to next ID.
    llvm::StringMap<uint64_t> names_;
  };

 public:
  /**
   * RAII scope class.
   */
  class CodeScope {
   public:
    /**
     * Create a scope.
     * @param codegen The code generator instance.
     */
    explicit CodeScope(CodeGen *codegen) : codegen_(codegen) { codegen_->EnterScope(); }

    /**
     * Destructor. Exits current scope.
     */
    ~CodeScope() { codegen_->ExitScope(); }

   private:
    CodeGen *codegen_;
  };

  /**
   * Create a code generator that generates code for the provided container.
   * @param context The context used to create all expressions.
   * @param accessor The catalog accessor for all catalog lookups.
   */
  explicit CodeGen(ast::Context *context, catalog::CatalogAccessor *accessor);

  /**
   * Destructor.
   */
  ~CodeGen();

  // ---------------------------------------------------------------------------
  //
  // Constant literals
  //
  // ---------------------------------------------------------------------------

  /**
   * @return A literal whose value is the provided boolean value.
   */
  [[nodiscard]] ast::Expr *ConstBool(bool val) const;

  /**
   * @return A literal whose value is the provided 8-bit signed integer.
   */
  [[nodiscard]] ast::Expr *Const8(int8_t val) const;

  /**
   * @return A literal whose value is the provided 16-bit signed integer.
   */
  [[nodiscard]] ast::Expr *Const16(int16_t val) const;

  /**
   * @return A literal whose value is the provided 32-bit signed integer.
   */
  [[nodiscard]] ast::Expr *Const32(int32_t val) const;

  /**
   * @return A literal whose value is the provided 64-bit signed integer.
   */
  [[nodiscard]] ast::Expr *Const64(int64_t val) const;

  /**
   * @return A literal whose value is the provided 64-bit floating point.
   */
  [[nodiscard]] ast::Expr *ConstDouble(double val) const;

  /**
   * @return A literal whose value is identical to the provided string.
   */
  [[nodiscard]] ast::Expr *ConstString(std::string_view str) const;

  /**
   * @return A literal null whose type matches the provided type
   */
  [[nodiscard]] ast::Expr *ConstNull(type::TypeId type) const;

  // ---------------------------------------------------------------------------
  //
  // Type representations (not full TPL types !!)
  //
  // ---------------------------------------------------------------------------

  /**
   * @return The type representation for a primitive boolean value.
   */

  [[nodiscard]] ast::Expr *BoolType() const;

  /**
   * @return The type representation for an 8-bit signed integer (i.e., int8)
   */
  [[nodiscard]] ast::Expr *Int8Type() const;

  /**
   * @return The type representation for an 16-bit signed integer (i.e., int16)
   */
  [[nodiscard]] ast::Expr *Int16Type() const;

  /**
   * @return The type representation for an 32-bit signed integer (i.e., int32)
   */
  [[nodiscard]] ast::Expr *Int32Type() const;

  /**
   * @return The type representation for an 64-bit signed integer (i.e., int64)
   */
  [[nodiscard]] ast::Expr *Int64Type() const;

  /**
   * @return The type representation for an 32-bit floating point number (i.e., float32)
   */
  [[nodiscard]] ast::Expr *Float32Type() const;

  /**
   * @return The type representation for an 64-bit floating point number (i.e., float64)
   */
  [[nodiscard]] ast::Expr *Float64Type() const;

  /**
   * @return The type representation for the provided builtin type.
   */
  [[nodiscard]] ast::Expr *BuiltinType(ast::BuiltinType::Kind builtin_kind) const;

  /**
   * @return The type representation for a TPL nil type.
   */
  [[nodiscard]] ast::Expr *Nil() const;

  /**
   * @return A type representation expression that is a pointer to the provided type.
   */
  [[nodiscard]] ast::Expr *PointerType(ast::Expr *base_type_repr) const;

  /**
   * @return A type representation expression that is a pointer to a named object.
   */
  [[nodiscard]] ast::Expr *PointerType(ast::Identifier type_name) const;

  /**
   * @return A type representation expression that is a pointer to the provided builtin type.
   */
  [[nodiscard]] ast::Expr *PointerType(ast::BuiltinType::Kind builtin) const;

  /**
   * @return A type representation expression that is "[num_elems]kind".
   */
  ast::Expr *ArrayType(uint64_t num_elems, ast::BuiltinType::Kind kind);

  /** @return An expression representing "arr[idx]". */
  ast::Expr *ArrayAccess(ast::Identifier arr, uint64_t idx);

  /**
   * Convert a SQL type into a type representation expression.
   * @param type The SQL type.
   * @return The corresponding TPL type.
   */
  [[nodiscard]] ast::Expr *TplType(sql::TypeId type);

  /**
   * Return the appropriate aggregate type for the given input aggregation expression.
   * @param agg_type The aggregate expression type.
   * @param ret_type The return type of the aggregate.
   * @return The corresponding TPL aggregate type.
   */
  [[nodiscard]] ast::Expr *AggregateType(parser::ExpressionType agg_type, sql::TypeId ret_type) const;

  /**
   * @return An expression that represents the address of the provided object.
   */
  [[nodiscard]] ast::Expr *AddressOf(ast::Expr *obj) const;

  /**
   * @return An expression that represents the address of the provided object.
   */
  [[nodiscard]] ast::Expr *AddressOf(ast::Identifier obj_name) const;

  /**
   * @return An expression that represents the size of a type with the provided name, in bytes.
   */
  [[nodiscard]] ast::Expr *SizeOf(ast::Identifier type_name) const;

  /**
   * @return The offset of the given member within the given object.
   */
  [[nodiscard]] ast::Expr *OffsetOf(ast::Identifier obj, ast::Identifier member) const;

  /**
   * Perform a pointer case of the given expression into the provided type representation.
   * @param base The type to cast the expression into.
   * @param arg The expression to cast.
   * @return The result of the cast.
   */
  [[nodiscard]] ast::Expr *PtrCast(ast::Expr *base, ast::Expr *arg) const;

  /**
   * Perform a pointer case of the given argument into a pointer to the type with the provided
   * base name.
   * @param base_name The name of the base type.
   * @param arg The argument to the cast.
   * @return The result of the cast.
   */
  [[nodiscard]] ast::Expr *PtrCast(ast::Identifier base_name, ast::Expr *arg) const;

  // ---------------------------------------------------------------------------
  //
  // Declarations
  //
  // ---------------------------------------------------------------------------

  /**
   * Declare a variable with the provided name and type representation with no initial value.
   *
   * @param name The name of the variable to declare.
   * @param type_repr The provided type representation.
   * @return The variable declaration.
   */
  [[nodiscard]] ast::VariableDecl *DeclareVarNoInit(ast::Identifier name, ast::Expr *type_repr);

  /**
   * Declare a variable with the provided name and builtin kind, but with no initial value.
   *
   * @param name The name of the variable.
   * @param kind The builtin kind of the declared variable.
   * @return The variable declaration.
   */
  [[nodiscard]] ast::VariableDecl *DeclareVarNoInit(ast::Identifier name, ast::BuiltinType::Kind kind);

  /**
   * Declare a variable with the provided name and initial value. The variable's type will be
   * inferred from its initial value.
   *
   * @param name The name of the variable to declare.
   * @param init The initial value to assign the variable.
   * @return The variable declaration.
   */
  [[nodiscard]] ast::VariableDecl *DeclareVarWithInit(ast::Identifier name, ast::Expr *init);

  /**
   * Declare a variable with the provided name, type representation, and initial value.
   *
   * Note: No check is performed to ensure the provided type representation matches the type of the
   *       provided initial expression here. That check will be done during semantic analysis. Thus,
   *       it's possible for users to pass wrong information here and for the call to return without
   *       error.
   *
   * @param name The name of the variable to declare.
   * @param type_repr The provided type representation of the variable.
   * @param init The initial value to assign the variable.
   * @return The variable declaration.
   */
  [[nodiscard]] ast::VariableDecl *DeclareVar(ast::Identifier name, ast::Expr *type_repr, ast::Expr *init);

  /**
   * Declare a struct with the provided name and struct field elements.
   *
   * @param name The name of the structure.
   * @param fields The fields constituting the struct.
   * @return The structure declaration.
   */
  [[nodiscard]] ast::StructDecl *DeclareStruct(ast::Identifier name,
                                               util::RegionVector<ast::FieldDecl *> &&fields) const;

  // ---------------------------------------------------------------------------
  //
  // Assignments
  //
  // ---------------------------------------------------------------------------

  /**
   * Generate an assignment of the client-provide value to the provided destination.
   *
   * @param dest Where the value is stored.
   * @param value The value to assign.
   * @return The assignment statement.
   */
  [[nodiscard]] ast::Stmt *Assign(ast::Expr *dest, ast::Expr *value);

  // ---------------------------------------------------------------------------
  //
  // Binary and comparison operations
  //
  // ---------------------------------------------------------------------------

  /**
   * Generate a binary operation of the provided operation type (<b>op</b>) between the
   * provided left and right operands, returning its result.
   * @param op The binary operation.
   * @param left The left input.
   * @param right The right input.
   * @return The result of the binary operation.
   */
  [[nodiscard]] ast::Expr *BinaryOp(parsing::Token::Type op, ast::Expr *left, ast::Expr *right) const;

  /**
   * Generate a comparison operation of the provided type between the provided left and right
   * operands, returning its result.
   * @param op The binary operation.
   * @param left The left input.
   * @param right The right input.
   * @return The result of the comparison.
   */
  [[nodiscard]] ast::Expr *Compare(parsing::Token::Type op, ast::Expr *left, ast::Expr *right) const;

  /**
   * Generate a comparison of the given object pointer to 'nil', checking if it's a nil pointer.
   * @param obj The object to compare.
   * @return The boolean result of the comparison.
   */
  [[nodiscard]] ast::Expr *IsNilPointer(ast::Expr *obj) const;

  /**
   * Generate a unary operation of the provided operation type (<b>op</b>) on the provided input.
   * @param op The unary operation.
   * @param input The input.
   * @return The result of the unary operation.
   */
  [[nodiscard]] ast::Expr *UnaryOp(parsing::Token::Type op, ast::Expr *input) const;

  // ---------------------------------------------------------------------------
  //
  // Struct/Array access
  //
  // ---------------------------------------------------------------------------

  /**
   * Generate an access to a member within an object/struct.
   * @param object The object to index into.
   * @param member The name of the struct member to access.
   * @return An expression accessing the desired struct member.
   */
  [[nodiscard]] ast::Expr *AccessStructMember(ast::Expr *object, ast::Identifier member);

  /**
   * Create a return statement without a return value.
   * @return The statement.
   */
  [[nodiscard]] ast::Stmt *Return();

  /**
   * Create a return statement that returns the given value.
   * @param ret The return value.
   * @return The statement.
   */
  [[nodiscard]] ast::Stmt *Return(ast::Expr *ret);

  // ---------------------------------------------------------------------------
  //
  // Generic function calls and all builtins function calls.
  //
  // ---------------------------------------------------------------------------

  /**
   * Generate a call to the provided function by name and with the provided arguments.
   * @param func_name The name of the function to call.
   * @param args The arguments to pass in the call.
   */
  [[nodiscard]] ast::Expr *Call(ast::Identifier func_name, std::initializer_list<ast::Expr *> args) const;

  /**
   * Generate a call to the provided function by name and with the provided arguments.
   * @param func_name The name of the function to call.
   * @param args The arguments to pass in the call.
   */
  [[nodiscard]] ast::Expr *Call(ast::Identifier func_name, const std::vector<ast::Expr *> &args) const;

  /**
   * Generate a call to the provided builtin function and arguments.
   * @param builtin The builtin to call.
   * @param args The arguments to pass in the call.
   * @return The expression representing the call.
   */
  [[nodiscard]] ast::Expr *CallBuiltin(ast::Builtin builtin, std::initializer_list<ast::Expr *> args) const;

  /**
   * Generate a call to the provided function using the provided arguments. This function is almost
   * identical to the previous with the exception of the type of the arguments parameter. It's
   * an alternative API for callers that manually build up their arguments list.
   * @param builtin The builtin to call.
   * @param args The arguments to pass in the call.
   * @return The expression representing the call.
   */
  [[nodiscard]] ast::Expr *CallBuiltin(ast::Builtin builtin, const std::vector<ast::Expr *> &args) const;

  // ---------------------------------------------------------------------------
  //
  // Actual TPL builtins
  //
  // ---------------------------------------------------------------------------

  /**
   * Call \@boolToSql(). Convert a boolean into a SQL boolean.
   * @param b The constant bool.
   * @return The SQL bool.
   */
  [[nodiscard]] ast::Expr *BoolToSql(bool b) const;

  /**
   * Call \@intToSql(). Convert a 64-bit integer into a SQL integer.
   * @param num The number to convert.
   * @return The SQL integer.
   */
  [[nodiscard]] ast::Expr *IntToSql(int64_t num) const;

  /**
   * Call \@floatToSql(). Convert a 64-bit floating point number into a SQL real.
   * @param num The number to convert.
   * @return The SQL real.
   */
  [[nodiscard]] ast::Expr *FloatToSql(double num) const;

  /**
   * Call \@dateToSql(). Convert a date into a SQL date.
   * @param date The date.
   * @return The SQL date.
   */
  [[nodiscard]] ast::Expr *DateToSql(sql::Date date) const;

  /**
   * Call \@dateToSql(). Convert a date into a SQL date.
   * @param year The number to convert.
   * @param month The number to convert.
   * @param day The number to convert.
   * @return The SQL date.
   */
  [[nodiscard]] ast::Expr *DateToSql(int32_t year, int32_t month, int32_t day) const;

  /**
   * Call \@timestampToSql(). Create a timestamp value.
   * @param timestamp The timestamp
   * @return The generated sql Timestamp.
   */
  [[nodiscard]] ast::Expr *TimestampToSql(sql::Timestamp timestamp) const;

  /**
   * Call \@timestampToSql(). Create a timestamp value.
   * @param julian_usec The number of microseconds in Julian time.
   * @return The generated sql Timestamp.
   */
  [[nodiscard]] ast::Expr *TimestampToSql(uint64_t julian_usec) const;

  /**
   * Call \@timestampToSql(). Create a timestamp value.
   * @param year Years.
   * @param month Months.
   * @param day Days.
   * @param h Hours.
   * @param m Minutes.
   * @param s Seconds.
   * @param ms Milliseconds.
   * @param us Microseconds.
   * @return The SQL date.
   */
  [[nodiscard]] ast::Expr *TimestampToSql(int32_t year, int32_t month, int32_t day, int32_t h, int32_t m, int32_t s,
                                          int32_t ms, int32_t us) const;

  /**
   * Call \@stringToSql(). Convert a string literal into a SQL string.
   * @param str The string.
   * @return The SQL varlen.
   */
  [[nodiscard]] ast::Expr *StringToSql(std::string_view str) const;

  // -------------------------------------------------------
  //
  // Table stuff
  //
  // -------------------------------------------------------

  /**
   * @return The catalog accessor.
   */
  [[nodiscard]] catalog::CatalogAccessor *GetCatalogAccessor() const;

  /**
   * Call \@tableIterInit(). Initializes a TableVectorIterator instance with a table ID.
   * @param table_iter The table iterator variable.
   * @param exec_ctx The execution context that the TableVectorIterator runs with.
   * @param table_oid The OID of the table to scan.
   * @param col_oids The column OIDs from the table that should be scanned.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *TableIterInit(ast::Expr *table_iter, ast::Expr *exec_ctx, catalog::table_oid_t table_oid,
                                         ast::Identifier col_oids);

  /**
   * Call \@tableIterAdvance(). Attempt to advance the iterator, returning true if successful and
   * false otherwise.
   * @param table_iter The table vector iterator.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *TableIterAdvance(ast::Expr *table_iter);

  /**
   * Call \@tableIterGetVPI(). Retrieve the vector projection iterator from a table vector iterator.
   * @param table_iter The table vector iterator.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *TableIterGetVPI(ast::Expr *table_iter);

  /**
   * Call \@tableIterClose(). Close and destroy a table vector iterator.
   * @param table_iter The table vector iterator.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *TableIterClose(ast::Expr *table_iter);

  /**
   * Call \@iterateTableParallel(). Performs a parallel scan over the table with the provided name,
   * using the provided query state and thread-state container and calling the provided scan
   * function.
   * @param table_oid The OID of the table to be scanned.
   * @param col_oids The column OIDs from the table that should be scanned.
   * @param query_state The query state pointer.
   * @param exec_ctx The execution context that we are running in.
   * @param worker_name The work function name.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *IterateTableParallel(catalog::table_oid_t table_oid, ast::Identifier col_oids,
                                                ast::Expr *query_state, ast::Expr *exec_ctx,
                                                ast::Identifier worker_name);

  /**
   * Call \@abortTxn(exec_ctx).
   * @param exec_ctx The execution context that we are running in.
   * @return The call.
   */
  ast::Expr *AbortTxn(ast::Expr *exec_ctx);

  /**
   * Call \@indexIteratorInit(&iter, execCtx, table_oid, index_oid, col_oids)
   * @param iter The identifier of the index iterator.
   * @param exec_ctx_var The execution context variable.
   * @param num_attrs Number of attributes
   * @param table_oid The oid of the index's table.
   * @param index_oid The oid the index.
   * @param col_oids The identifier of the array of column oids to read.
   * @return The expression corresponding to the builtin call.
   */
  [[nodiscard]] ast::Expr *IndexIteratorInit(ast::Identifier iter, ast::Expr *exec_ctx_var, uint32_t num_attrs,
                                             uint32_t table_oid, uint32_t index_oid, ast::Identifier col_oids);

  /**
   * Call \@indexIteratorScanType(&iter[, limit])
   * @param iter The identifier of the index iterator.
   * @param scan_type The type of scan to perform.
   * @param limit The limit of the scan in case of limited scans.
   * @return The expression corresponding to the builtin call.
   */
  [[nodiscard]] ast::Expr *IndexIteratorScan(ast::Identifier iter, planner::IndexScanType scan_type, uint32_t limit);

  // -------------------------------------------------------
  //
  // VPI stuff
  //
  // -------------------------------------------------------

  /**
   * Call \@vpiIsFiltered(). Determines if the provided vector projection iterator is filtered.
   * @param vpi The vector projection iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *VPIIsFiltered(ast::Expr *vpi);

  /**
   * Call \@vpiHasNext() or \@vpiHasNextFiltered(). Check if the provided unfiltered (or filtered) VPI
   * has more tuple data to iterate over.
   * @param vpi The vector projection iterator.
   * @param filtered Flag indicating if the VPI is filtered.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *VPIHasNext(ast::Expr *vpi, bool filtered);

  /**
   * Call \@vpiAdvance() or \@vpiAdvanceFiltered(). Advance the provided unfiltered (or filtered) VPI
   * to the next valid tuple.
   * @param vpi The vector projection iterator.
   * @param filtered Flag indicating if the VPI is filtered.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *VPIAdvance(ast::Expr *vpi, bool filtered);

  /**
   * Call \@vpiInit(). Initialize a new VPI using the provided vector projection. The last TID list
   * argument is optional and can be NULL.
   * @param vpi The vector projection iterator.
   * @param vp The vector projection.
   * @param tids The TID list.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *VPIInit(ast::Expr *vpi, ast::Expr *vp, ast::Expr *tids);

  /**
   * Call \@vpiMatch(). Mark the current tuple the provided vector projection iterator is positioned
   * at as valid or invalid depending on the value of the provided condition.
   * @param vpi The vector projection iterator.
   * @param cond The boolean condition setting the current tuples filtration state.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *VPIMatch(ast::Expr *vpi, ast::Expr *cond);

  /**
   * Call \@vpiGet[Type][Nullable](). Reads a value from the provided vector projection iterator of
   * the given type and NULL-ability flag, and at the provided column index.
   * @param vpi The vector projection iterator.
   * @param type_id The SQL type of the column value to read.
   * @param nullable NULL-ability flag.
   * @param idx The index of the column in the VPI to read.
   */
  [[nodiscard]] ast::Expr *VPIGet(ast::Expr *vpi, sql::TypeId type_id, bool nullable, uint32_t idx);

  /**
   * Call \@filter[Comparison](). Invokes the vectorized filter on the provided vector projection
   * and column index, populating the results in the provided tuple ID list.
   * @param exec_ctx The execution context that we are running in.
   * @param vp The vector projection.
   * @param comp_type The comparison type.
   * @param col_idx The index of the column in the vector projection to apply the filter on.
   * @param filter_val The filtering value.
   * @param tids The TID list.
   */
  [[nodiscard]] ast::Expr *VPIFilter(ast::Expr *exec_ctx, ast::Expr *vp, parser::ExpressionType comp_type,
                                     uint32_t col_idx, ast::Expr *filter_val, ast::Expr *tids);
  /**
   * Call \@prGet(pr, attr_idx).
   * @param pr The projected row being accessed.
   * @param type The type of the column being accessed.
   * @param nullable Whether the column being accessed is nullable.
   * @param attr_idx Index of the column being accessed.
   * @return The expression corresponding to the builtin call.
   */
  [[nodiscard]] ast::Expr *PRGet(ast::Expr *pr, noisepage::type::TypeId type, bool nullable, uint32_t attr_idx);

  /**
   * Call \@prSet(pr, attr_idx, val, [own]).
   * @param pr The projected row being accessed.
   * @param type The type of the column being accessed.
   * @param nullable Whether the column being accessed is nullable.
   * @param attr_idx Index of the column being accessed.
   * @param val The value to set the column to.
   * @param own When inserting varchars, whether the VarlenEntry should own its content.
   * @return The expression corresponding to the builtin call.
   */
  [[nodiscard]] ast::Expr *PRSet(ast::Expr *pr, type::TypeId type, bool nullable, uint32_t attr_idx, ast::Expr *val,
                                 bool own = false);

  // -------------------------------------------------------
  //
  // Filter Manager stuff
  //
  // -------------------------------------------------------

  /**
   * Call \@filterManagerInit(). Initialize the provided filter manager instance.
   * @param filter_manager The filter manager pointer.
   * @param exec_ctx The execution context variable.
   */
  [[nodiscard]] ast::Expr *FilterManagerInit(ast::Expr *filter_manager, ast::Expr *exec_ctx);

  /**
   * Call \@filterManagerFree(). Destroy and clean up the provided filter manager instance.
   * @param filter_manager The filter manager pointer.
   */
  [[nodiscard]] ast::Expr *FilterManagerFree(ast::Expr *filter_manager);

  /**
   * Call \@filterManagerInsert(). Insert a list of clauses.
   * @param filter_manager The filter manager pointer.
   * @param clause_fn_names A vector containing the identifiers of the clauses.
   */
  [[nodiscard]] ast::Expr *FilterManagerInsert(ast::Expr *filter_manager,
                                               const std::vector<ast::Identifier> &clause_fn_names);

  /**
   * Call \@filterManagerRun(). Runs all filters on the input vector projection iterator.
   * @param filter_manager The filter manager pointer.
   * @param vpi The input vector projection iterator.
   * @param exec_ctx The execution context.
   */
  [[nodiscard]] ast::Expr *FilterManagerRunFilters(ast::Expr *filter_manager, ast::Expr *vpi, ast::Expr *exec_ctx);

  /**
   * Call \@execCtxRegisterHook(exec_ctx, hook_idx, hook).
   * @param exec_ctx The execution context to modify.
   * @param hook_idx Index to install hook at
   * @param hook Hook function to register
   * @return The call.
   */
  [[nodiscard]] ast::Expr *ExecCtxRegisterHook(ast::Expr *exec_ctx, uint32_t hook_idx, ast::Identifier hook);

  /**
   * Call \@execCtxClearHooks(exec_ctx).
   * @param exec_ctx The execution context to modify.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *ExecCtxClearHooks(ast::Expr *exec_ctx);

  /**
   * Call \@execCtxInitHooks(exec_ctx, num_hooks).
   * @param exec_ctx The execution context to modify.
   * @param num_hooks Number of hooks
   * @return The call.
   */
  [[nodiscard]] ast::Expr *ExecCtxInitHooks(ast::Expr *exec_ctx, uint32_t num_hooks);

  /**
   * Call \@execCtxAddRowsAffected(exec_ctx, num_rows_affected).
   * @param exec_ctx The execution context to modify.
   * @param num_rows_affected The amount to increment or decrement the number of rows affected.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *ExecCtxAddRowsAffected(ast::Expr *exec_ctx, int64_t num_rows_affected);

  /**
   * Call \@execCtxRecordFeature(exec_ctx, pipeline_id, feature_id, feature_attribute, value).
   * @param ouvec OU feature vector to update
   * @param pipeline_id The ID of the pipeline whose feature is to be recorded.
   * @param feature_id The ID of the feature to be recorded.
   * @param feature_attribute The attribute of the feature to record.
   * @param mode Update mode
   * @param value The value to be recorded.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *ExecOUFeatureVectorRecordFeature(
      ast::Expr *ouvec, pipeline_id_t pipeline_id, feature_id_t feature_id,
      brain::ExecutionOperatingUnitFeatureAttribute feature_attribute,
      brain::ExecutionOperatingUnitFeatureUpdateMode mode, ast::Expr *value);

  /**
   * Call \@execCtxGetMemPool(). Return the memory pool within an execution context.
   * @param exec_ctx The execution context variable.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *ExecCtxGetMemoryPool(ast::Expr *exec_ctx);

  /**
   * Call \@execCtxGetTLS(). Return the thread state container within an execution context.
   * @param exec_ctx The name of the execution context variable.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *ExecCtxGetTLS(ast::Expr *exec_ctx);

  /**
   * Call \@tlsGetCurrentThreadState(). Retrieves the current threads state in the state container.
   * It's assumed a previous call to \@tlsReset() was made to specify the state parameters.
   * @param tls The state container pointer.
   * @param state_type_name The name of the expected state type to cast the result to.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *TLSAccessCurrentThreadState(ast::Expr *tls, ast::Identifier state_type_name);

  /**
   * Call \@tlsIterate(). Invokes the provided callback function for all thread-local state objects.
   * The context is provided as the first argument, and the thread-local state as the second.
   * @param tls A pointer to the thread state container.
   * @param context An opaque context object.
   * @param func The callback function.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *TLSIterate(ast::Expr *tls, ast::Expr *context, ast::Identifier func);

  /**
   * Call \@tlsReset(). Reset the thread state container to a new state type with its own
   * initialization and tear-down functions.
   * @param tls The name of the thread state container variable.
   * @param tls_state_name The name of the thread state struct type.
   * @param init_fn The name of the initialization function.
   * @param tear_down_fn The name of the tear-down function.
   * @param context A context to pass along to each of the init and tear-down functions.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *TLSReset(ast::Expr *tls, ast::Identifier tls_state_name, ast::Identifier init_fn,
                                    ast::Identifier tear_down_fn, ast::Expr *context);

  /**
   * Call \@tlsClear(). Clears all thread-local states.
   * @param tls The name of the thread state container variable.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *TLSClear(ast::Expr *tls);

  // -------------------------------------------------------
  //
  // Hash
  //
  // -------------------------------------------------------

  /**
   * Invoke \@hash(). Hashes all input arguments and combines them into a single hash value.
   * @param values The values to hash.
   * @return The result of the hash.
   */
  [[nodiscard]] ast::Expr *Hash(const std::vector<ast::Expr *> &values);

  // -------------------------------------------------------
  //
  // Join stuff
  //
  // -------------------------------------------------------

  /**
   * Call \@joinHTInit(). Initialize the provided join hash table using a memory pool and storing
   * the build-row structures with the provided name.
   * @param join_hash_table The join hash table.
   * @param exec_ctx The execution context.
   * @param build_row_type_name The name of the materialized build-side row in the hash table.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *JoinHashTableInit(ast::Expr *join_hash_table, ast::Expr *exec_ctx,
                                             ast::Identifier build_row_type_name);

  /**
   * Call \@joinHTInsert(). Allocates a new tuple in the join hash table with the given hash value.
   * The returned value is a pointer to an element with the given type.
   * @param join_hash_table The join hash table.
   * @param hash_val The hash value of the tuple that's to be inserted.
   * @param tuple_type_name The name of the struct type representing the tuple to be inserted.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *JoinHashTableInsert(ast::Expr *join_hash_table, ast::Expr *hash_val,
                                               ast::Identifier tuple_type_name);

  /**
   * Call \@joinHTBuild(). Performs the hash table build step of a hash join. Called on the provided
   * join hash table expected to be a *JoinHashTable.
   * @param join_hash_table The pointer to the join hash table.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *JoinHashTableBuild(ast::Expr *join_hash_table);

  /**
   * Call \@joinHTBuildParallel(). Performs the parallel hash table build step of a hash join. Called
   * on the provided global join hash table (expected to be a *JoinHashTable), and a pointer to the
   * thread state container where thread-local join hash tables are stored at the given offset.
   * @param join_hash_table The global join hash table.
   * @param thread_state_container The thread state container.
   * @param offset The offset in the thread state container where thread-local tables are.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *JoinHashTableBuildParallel(ast::Expr *join_hash_table, ast::Expr *thread_state_container,
                                                      ast::Expr *offset);

  /**
   * Call \@joinHTLookup(). Performs a single lookup into the hash table with a tuple with the
   * provided hash value. The provided iterator will provide tuples in the hash table that match the
   * provided hash, but may not match on key (i.e., it may offer false positives). It is the
   * responsibility of the user to resolve such hash collisions.
   * @param join_hash_table The join hash table.
   * @param entry_iter An iterator over a list of entries.
   * @param hash_val The hash value of the probe key.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *JoinHashTableLookup(ast::Expr *join_hash_table, ast::Expr *entry_iter, ast::Expr *hash_val);

  /**
   * Call \@joinHTFree(). Cleanup and destroy the provided join hash table instance.
   * @param join_hash_table The join hash table.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *JoinHashTableFree(ast::Expr *join_hash_table);

  /**
   * Call \@htEntryIterHasNext(). Determine if the provided iterator has more entries. Entries
   * @param iter The iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *HTEntryIterHasNext(ast::Expr *iter);

  /**
   * Call \@htEntryIterGetRow(). Retrieves a pointer to the current row the iterator is positioned at
   * casted to the provided row type.
   * @param iter The iterator.
   * @param row_type The name of the struct type the row is expected to be.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *HTEntryIterGetRow(ast::Expr *iter, ast::Identifier row_type);

  /**
   * Call \@joinHTIterInit(). Initializes a join hash table iterator which iterates over every element
   * of the hash table.
   * @param iter A pointer to the iterator.
   * @param ht A pointer to the hash table to iterate over.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *JoinHTIteratorInit(ast::Expr *iter, ast::Expr *ht);

  /**
   * Call \@joinHTIterHasNext(). Determines if the given iterator has more data.
   * @param iter A pointer to the iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *JoinHTIteratorHasNext(ast::Expr *iter);

  /**
   * Call \@joinHTIterNext(). Advances the iterator by one element.
   * @param iter A pointer to the iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *JoinHTIteratorNext(ast::Expr *iter);

  /**
   * Call \@joinHTIterGetRow(). Returns a pointer to the payload the iterator is currently positioned at.
   * @param iter A pointer to the iterator.
   * @param payload_type The type of the payload
   * @return The call.
   */
  [[nodiscard]] ast::Expr *JoinHTIteratorGetRow(ast::Expr *iter, ast::Identifier payload_type);

  /**
   * Call \@joinHTIterFree(). Cleans up and destroys the given iterator.
   * @param iter A pointer to the iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *JoinHTIteratorFree(ast::Expr *iter);

  // -------------------------------------------------------
  //
  // Hash aggregation
  //
  // -------------------------------------------------------

  /**
   * Call \@aggHTInit(). Initializes an aggregation hash table.
   * @param agg_ht A pointer to the aggregation hash table.
   * @param exec_ctx The execution context.
   * @param agg_payload_type The name of the struct representing the aggregation payload.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableInit(ast::Expr *agg_ht, ast::Expr *exec_ctx, ast::Identifier agg_payload_type);

  /**
   * Call \@aggHTLookup(). Performs a single key lookup in an aggregation hash table. The hash value
   * is provided, as is a key check function to resolve hash collisions. The result of the lookup
   * is casted to the provided type.
   * @param agg_ht A pointer to the aggregation hash table.
   * @param hash_val The hash value of the key.
   * @param key_check The function to perform key-equality check.
   * @param input The probe aggregate values.
   * @param agg_payload_type The name of the struct representing the aggregation payload.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableLookup(ast::Expr *agg_ht, ast::Expr *hash_val, ast::Identifier key_check,
                                              ast::Expr *input, ast::Identifier agg_payload_type);

  /**
   * Call \@aggHTInsert(). Inserts a new entry into the aggregation hash table. The result of the
   * insertion is casted to the provided type.
   * @param agg_ht A pointer to the aggregation hash table.
   * @param hash_val The hash value of the key.
   * @param partitioned Whether the insertion is in partitioned mode.
   * @param agg_payload_type The name of the struct representing the aggregation payload.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableInsert(ast::Expr *agg_ht, ast::Expr *hash_val, bool partitioned,
                                              ast::Identifier agg_payload_type);

  /**
   * Call \@aggHTLink(). Directly inserts a new partial aggregate into the provided aggregation hash table.
   * @param agg_ht A pointer to the aggregation hash table.
   * @param entry A pointer to the hash table entry storing the partial aggregate data.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableLinkEntry(ast::Expr *agg_ht, ast::Expr *entry);

  /**
   * Call \@aggHTMoveParts(). Move all overflow partitions stored in thread-local aggregation hash
   * tables at the given offset inside the provided thread state container into the global hash
   * table.
   * @param agg_ht A pointer to the global aggregation hash table.
   * @param tls A pointer to the thread state container.
   * @param tl_agg_ht_offset The offset in the state container where the thread-local aggregation
   *                         hash tables.
   * @param merge_partitions_fn_name The name of the merging function to merge partial aggregates
   *                                 into the global hash table.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableMovePartitions(ast::Expr *agg_ht, ast::Expr *tls, ast::Expr *tl_agg_ht_offset,
                                                      ast::Identifier merge_partitions_fn_name);

  /**
   * Call \@aggHTParallelPartScan(). Performs a parallel partitioned scan over an aggregation hash
   * table, using the provided worker function as a callback.
   * @param agg_ht A pointer to the global hash table.
   * @param query_state A pointer to the query state.
   * @param thread_state_container A pointer to the thread state.
   * @param worker_fn The name of the function used to scan over a partition of the hash table.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableParallelScan(ast::Expr *agg_ht, ast::Expr *query_state,
                                                    ast::Expr *thread_state_container, ast::Identifier worker_fn);

  /**
   * Call \@aggHTFree(). Cleans up and destroys the provided aggregation hash table.
   * @param agg_ht A pointer to the aggregation hash table.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableFree(ast::Expr *agg_ht);

  /**
   * Call \@aggHTPartIterHasNext(). Determines if the provided overflow partition iterator has more
   * data.
   * @param iter A pointer to the overflow partition iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggPartitionIteratorHasNext(ast::Expr *iter);

  /**
   * Call \@aggHTPartIterNext(). Advanced the iterator by one element.
   * @param iter A pointer to the overflow partition iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggPartitionIteratorNext(ast::Expr *iter);

  /**
   * Call \@aggHTPartIterGetHash(). Returns the hash value of the entry the iterator is currently
   * positioned at.
   * @param iter A pointer to the overflow partition iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggPartitionIteratorGetHash(ast::Expr *iter);

  /**
   * Call \@aggHTPartIterGetRow(). Returns a pointer to the aggregate payload struct of the entry the
   * iterator is currently positioned at.
   * @param iter A pointer to the overflow partition iterator.
   * @param agg_payload_type The type of aggregate payload.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggPartitionIteratorGetRow(ast::Expr *iter, ast::Identifier agg_payload_type);

  /**
   * Call \@aggHTPartIterGetRowEntry(). Returns a pointer to the current hash table entry.
   * @param iter A pointer to the overflow partition iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggPartitionIteratorGetRowEntry(ast::Expr *iter);

  /**
   * Call \@aggHTIterInit(). Initializes an aggregation hash table iterator.
   * @param iter A pointer to the iterator.
   * @param agg_ht A pointer to the hash table to iterate over.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableIteratorInit(ast::Expr *iter, ast::Expr *agg_ht);

  /**
   * Call \@aggHTIterHasNExt(). Determines if the given iterator has more data.
   * @param iter A pointer to the iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableIteratorHasNext(ast::Expr *iter);

  /**
   * Call \@aggHTIterNext(). Advances the iterator by one element.
   * @param iter A pointer to the iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableIteratorNext(ast::Expr *iter);

  /**
   * Call \@aggHTIterGetRow(). Returns a pointer to the aggregate payload the iterator is currently positioned at.
   * @param iter A pointer to the iterator.
   * @param agg_payload_type The name of the aggregate payload's type.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableIteratorGetRow(ast::Expr *iter, ast::Identifier agg_payload_type);

  /**
   * Call \@aggHTIterClose(). Cleans up and destroys the given iterator.
   * @param iter A pointer to the iterator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggHashTableIteratorClose(ast::Expr *iter);

  /**
   * Call \@aggInit(). Initializes and aggregator.
   * @param agg A pointer to the aggregator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggregatorInit(ast::Expr *agg);

  /**
   * Call \@aggAdvance(). Advanced an aggregator with the provided input value.
   * @param agg A pointer to the aggregator.
   * @param val The input value.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggregatorAdvance(ast::Expr *agg, ast::Expr *val);

  /**
   * Call \@aggMerge(). Merges two aggregators storing the result in the first argument.
   * @param agg1 A pointer to the aggregator.
   * @param agg2 A pointer to the aggregator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggregatorMerge(ast::Expr *agg1, ast::Expr *agg2);

  /**
   * Call \@aggResult(). Finalizes and returns the result of the aggregation.
   * @param agg A pointer to the aggregator.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *AggregatorResult(ast::Expr *agg);

  // -------------------------------------------------------
  //
  // Sorter stuff
  //
  // -------------------------------------------------------

  /**
   * Call \@sorterInit(). Initialize the provided sorter instance using a memory pool, comparison
   * function and the struct that will be materialized into the sorter instance.
   * @param sorter The sorter instance.
   * @param exec_ctx The execution context that we are running in.
   * @param cmp_func_name The name of the comparison function to use.
   * @param sort_row_type_name The name of the materialized sort-row type.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SorterInit(ast::Expr *sorter, ast::Expr *exec_ctx, ast::Identifier cmp_func_name,
                                      ast::Identifier sort_row_type_name);

  /**
   * Call \@sorterInsert(). Prepare an insert into the provided sorter whose type is the given type.
   * @param sorter The sorter instance.
   * @param sort_row_type_name The name of the TPL type that will be stored in the sorter.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SorterInsert(ast::Expr *sorter, ast::Identifier sort_row_type_name);

  /**
   * Call \@sorterInsertTopK(). Prepare a top-k insert into the provided sorter whose type is the
   * given type.
   * @param sorter The sorter instance.
   * @param sort_row_type_name The name of the TPL type that will be stored in the sorter.
   * @param top_k The top-k value.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SorterInsertTopK(ast::Expr *sorter, ast::Identifier sort_row_type_name, uint64_t top_k);

  /**
   * Call \@sorterInsertTopK(). Complete a previous top-k insert into the provided sorter.
   * @param sorter The sorter instance.
   * @param top_k The top-k value.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SorterInsertTopKFinish(ast::Expr *sorter, uint64_t top_k);

  /**
   * Call \@sorterSort().  Sort the provided sorter instance.
   * @param sorter The sorter instance.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SorterSort(ast::Expr *sorter);

  /**
   * Call \@sorterSortParallel(). Perform a parallel sort of all sorter instances contained in the
   * provided thread-state  container at the given offset, merging sorter results into a central
   * sorter instance.
   * @param sorter The central sorter instance that will contain the results of the sort.
   * @param tls The thread state container.
   * @param offset The offset within the container where the thread-local sorter is.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SortParallel(ast::Expr *sorter, ast::Expr *tls, ast::Expr *offset);

  /**
   * Call \@sorterSortTopKParallel(). Perform a parallel top-k sort of all sorter instances contained
   * in the provided thread-local container at the given offset.
   * @param sorter The central sorter instance that will contain the results of the sort.
   * @param tls The thread-state container.
   * @param offset The offset within the container where the thread-local sorters are.
   * @param top_k The top-K value.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SortTopKParallel(ast::Expr *sorter, ast::Expr *tls, ast::Expr *offset, std::size_t top_k);

  /**
   * Call \@sorterFree(). Destroy the provided sorter instance.
   * @param sorter The sorter instance.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *SorterFree(ast::Expr *sorter);

  /**
   * Call \@sorterIterInit(). Initialize the provided sorter iterator over the given sorter.
   * @param iter The sorter iterator.
   * @param sorter The sorter instance to iterate.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *SorterIterInit(ast::Expr *iter, ast::Expr *sorter);

  /**
   * Call \@sorterIterHasNext(). Check if the sorter iterator has more data.
   * @param iter The iterator.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *SorterIterHasNext(ast::Expr *iter);

  /**
   * Call \@sorterIterNext(). Advances the sorter iterator one tuple.
   * @param iter The iterator.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *SorterIterNext(ast::Expr *iter);

  /**
   * Call \@sorterIterSkipRows(). Skips N rows in the provided sorter iterator.
   * @param iter The iterator.
   * @param n The number of rows to skip.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *SorterIterSkipRows(ast::Expr *iter, uint32_t n);

  /**
   * Call \@sorterIterGetRow(). Retrieves a pointer to the current iterator row casted to the
   * provided row type.
   * @param iter The iterator.
   * @param row_type_name The name of the TPL type that will be stored in the sorter.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *SorterIterGetRow(ast::Expr *iter, ast::Identifier row_type_name);

  /**
   * Call \@sorterIterClose(). Destroy and cleanup the provided sorter iterator instance.
   * @param iter The sorter iterator.
   * @return The call expression.
   */
  [[nodiscard]] ast::Expr *SorterIterClose(ast::Expr *iter);

  /**
   * Call \@like(). Implements the SQL LIKE() operation.
   * @param str The input string.
   * @param pattern The input pattern.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *Like(ast::Expr *str, ast::Expr *pattern);

  /**
   * Invoke !\@like(). Implements the SQL NOT LIKE() operation.
   * @param str The input string.
   * @param pattern The input pattern.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *NotLike(ast::Expr *str, ast::Expr *pattern);

  /**
   * Call \@csvReaderInit(). Initialize a CSV reader for the given file name.
   * @param reader The reader.
   * @param file_name The filename.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *CSVReaderInit(ast::Expr *reader, std::string_view file_name);

  /**
   * Call \@csvReaderAdvance(). Advance the reader one row.
   * @param reader The reader.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *CSVReaderAdvance(ast::Expr *reader);

  /**
   * Call \@csvReaderGetField(). Read the field at the given index in the current row.
   * @param reader The reader.
   * @param field_index The index of the field in the row to read.
   * @param result Where the result is written to.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *CSVReaderGetField(ast::Expr *reader, uint32_t field_index, ast::Expr *result);

  /**
   * Call \@csvReaderGetRecordNumber(). Get the current record number.
   * @param reader The reader.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *CSVReaderGetRecordNumber(ast::Expr *reader);

  /**
   * Call \@csvReaderClose(). Destroy a CSV reader.
   * @param reader The reader.
   * @return The call.
   */
  [[nodiscard]] ast::Expr *CSVReaderClose(ast::Expr *reader);

  /**
   * Call storageInterfaceInit(&storage_interface, execCtx, table_oid, col_oids, need_indexes)
   * @param si The storage interface to initialize
   * @param exec_ctx The execution context that we are running in.
   * @param table_oid The oid of the table being accessed.
   * @param col_oids The identifier of the array of column oids to access.
   * @param need_indexes Whether the storage interface will need to use indexes
   * @return The expression corresponding to the builtin call.
   */
  ast::Expr *StorageInterfaceInit(ast::Identifier si, ast::Expr *exec_ctx, uint32_t table_oid, ast::Identifier col_oids,
                                  bool need_indexes);

  // ---------------------------------------------------------------------------
  //
  // Identifiers
  //
  // ---------------------------------------------------------------------------

  /**
   * @return A new unique identifier using the given string as a prefix.
   */
  ast::Identifier MakeFreshIdentifier(const std::string &str);

  /**
   * @return An identifier whose contents are identical to the input string.
   */
  ast::Identifier MakeIdentifier(std::string_view str) const;

  /**
   * @return A new identifier expression representing the given identifier.
   */
  ast::IdentifierExpr *MakeExpr(ast::Identifier ident) const;

  /**
   * @return The expression as a standalone statement.
   */
  ast::Stmt *MakeStmt(ast::Expr *expr) const;

  /**
   * @return An empty list of statements.
   */
  ast::BlockStmt *MakeEmptyBlock() const;

  /**
   * @return An empty list of fields.
   */
  util::RegionVector<ast::FieldDecl *> MakeEmptyFieldList() const;

  /**
   * @return A field list with the given fields.
   */
  util::RegionVector<ast::FieldDecl *> MakeFieldList(std::initializer_list<ast::FieldDecl *> fields) const;

  /**
   * Create a single field.
   * @param name The name of the field.
   * @param type The type representation of the field.
   * @return The field declaration.
   */
  ast::FieldDecl *MakeField(ast::Identifier name, ast::Expr *type) const;

  /**
   * @return The current source code position.
   */
  const SourcePosition &GetPosition() const { return position_; }

  /**
   * Move to a new line.
   */
  void NewLine() { position_.line_++; }

  /**
   * Increase current indentation level.
   */
  void Indent() { position_.column_ += 4; }

  /**
   * Decrease Remove current indentation level.
   */
  void UnIndent() { position_.column_ -= 4; }

  // ---------------------------------------------------------------------------
  //
  // Minirunner support.
  //
  // ---------------------------------------------------------------------------

  /** @return PipelineOperatingUnits instance. */
  common::ManagedPointer<brain::PipelineOperatingUnits> GetPipelineOperatingUnits() const {
    return common::ManagedPointer(pipeline_operating_units_);
  }

  /** @return The AST context used for compilation. */
  common::ManagedPointer<ast::Context> GetAstContext() const { return common::ManagedPointer(context_); }

  /** @return Release ownership of the PipelineOperatingUnits instance. */
  std::unique_ptr<brain::PipelineOperatingUnits> ReleasePipelineOperatingUnits() {
    return std::move(pipeline_operating_units_);
  }

 private:
  // Enter a new lexical scope.
  void EnterScope();

  // Exit the current lexical scope.
  void ExitScope();

  // Return the AST node factory.
  ast::AstNodeFactory *GetFactory();

 private:
  // The context used to create AST nodes.
  ast::Context *context_;
  // The current position in the source.
  SourcePosition position_;
  // Cache of code scopes.
  uint32_t num_cached_scopes_;
  std::array<std::unique_ptr<Scope>, DEFAULT_SCOPE_CACHE_SIZE> scope_cache_ = {nullptr};
  // Current scope.
  Scope *scope_;
  // The catalog accessor.
  catalog::CatalogAccessor *accessor_;
  // Minirunner-related.
  std::unique_ptr<brain::PipelineOperatingUnits> pipeline_operating_units_;
};

}  // namespace noisepage::execution::compiler
