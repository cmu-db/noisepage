#pragma once
#include <string>
#include <unordered_map>
#include "planner/plannodes/abstract_plan_node.h"
#include "parser/expression/tuple_value_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "execution/ast/ast.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler_defs.h"
#include "planner/plannodes/aggregate_plan_node.h"

namespace tpl::compiler {

/**
 * Each operator needs three things:
 * 1. A list of functions (e.g. a key checker for aggregates)
 * 2. A list of structs (e.g. aggregation payload struct)
 * 3. A list of query state fields (e.g. Aggregation hash table)
 * This class keeps track of these things.
 * Each operator will subclass this one to decide how generate code for each of these.
 * See sample_tpl/tpch/q1.tpl to see how these fields are used together.
 */
class OperatorState {
 public:
  /**
   * TODO: I probably need to pass in the children operator states.
   * @param op
   */
  explicit OperatorState(const terrier::planner::AbstractPlanNode *op) : op_(op) {}

  ast::Identifier GetIdentifier(const std::string & field_name) {
    return identifiers.at(field_name);
  }

  ast::Identifier GetIdentifier(terrier::parser::TupleValueExpression * expr) {
    return GetIdentifier(expr->GetTableName() + "_" + expr->GetColumnName());
  }

  virtual void GenStructs(CodeGen * codegen, std::vector<ast::Decl*> *decls) {}
  virtual void GenSetup(CodeGen * codegen, std::vector<ast::Stmt*> *stmt) {}
  virtual void GenFunctions(CodeGen * codegen, std::vector<ast::Decl*> *decls, OperatorState ** curr_state) {}
  virtual void GenState(CodeGen * codegen, std::vector<ast::FieldDecl*>* fields) {}
  virtual ast::Identifier GetOutputStruct() {
    return ast::Identifier(nullptr);
  }

  void Initialize(CodeGen* codegen, std::vector<ast::Decl*> *decls, std::vector<ast::Stmt*> *stmts, std::vector<ast::FieldDecl*>* fields, OperatorState ** curr_state) {
    GenStructs(codegen, decls);
    GenSetup(codegen, stmts);
    GenFunctions(codegen, decls);
    GenState(codegen, fields);
  }

 protected:
  void GenOutputStruct(CodeGen * codegen, std::vector<ast::Decl*> *decls, const std::string & name) {
    // Generate TableRow struct
    auto out_schema = op_->GetOutputSchema();
    // First generate the struct members from the output schema
    util::RegionVector<ast::FieldDecl *> members(codegen->GetRegion());
    members.reserve(out_schema->GetColumns().size());
    for (const auto & col: out_schema->GetColumns()) {
      ast::Identifier field_name = codegen->NewIdentifer(col.GetName());
      ast::Expr* type = codegen->TyConvert();
      ast::FieldDecl * decl = codegen->GetNodeFactory()->NewFieldDecl(DUMMY_POS, field_name, type);
      members.push_back(decl);
      identifiers.insert({col.GetName(), field_name});
    }
    // Now create the declaration
    ast::Identifier struct_name = codegen->NewIdentifier(name);
    identifiers.insert({name, struct_name});
    ast::StructTypeRepr * struct_type = codegen->GetNodeFactory()->NewStructType(DUMMY_POS, std::move(members));
    decls->push_back(codegen->GetNodeFactory()->NewStructDecl(DUMMY_POS, struct_name, struct_type));
  }
  std::unordered_map<std::string, ast::Identifier> identifiers{};
  std::unordered_map<std::string, ast::Expr*> types{};
  const terrier::planner::AbstractPlanNode *op_;
};

/**
 * The operator state for a sequential scan
 */
class SeqScanState : public OperatorState {
 public:
  explicit SeqScanState(const terrier::planner::AbstractPlanNode * op) : OperatorState(op) {
  }

  void GenStructs(CodeGen * codegen, std::vector<ast::Decl*> *decls) override {
    // Generate TableRow struct
    GenOutputStruct(codegen, decls, table_row);
  }

  ast::Identifier GetOutputStruct() override {
    return GetIdentifier(table_row);
  }

  static constexpr const char* table_row = "TableRow";
};

/**
 * The operator state for an aggregate
 */
class AggregateState : public OperatorState {
 public:
  explicit AggregateState(const terrier::planner::AbstractPlanNode *op) : OperatorState(op) {
  }

  void GenStructs(CodeGen * codegen, std::vector<ast::Decl*> *decls) override {
    GenPayloadStruct(codegen, decls);
    GenOutputStruct(codegen, decls);
  }

  void GenState(CodeGen * codegen, std::vector<ast::FieldDecl*>* fields) override {
    ast::Identifier table_ident = codegen->NewIdentifier(hash_table_field);
    ast::Expr * table_type = codegen->TyAggHashTable();
    ast::FieldDecl * field = codegen->GetNodeFactory()->NewFieldDecl(DUMMY_POS, table_ident, table_type);

  }

  ast::Identifier GetOutputStruct() override {
    return GetIdentifier(agg_row);
  }

  void GenFunctions(CodeGen * codegen, std::vector<ast::Decl*> *decls, OperatorState ** curr_state) override {
    // Generate the key check functions
    TERRIER_ASSERT(*curr_state != nullptr, "Aggregate cannot be a leaf operator");
    // Generate the function type (*Payload, *curr_state.output_row) -> bool
    ast::Identifier payload_row = GetIdentifier("payload_row");
    ast::Identifier input_row = GetIdentifier("input_row");
    util::RegionVector<ast::FieldDecl *> params(codegen->GetRegion());
    ast::IdentifierExpr * payload_type = codegen->GetNodeFactory()->NewIdentifierExpr(DUMMY_POS, GetIdentifier(agg_payload));
    ast::IdentifierExpr * input_type = codegen->GetNodeFactory()->NewIdentifierExpr(DUMMY_POS, (*curr_state)->GetOutputStruct());
    params.emplace_back(codegen->GetNodeFactory()->NewFieldDecl(DUMMY_POS, payload_row, payload_type));
    params.emplace_back(codegen->GetNodeFactory()->NewFieldDecl(DUMMY_POS, input_row, input_type));
    ast::FunctionTypeRepr * func_type = codegen->GetNodeFactory()->NewFunctionType(DUMMY_POS, params, codegen->TyBool());
    // Generate the function body
    auto agg_op = dynamic_cast<const terrier::planner::AggregatePlanNode*>(op_);
    auto group_by_terms = agg_op->GetGroupByTerms();
    util::RegionVector<ast::Stmt *> statements(codegen->GetRegion());
    for (const auto & term: group_by_terms) {
      // Generate payload_row.term
      ast::Expr * payload_expr = codegen->GetNodeFactory()->NewIdentifierExpr(DUMMY_POS, payload_row);
      ast::Expr * payload_member = codegen->GetNodeFactory()->NewIdentifierExpr(DUMMY_POS, this->GetIdentifier(term.get()));
      ast::MemberExpr * payload = codegen->GetNodeFactory()->NewMemberExpr(payload_expr, payload_member);
      // Generate input_row.term
      ast::Expr * input_expr = codegen->GetNodeFactory()->NewIdentifierExpr(DUMMY_POS, input_row);
      ast::Expr * input_member = codegen->GetNodeFactory()->NewIdentifierExpr(DUMMY_POS, (*curr_state)->GetIdentifier(term.get()));
      ast::MemberExpr * input = codegen->GetNodeFactory()->NewMemberExpr(input_expr, input_member);
      // Generate payload_row.term = input_row.term
      ast::AssignmentStmt * assigment = codegen->GetNodeFactory()->NewAssignmentStmt(DUMMY_POS, payload, input);
      statements.emplace_back(assigment);
    }
    // Generate the full function
    ast::BlockStmt * block = codegen->GetNodeFactory()->NewBlockStmt(DUMMY_POS, DUMMY_POS, std::move(statements));
    ast::FunctionLitExpr * func_lit = codegen->GetNodeFactory()->NewFunctionLitExpr(func_type, block);
    ast::Identifier func_name = codegen->NewIdentifer(keycheck_fn);
    identifiers.insert({keycheck_fn, func_name});
    ast::FunctionDecl * func_decl = codegen->GetNodeFactory()->NewFunctionDecl(DUMMY_POS, func_name, func_lit);
    decls->emplace_back(func_decl);
  }

 private:
  void GenPayloadStruct(CodeGen * codegen, std::vector<ast::Decl*> *decls) {
    auto agg_op = dynamic_cast<const terrier::planner::AggregatePlanNode*>(op_);
    TERRIER_ASSERT(agg_op != nullptr, "AggregateState received a non aggregate node");
    util::RegionVector<ast::FieldDecl *> members(codegen->GetRegion());
    // First add the group by members
    auto group_by_terms = agg_op->GetGroupByTerms();
    for (const auto & term: group_by_terms) {
      std::string expr_name = codegen->ExpressionName(term);
      ast::Identifier field_name = codegen->NewIdentifer(expr_name);
      ast::Expr * type = codegen->ExpressionType(term);
      ast::FieldDecl * decl = codegen->GetNodeFactory()->NewFieldDecl(DUMMY_POS, field_name, type);
      members.push_back(decl);
      identifiers.insert({expr_name, field_name});
    }
    // Then add the aggregate members
    auto agg_terms = agg_op->GetAggregateTerms();
    for (const auto & term: agg_terms) {
      std::string expr_name = codegen->ExpressionName(term);
      ast::Identifier field_name = codegen->NewIdentifer(expr_name);
      ast::Expr * type = codegen->AggregateType(term);
      ast::FieldDecl * decl = codegen->GetNodeFactory()->NewFieldDecl(DUMMY_POS, field_name, type);
      members.push_back(decl);
      identifiers.insert({expr_name, field_name});
    }
    // TODO: Add support for HAVING depending on the plan node structure

    // Finish by creating the struct
    ast::Identifier struct_name = codegen->NewIdentifier(agg_payload);
    identifiers.insert({agg_payload, struct_name});
    ast::StructTypeRepr * struct_type = codegen->GetNodeFactory()->NewStructType(DUMMY_POS, std::move(members));
    decls->push_back(codegen->GetNodeFactory()->NewStructDecl(DUMMY_POS, struct_name, struct_type));
  }

  static constexpr const char* keycheck_fn = "aggKeyCheck";
  static constexpr const char* agg_payload = "AggPayload";
  static constexpr const char* agg_row = "AggRow";
  static constexpr const char* hash_table_field = "agg_table";
};


class HashJoinState : public OperatorState {
  explicit HashJoinState(const terrier::planner::AbstractPlanNode &op) : OperatorState(op) {}

  void GenStructs(CodeGen * codegen, std::vector<ast::Decl*> *decls) override {
    GenOutputStruct(codegen, decls, join_row);
    GenBuildRow(codegen, decls);

  }

    void Initialize(std::vector<ast::Decl> *decls, std::vector<ast::Stmt> *stmts) override {
    /*
     * I. Generate Probe Row Struct.
     */

    /*
     * II. Generate Field Row Struct.
     */

    /*
     * III. Generate Check Key Function.
     * TODO: How to get ids of fields
     */

    /*
     * IV. Generate setup statements
     */
  }

  static constexpr const char* keycheck_fn = "keyCheck";
  static constexpr const char* build_row = "BuildRow";
  static constexpr const char* join_row = "BuildRow";
  static constexpr const char* hash_table_field = "join_table";
};

}
