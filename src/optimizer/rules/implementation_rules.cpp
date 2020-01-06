#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "loggers/optimizer_logger.h"
#include "optimizer/group_expression.h"
#include "optimizer/index_util.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/physical_operators.h"
#include "optimizer/properties.h"
#include "optimizer/rules/implementation_rules.h"
#include "optimizer/util.h"
#include "parser/expression_util.h"
#include "type/transient_value_factory.h"

namespace terrier::optimizer {

///////////////////////////////////////////////////////////////////////////////
/// LogicalGetToPhysicalTableFreeScan
///////////////////////////////////////////////////////////////////////////////
LogicalGetToPhysicalTableFreeScan::LogicalGetToPhysicalTableFreeScan() {
  type_ = RuleType::GET_TO_DUMMY_SCAN;
  match_pattern_ = new Pattern(OpType::LOGICALGET);
}

bool LogicalGetToPhysicalTableFreeScan::Check(common::ManagedPointer<OperatorExpression> plan,
                                              OptimizationContext *context) const {
  (void)context;
  const auto get = plan->GetOp().As<LogicalGet>();
  return get->GetTableOid() == catalog::INVALID_TABLE_OID;
}

void LogicalGetToPhysicalTableFreeScan::Transform(UNUSED_ATTRIBUTE common::ManagedPointer<OperatorExpression> input,
                                                  std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                                  UNUSED_ATTRIBUTE OptimizationContext *context) const {
  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto result_plan = std::make_unique<OperatorExpression>(TableFreeScan::Make(), std::move(c));
  transformed->emplace_back(std::move(result_plan));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalGetToPhysicalSeqScan
///////////////////////////////////////////////////////////////////////////////
LogicalGetToPhysicalSeqScan::LogicalGetToPhysicalSeqScan() {
  type_ = RuleType::GET_TO_SEQ_SCAN;
  match_pattern_ = new Pattern(OpType::LOGICALGET);
}

bool LogicalGetToPhysicalSeqScan::Check(common::ManagedPointer<OperatorExpression> plan,
                                        OptimizationContext *context) const {
  (void)context;
  const auto get = plan->GetOp().As<LogicalGet>();
  return get->GetTableOid() != catalog::INVALID_TABLE_OID;
}

void LogicalGetToPhysicalSeqScan::Transform(common::ManagedPointer<OperatorExpression> input,
                                            std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                            UNUSED_ATTRIBUTE OptimizationContext *context) const {
  TERRIER_ASSERT(input->GetChildren().empty(), "Get should have no children");
  const auto get = input->GetOp().As<LogicalGet>();

  // Need to copy because SeqScan uses std::move
  auto db_oid = get->GetDatabaseOid();
  auto ns_oid = get->GetNamespaceOid();
  auto tbl_oid = get->GetTableOid();
  auto tbl_alias = std::string(get->GetTableAlias());
  auto preds = std::vector<AnnotatedExpression>(get->GetPredicates());
  auto is_update = get->GetIsForUpdate();
  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto result_plan = std::make_unique<OperatorExpression>(
      SeqScan::Make(db_oid, ns_oid, tbl_oid, std::move(preds), tbl_alias, is_update), std::move(c));
  transformed->emplace_back(std::move(result_plan));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalGetToPhysicalIndexScan
///////////////////////////////////////////////////////////////////////////////
LogicalGetToPhysicalIndexScan::LogicalGetToPhysicalIndexScan() {
  type_ = RuleType::GET_TO_INDEX_SCAN;
  match_pattern_ = new Pattern(OpType::LOGICALGET);
}

bool LogicalGetToPhysicalIndexScan::Check(common::ManagedPointer<OperatorExpression> plan,
                                          OptimizationContext *context) const {
  // If there is a index for the table, return true,
  // else return false
  (void)context;
  const auto get = plan->GetOp().As<LogicalGet>();
  if (get == nullptr) {
    return false;
  }

  if (get->GetTableOid() == catalog::INVALID_TABLE_OID) {
    return false;
  }

  auto *accessor = context->GetOptimizerContext()->GetCatalogAccessor();
  return !accessor->GetIndexOids(get->GetTableOid()).empty();
}

void LogicalGetToPhysicalIndexScan::Transform(common::ManagedPointer<OperatorExpression> input,
                                              std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                              UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto get = input->GetOp().As<LogicalGet>();
  TERRIER_ASSERT(input->GetChildren().empty(), "Get should have no children");

  auto db_oid = get->GetDatabaseOid();
  auto ns_oid = get->GetNamespaceOid();
  bool is_update = get->GetIsForUpdate();
  auto *accessor = context->GetOptimizerContext()->GetCatalogAccessor();

  auto sort = context->GetRequiredProperties()->GetPropertyOfType(PropertyType::SORT);
  std::vector<catalog::col_oid_t> sort_col_ids;
  if (sort != nullptr) {
    // Check if can satisfy sort property with an index
    auto sort_prop = sort->As<PropertySort>();
    if (IndexUtil::CheckSortProperty(sort_prop)) {
      auto indexes = accessor->GetIndexOids(get->GetTableOid());
      for (auto index : indexes) {
        if (IndexUtil::SatisfiesSortWithIndex(accessor, sort_prop, get->GetTableOid(), index)) {
          std::vector<AnnotatedExpression> preds = get->GetPredicates();
          std::string tbl_alias = std::string(get->GetTableAlias());
          std::vector<std::unique_ptr<OperatorExpression>> c;
          auto result = std::make_unique<OperatorExpression>(
              IndexScan::Make(db_oid, ns_oid, index, std::move(preds), tbl_alias, is_update, {}, {}, {}), std::move(c));
          transformed->emplace_back(std::move(result));
        }
      }
    }
  }

  // Check whether any index can fulfill predicate predicate evaluation
  if (!get->GetPredicates().empty()) {
    IndexUtilMetadata metadata;
    IndexUtil::PopulateMetadata(get->GetPredicates(), &metadata);

    // Find match index for the predicates
    auto indexes = accessor->GetIndexOids(get->GetTableOid());
    for (auto &index : indexes) {
      IndexUtilMetadata output;
      if (IndexUtil::SatisfiesPredicateWithIndex(accessor, get->GetTableOid(), index, &metadata, &output)) {
        std::vector<AnnotatedExpression> preds = get->GetPredicates();
        std::string tbl_alias = std::string(get->GetTableAlias());

        // Consider making IndexScan take in IndexUtilMetadata
        // instead to wrap all these vectors?
        std::vector<std::unique_ptr<OperatorExpression>> c;
        auto result = std::make_unique<OperatorExpression>(
            IndexScan::Make(db_oid, ns_oid, index, std::move(preds), std::move(tbl_alias), is_update,
                            std::move(output.GetPredicateColumnIds()), std::move(output.GetPredicateExprTypes()),
                            std::move(output.GetPredicateValues())),
            std::move(c));
        transformed->emplace_back(std::move(result));
      }
    }
  }
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalQueryDerivedGetToPhysicalQueryDerivedScan
///////////////////////////////////////////////////////////////////////////////
LogicalQueryDerivedGetToPhysicalQueryDerivedScan::LogicalQueryDerivedGetToPhysicalQueryDerivedScan() {
  type_ = RuleType::QUERY_DERIVED_GET_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALQUERYDERIVEDGET);

  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalQueryDerivedGetToPhysicalQueryDerivedScan::Check(common::ManagedPointer<OperatorExpression> plan,
                                                             OptimizationContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void LogicalQueryDerivedGetToPhysicalQueryDerivedScan::Transform(
    common::ManagedPointer<OperatorExpression> input, std::vector<std::unique_ptr<OperatorExpression>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto get = input->GetOp().As<LogicalQueryDerivedGet>();

  auto tbl_alias = std::string(get->GetTableAlias());
  std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> expr_map;
  expr_map = get->GetAliasToExprMap();

  auto input_child = input->GetChildren()[0];
  std::vector<std::unique_ptr<OperatorExpression>> c;
  c.emplace_back(input_child->Copy());
  auto result_plan =
      std::make_unique<OperatorExpression>(QueryDerivedScan::Make(tbl_alias, std::move(expr_map)), std::move(c));
  transformed->emplace_back(std::move(result_plan));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalExternalFileGetToPhysicalExternalFileGet
///////////////////////////////////////////////////////////////////////////////
LogicalExternalFileGetToPhysicalExternalFileGet::LogicalExternalFileGetToPhysicalExternalFileGet() {
  type_ = RuleType::EXTERNAL_FILE_GET_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALEXTERNALFILEGET);
}

bool LogicalExternalFileGetToPhysicalExternalFileGet::Check(common::ManagedPointer<OperatorExpression> plan,
                                                            OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalExternalFileGetToPhysicalExternalFileGet::Transform(
    common::ManagedPointer<OperatorExpression> input, std::vector<std::unique_ptr<OperatorExpression>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto get = input->GetOp().As<LogicalExternalFileGet>();
  TERRIER_ASSERT(input->GetChildren().empty(), "ExternalFileScan should have no children");

  auto format = get->GetFormat();
  const auto &filename = get->GetFilename();
  auto delimiter = get->GetDelimiter();
  auto quote = get->GetQuote();
  auto escape = get->GetEscape();
  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto result_plan = std::make_unique<OperatorExpression>(
      ExternalFileScan::Make(format, filename, delimiter, quote, escape), std::move(c));
  transformed->emplace_back(std::move(result_plan));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalDeleteToPhysicalDelete
///////////////////////////////////////////////////////////////////////////////
LogicalDeleteToPhysicalDelete::LogicalDeleteToPhysicalDelete() {
  type_ = RuleType::DELETE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALDELETE);
  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalDeleteToPhysicalDelete::Check(common::ManagedPointer<OperatorExpression> plan,
                                          OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalDeleteToPhysicalDelete::Transform(common::ManagedPointer<OperatorExpression> input,
                                              std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                              UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto del = input->GetOp().As<LogicalDelete>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalDelete should have 1 child");

  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));
  auto result = std::make_unique<OperatorExpression>(
      Delete::Make(del->GetDatabaseOid(), del->GetNamespaceOid(), del->GetTableAlias(), del->GetTableOid()),
      std::move(c));
  transformed->emplace_back(std::move(result));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalUpdateToPhysicalUpdate
///////////////////////////////////////////////////////////////////////////////
LogicalUpdateToPhysicalUpdate::LogicalUpdateToPhysicalUpdate() {
  type_ = RuleType::UPDATE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALUPDATE);

  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalUpdateToPhysicalUpdate::Check(common::ManagedPointer<OperatorExpression> plan,
                                          OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalUpdateToPhysicalUpdate::Transform(common::ManagedPointer<OperatorExpression> input,
                                              std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                              UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto update_op = input->GetOp().As<LogicalUpdate>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalUpdate should have 1 child");
  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));

  std::vector<common::ManagedPointer<parser::UpdateClause>> cls = update_op->GetUpdateClauses();
  auto result = std::make_unique<OperatorExpression>(
      Update::Make(update_op->GetDatabaseOid(), update_op->GetNamespaceOid(), update_op->GetTableAlias(),
                   update_op->GetTableOid(), std::move(cls)),
      std::move(c));
  transformed->emplace_back(std::move(result));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalInsertToPhysicalInsert
///////////////////////////////////////////////////////////////////////////////
LogicalInsertToPhysicalInsert::LogicalInsertToPhysicalInsert() {
  type_ = RuleType::INSERT_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALINSERT);
}

bool LogicalInsertToPhysicalInsert::Check(common::ManagedPointer<OperatorExpression> plan,
                                          OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalInsertToPhysicalInsert::Transform(common::ManagedPointer<OperatorExpression> input,
                                              std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                              UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto insert_op = input->GetOp().As<LogicalInsert>();
  TERRIER_ASSERT(input->GetChildren().empty(), "LogicalInsert should have 0 children");

  // TODO(wz2): For now any insert will update all indexes
  auto *accessor = context->GetOptimizerContext()->GetCatalogAccessor();
  auto tbl_oid = insert_op->GetTableOid();
  auto indexes = accessor->GetIndexOids(tbl_oid);

  std::vector<std::unique_ptr<OperatorExpression>> c;
  std::vector<catalog::col_oid_t> cols(insert_op->GetColumns());
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> vals = *(insert_op->GetValues());
  auto result = std::make_unique<OperatorExpression>(
      Insert::Make(insert_op->GetDatabaseOid(), insert_op->GetNamespaceOid(), insert_op->GetTableOid(), std::move(cols),
                   std::move(vals), std::move(indexes)),
      std::move(c));
  transformed->emplace_back(std::move(result));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalInsertSelectToPhysicalInsertSelect
///////////////////////////////////////////////////////////////////////////////
LogicalInsertSelectToPhysicalInsertSelect::LogicalInsertSelectToPhysicalInsertSelect() {
  type_ = RuleType::INSERT_SELECT_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALINSERTSELECT);

  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalInsertSelectToPhysicalInsertSelect::Check(common::ManagedPointer<OperatorExpression> plan,
                                                      OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalInsertSelectToPhysicalInsertSelect::Transform(common::ManagedPointer<OperatorExpression> input,
                                                          std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                                          UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto insert_op = input->GetOp().As<LogicalInsertSelect>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalInsertSelect should have 1 child");

  // For now, insert any tuple will modify indexes
  auto *accessor = context->GetOptimizerContext()->GetCatalogAccessor();
  auto tbl_oid = insert_op->GetTableOid();
  auto indexes = accessor->GetIndexOids(tbl_oid);

  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));
  auto op =
      std::make_unique<OperatorExpression>(InsertSelect::Make(insert_op->GetDatabaseOid(), insert_op->GetNamespaceOid(),
                                                              insert_op->GetTableOid(), std::move(indexes)),
                                           std::move(c));
  transformed->emplace_back(std::move(op));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalAggregateAndGroupByToHashGroupBy
///////////////////////////////////////////////////////////////////////////////
LogicalGroupByToPhysicalHashGroupBy::LogicalGroupByToPhysicalHashGroupBy() {
  type_ = RuleType::AGGREGATE_TO_HASH_AGGREGATE;
  match_pattern_ = new Pattern(OpType::LOGICALAGGREGATEANDGROUPBY);

  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalGroupByToPhysicalHashGroupBy::Check(common::ManagedPointer<OperatorExpression> plan,
                                                OptimizationContext *context) const {
  (void)context;
  const auto agg_op = plan->GetOp().As<LogicalAggregateAndGroupBy>();
  return !agg_op->GetColumns().empty();
}

void LogicalGroupByToPhysicalHashGroupBy::Transform(common::ManagedPointer<OperatorExpression> input,
                                                    std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                                    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto agg_op = input->GetOp().As<LogicalAggregateAndGroupBy>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalAggregateAndGroupBy should have 1 child");

  std::vector<common::ManagedPointer<parser::AbstractExpression>> cols = agg_op->GetColumns();
  std::vector<AnnotatedExpression> having = agg_op->GetHaving();

  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));

  auto result =
      std::make_unique<OperatorExpression>(HashGroupBy::Make(std::move(cols), std::move(having)), std::move(c));
  transformed->emplace_back(std::move(result));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalAggregateToPhysicalAggregate
///////////////////////////////////////////////////////////////////////////////
LogicalAggregateToPhysicalAggregate::LogicalAggregateToPhysicalAggregate() {
  type_ = RuleType::AGGREGATE_TO_PLAIN_AGGREGATE;
  match_pattern_ = new Pattern(OpType::LOGICALAGGREGATEANDGROUPBY);

  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalAggregateToPhysicalAggregate::Check(common::ManagedPointer<OperatorExpression> plan,
                                                OptimizationContext *context) const {
  (void)context;
  const auto agg_op = plan->GetOp().As<LogicalAggregateAndGroupBy>();
  return agg_op->GetColumns().empty();
}

void LogicalAggregateToPhysicalAggregate::Transform(common::ManagedPointer<OperatorExpression> input,
                                                    std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                                    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalAggregateAndGroupBy should have 1 child");

  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));

  auto result = std::make_unique<OperatorExpression>(Aggregate::Make(), std::move(c));
  transformed->emplace_back(std::move(result));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalInnerJoinToPhysicalInnerNLJoin
///////////////////////////////////////////////////////////////////////////////
LogicalInnerJoinToPhysicalInnerNLJoin::LogicalInnerJoinToPhysicalInnerNLJoin() {
  type_ = RuleType::INNER_JOIN_TO_NL_JOIN;

  auto left_child = new Pattern(OpType::LEAF);
  auto right_child = new Pattern(OpType::LEAF);

  // Initialize a pattern for optimizer to match
  match_pattern_ = new Pattern(OpType::LOGICALINNERJOIN);

  // Add node - we match join relation R and S
  match_pattern_->AddChild(left_child);
  match_pattern_->AddChild(right_child);
}

bool LogicalInnerJoinToPhysicalInnerNLJoin::Check(common::ManagedPointer<OperatorExpression> plan,
                                                  OptimizationContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void LogicalInnerJoinToPhysicalInnerNLJoin::Transform(common::ManagedPointer<OperatorExpression> input,
                                                      std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                                      UNUSED_ATTRIBUTE OptimizationContext *context) const {
  // first build an expression representing hash join
  const auto inner_join = input->GetOp().As<LogicalInnerJoin>();

  const auto &children = input->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "Inner Join should have two child");
  auto left_group_id = children[0]->GetOp().As<LeafOperator>()->GetOriginGroup();
  auto right_group_id = children[1]->GetOp().As<LeafOperator>()->GetOriginGroup();
  auto &left_group_alias = context->GetOptimizerContext()->GetMemo().GetGroupByID(left_group_id)->GetTableAliases();
  auto &right_group_alias = context->GetOptimizerContext()->GetMemo().GetGroupByID(right_group_id)->GetTableAliases();
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys;

  std::vector<AnnotatedExpression> join_preds = inner_join->GetJoinPredicates();
  OptimizerUtil::ExtractEquiJoinKeys(join_preds, &left_keys, &right_keys, left_group_alias, right_group_alias);

  TERRIER_ASSERT(right_keys.size() == left_keys.size(), "# left/right keys should equal");
  std::vector<std::unique_ptr<OperatorExpression>> child;
  child.emplace_back(children[0]->Copy());
  child.emplace_back(children[1]->Copy());
  auto result = std::make_unique<OperatorExpression>(
      InnerNLJoin::Make(std::move(join_preds), std::move(left_keys), std::move(right_keys)), std::move(child));
  transformed->emplace_back(std::move(result));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalInnerJoinToPhysicalInnerHashJoin
///////////////////////////////////////////////////////////////////////////////
LogicalInnerJoinToPhysicalInnerHashJoin::LogicalInnerJoinToPhysicalInnerHashJoin() {
  type_ = RuleType::INNER_JOIN_TO_HASH_JOIN;

  // Make three node types for pattern matching
  auto left_child(new Pattern(OpType::LEAF));
  auto right_child(new Pattern(OpType::LEAF));

  // Initialize a pattern for optimizer to match
  match_pattern_ = new Pattern(OpType::LOGICALINNERJOIN);

  // Add node - we match join relation R and S as well as the predicate exp
  match_pattern_->AddChild(left_child);
  match_pattern_->AddChild(right_child);
}

bool LogicalInnerJoinToPhysicalInnerHashJoin::Check(common::ManagedPointer<OperatorExpression> plan,
                                                    OptimizationContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void LogicalInnerJoinToPhysicalInnerHashJoin::Transform(common::ManagedPointer<OperatorExpression> input,
                                                        std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                                        UNUSED_ATTRIBUTE OptimizationContext *context) const {
  // first build an expression representing hash join
  const auto inner_join = input->GetOp().As<LogicalInnerJoin>();

  auto children = input->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "Inner Join should have two child");
  auto left_group_id = children[0]->GetOp().As<LeafOperator>()->GetOriginGroup();
  auto right_group_id = children[1]->GetOp().As<LeafOperator>()->GetOriginGroup();
  auto &left_group_alias = context->GetOptimizerContext()->GetMemo().GetGroupByID(left_group_id)->GetTableAliases();
  auto &right_group_alias = context->GetOptimizerContext()->GetMemo().GetGroupByID(right_group_id)->GetTableAliases();
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys;

  std::vector<AnnotatedExpression> join_preds = inner_join->GetJoinPredicates();
  OptimizerUtil::ExtractEquiJoinKeys(join_preds, &left_keys, &right_keys, left_group_alias, right_group_alias);

  TERRIER_ASSERT(right_keys.size() == left_keys.size(), "# left/right keys should equal");
  std::vector<std::unique_ptr<OperatorExpression>> child;
  child.emplace_back(children[0]->Copy());
  child.emplace_back(children[1]->Copy());
  if (!left_keys.empty()) {
    auto result = std::make_unique<OperatorExpression>(
        InnerHashJoin::Make(std::move(join_preds), std::move(left_keys), std::move(right_keys)), std::move(child));
    transformed->emplace_back(std::move(result));
  }
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalLimitToPhysicalLimit
///////////////////////////////////////////////////////////////////////////////
LogicalLimitToPhysicalLimit::LogicalLimitToPhysicalLimit() {
  type_ = RuleType::IMPLEMENT_LIMIT;

  match_pattern_ = new Pattern(OpType::LOGICALLIMIT);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
}

bool LogicalLimitToPhysicalLimit::Check(common::ManagedPointer<OperatorExpression> plan,
                                        OptimizationContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void LogicalLimitToPhysicalLimit::Transform(common::ManagedPointer<OperatorExpression> input,
                                            std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                            OptimizationContext *context) const {
  (void)context;
  const auto limit_op = input->GetOp().As<LogicalLimit>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalLimit should have 1 child");

  std::vector<common::ManagedPointer<parser::AbstractExpression>> sorts = limit_op->GetSortExpressions();
  std::vector<OrderByOrderingType> types = limit_op->GetSortDirections();
  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));

  auto result_plan = std::make_unique<OperatorExpression>(
      Limit::Make(limit_op->GetOffset(), limit_op->GetLimit(), std::move(sorts), std::move(types)), std::move(c));
  transformed->emplace_back(std::move(result_plan));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalExport to Physical Export
///////////////////////////////////////////////////////////////////////////////
LogicalExportToPhysicalExport::LogicalExportToPhysicalExport() {
  type_ = RuleType::EXPORT_EXTERNAL_FILE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALEXPORTEXTERNALFILE);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
}

bool LogicalExportToPhysicalExport::Check(common::ManagedPointer<OperatorExpression> plan,
                                          OptimizationContext *context) const {
  return true;
}

void LogicalExportToPhysicalExport::Transform(common::ManagedPointer<OperatorExpression> input,
                                              std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                                              UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto export_op = input->GetOp().As<LogicalExportExternalFile>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalExport should have 1 child");

  std::string file = std::string(export_op->GetFilename());
  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));

  auto result_plan = std::make_unique<OperatorExpression>(
      ExportExternalFile::Make(export_op->GetFormat(), std::move(file), export_op->GetDelimiter(),
                               export_op->GetQuote(), export_op->GetEscape()),
      std::move(c));
  transformed->emplace_back(std::move(result_plan));
}

}  // namespace terrier::optimizer
