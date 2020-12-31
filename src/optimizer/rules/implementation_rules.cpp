#include "optimizer/rules/implementation_rules.h"

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
#include "optimizer/logical_operators.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/physical_operators.h"
#include "optimizer/properties.h"
#include "optimizer/util.h"
#include "parser/expression_util.h"
#include "storage/storage_defs.h"

namespace noisepage::optimizer {

///////////////////////////////////////////////////////////////////////////////
/// LogicalGetToPhysicalTableFreeScan
///////////////////////////////////////////////////////////////////////////////
LogicalGetToPhysicalTableFreeScan::LogicalGetToPhysicalTableFreeScan() {
  type_ = RuleType::GET_TO_DUMMY_SCAN;
  match_pattern_ = new Pattern(OpType::LOGICALGET);
}

bool LogicalGetToPhysicalTableFreeScan::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                              OptimizationContext *context) const {
  (void)context;
  const auto get = plan->Contents()->GetContentsAs<LogicalGet>();
  return get->GetTableOid() == catalog::INVALID_TABLE_OID;
}

void LogicalGetToPhysicalTableFreeScan::Transform(UNUSED_ATTRIBUTE common::ManagedPointer<AbstractOptimizerNode> input,
                                                  std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                                  UNUSED_ATTRIBUTE OptimizationContext *context) const {
  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  auto result_plan = std::make_unique<OperatorNode>(
      TableFreeScan::Make().RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()), std::move(c),
      context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(result_plan));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalGetToPhysicalSeqScan
///////////////////////////////////////////////////////////////////////////////
LogicalGetToPhysicalSeqScan::LogicalGetToPhysicalSeqScan() {
  type_ = RuleType::GET_TO_SEQ_SCAN;
  match_pattern_ = new Pattern(OpType::LOGICALGET);
}

bool LogicalGetToPhysicalSeqScan::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                        OptimizationContext *context) const {
  (void)context;
  const auto get = plan->Contents()->GetContentsAs<LogicalGet>();
  return get->GetTableOid() != catalog::INVALID_TABLE_OID;
}

void LogicalGetToPhysicalSeqScan::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                            std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                            UNUSED_ATTRIBUTE OptimizationContext *context) const {
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "Get should have no children");
  const auto get = input->Contents()->GetContentsAs<LogicalGet>();

  // Need to copy because SeqScan uses std::move
  auto db_oid = get->GetDatabaseOid();
  auto tbl_oid = get->GetTableOid();
  auto tbl_alias = std::string(get->GetTableAlias());
  auto preds = std::vector<AnnotatedExpression>(get->GetPredicates());
  auto is_update = get->GetIsForUpdate();
  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  auto result_plan =
      std::make_unique<OperatorNode>(SeqScan::Make(db_oid, tbl_oid, std::move(preds), tbl_alias, is_update)
                                         .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                     std::move(c), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(result_plan));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalGetToPhysicalIndexScan
///////////////////////////////////////////////////////////////////////////////
LogicalGetToPhysicalIndexScan::LogicalGetToPhysicalIndexScan() {
  type_ = RuleType::GET_TO_INDEX_SCAN;
  match_pattern_ = new Pattern(OpType::LOGICALGET);
}

bool LogicalGetToPhysicalIndexScan::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                          OptimizationContext *context) const {
  // If there is a index for the table, return true,
  // else return false
  (void)context;
  const auto get = plan->Contents()->GetContentsAs<LogicalGet>();
  if (get == nullptr) {
    return false;
  }

  if (get->GetTableOid() == catalog::INVALID_TABLE_OID) {
    return false;
  }

  auto *accessor = context->GetOptimizerContext()->GetCatalogAccessor();
  return !accessor->GetIndexOids(get->GetTableOid()).empty();
}

void LogicalGetToPhysicalIndexScan::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                              std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                              UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto get = input->Contents()->GetContentsAs<LogicalGet>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "Get should have no children");

  auto db_oid = get->GetDatabaseOid();
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
          planner::IndexScanType scan_type;
          std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> bounds;
          auto predicate_satisfaction = IndexUtil::SatisfiesPredicateWithIndex(
              accessor, get->GetTableOid(), get->GetTableAlias(), index, preds, allow_cves_, &scan_type, &bounds);
          auto op = std::make_unique<OperatorNode>(
              IndexScan::Make(db_oid, get->GetTableOid(), index, std::move(preds), is_update,
                              planner::IndexScanType::AscendingOpenBoth, {}, predicate_satisfaction.second)
                  .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
              std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
          transformed->emplace_back(std::move(op));
        }
      }
    }
  }

  // Check whether any index can fulfill predicate predicate evaluation
  if (!get->GetPredicates().empty()) {
    // Find match index for the predicates
    auto indexes = accessor->GetIndexOids(get->GetTableOid());
    for (auto &index : indexes) {
      planner::IndexScanType scan_type;
      std::unordered_map<catalog::indexkeycol_oid_t, std::vector<planner::IndexExpression>> bounds;
      std::vector<AnnotatedExpression> preds = get->GetPredicates();
      auto predicate_satisfaction = IndexUtil::SatisfiesPredicateWithIndex(
          accessor, get->GetTableOid(), get->GetTableAlias(), index, preds, allow_cves_, &scan_type, &bounds);
      if (predicate_satisfaction.first) {
        auto op = std::make_unique<OperatorNode>(
            IndexScan::Make(db_oid, get->GetTableOid(), index, std::move(preds), is_update, scan_type,
                            std::move(bounds), predicate_satisfaction.second)
                .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
            std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
        transformed->emplace_back(std::move(op));
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

bool LogicalQueryDerivedGetToPhysicalQueryDerivedScan::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                             OptimizationContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void LogicalQueryDerivedGetToPhysicalQueryDerivedScan::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto get = input->Contents()->GetContentsAs<LogicalQueryDerivedGet>();

  auto tbl_alias = std::string(get->GetTableAlias());
  std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> expr_map;
  expr_map = get->GetAliasToExprMap();

  auto input_child = input->GetChildren()[0];
  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  c.emplace_back(input_child->Copy());
  auto result_plan =
      std::make_unique<OperatorNode>(QueryDerivedScan::Make(tbl_alias, std::move(expr_map))
                                         .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                     std::move(c), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(result_plan));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalExternalFileGetToPhysicalExternalFileGet
///////////////////////////////////////////////////////////////////////////////
LogicalExternalFileGetToPhysicalExternalFileGet::LogicalExternalFileGetToPhysicalExternalFileGet() {
  type_ = RuleType::EXTERNAL_FILE_GET_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALEXTERNALFILEGET);
}

bool LogicalExternalFileGetToPhysicalExternalFileGet::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                            OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalExternalFileGetToPhysicalExternalFileGet::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto get = input->Contents()->GetContentsAs<LogicalExternalFileGet>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "ExternalFileScan should have no children");

  auto format = get->GetFormat();
  const auto &filename = get->GetFilename();
  auto delimiter = get->GetDelimiter();
  auto quote = get->GetQuote();
  auto escape = get->GetEscape();
  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  auto result_plan =
      std::make_unique<OperatorNode>(ExternalFileScan::Make(format, filename, delimiter, quote, escape)
                                         .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                     std::move(c), context->GetOptimizerContext()->GetTxn());
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

bool LogicalDeleteToPhysicalDelete::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                          OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalDeleteToPhysicalDelete::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                              std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                              UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto del = input->Contents()->GetContentsAs<LogicalDelete>();
  NOISEPAGE_ASSERT(input->GetChildren().size() == 1, "LogicalDelete should have 1 child");

  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));
  auto result =
      std::make_unique<OperatorNode>(Delete::Make(del->GetDatabaseOid(), del->GetTableAlias(), del->GetTableOid())
                                         .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                     std::move(c), context->GetOptimizerContext()->GetTxn());
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

bool LogicalUpdateToPhysicalUpdate::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                          OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalUpdateToPhysicalUpdate::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                              std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                              UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto update_op = input->Contents()->GetContentsAs<LogicalUpdate>();
  NOISEPAGE_ASSERT(input->GetChildren().size() == 1, "LogicalUpdate should have 1 child");
  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));

  std::vector<common::ManagedPointer<parser::UpdateClause>> cls = update_op->GetUpdateClauses();
  auto result = std::make_unique<OperatorNode>(
      Update::Make(update_op->GetDatabaseOid(), update_op->GetTableAlias(), update_op->GetTableOid(), std::move(cls))
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::move(c), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(result));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalInsertToPhysicalInsert
///////////////////////////////////////////////////////////////////////////////
LogicalInsertToPhysicalInsert::LogicalInsertToPhysicalInsert() {
  type_ = RuleType::INSERT_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALINSERT);
}

bool LogicalInsertToPhysicalInsert::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                          OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalInsertToPhysicalInsert::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                              std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                              UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto insert_op = input->Contents()->GetContentsAs<LogicalInsert>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalInsert should have 0 children");

  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  std::vector<catalog::col_oid_t> cols(insert_op->GetColumns());
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> vals = *(insert_op->GetValues());
  auto result = std::make_unique<OperatorNode>(
      Insert::Make(insert_op->GetDatabaseOid(), insert_op->GetTableOid(), std::move(cols), std::move(vals))
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::move(c), context->GetOptimizerContext()->GetTxn());
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

bool LogicalInsertSelectToPhysicalInsertSelect::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                      OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalInsertSelectToPhysicalInsertSelect::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto insert_op = input->Contents()->GetContentsAs<LogicalInsertSelect>();
  NOISEPAGE_ASSERT(input->GetChildren().size() == 1, "LogicalInsertSelect should have 1 child");

  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));
  auto op = std::make_unique<OperatorNode>(InsertSelect::Make(insert_op->GetDatabaseOid(), insert_op->GetTableOid())
                                               .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                           std::move(c), context->GetOptimizerContext()->GetTxn());
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

bool LogicalGroupByToPhysicalHashGroupBy::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                OptimizationContext *context) const {
  (void)context;
  const auto agg_op = plan->Contents()->GetContentsAs<LogicalAggregateAndGroupBy>();
  return !agg_op->GetColumns().empty() || !agg_op->GetHaving().empty();
}

void LogicalGroupByToPhysicalHashGroupBy::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                                    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                                    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto agg_op = input->Contents()->GetContentsAs<LogicalAggregateAndGroupBy>();
  NOISEPAGE_ASSERT(input->GetChildren().size() == 1, "LogicalAggregateAndGroupBy should have 1 child");

  std::vector<common::ManagedPointer<parser::AbstractExpression>> cols = agg_op->GetColumns();
  std::vector<AnnotatedExpression> having = agg_op->GetHaving();

  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));

  auto result = std::make_unique<OperatorNode>(HashGroupBy::Make(std::move(cols), std::move(having))
                                                   .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                               std::move(c), context->GetOptimizerContext()->GetTxn());
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

bool LogicalAggregateToPhysicalAggregate::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                OptimizationContext *context) const {
  (void)context;
  const auto agg_op = plan->Contents()->GetContentsAs<LogicalAggregateAndGroupBy>();
  return agg_op->GetColumns().empty() && agg_op->GetHaving().empty();
}

void LogicalAggregateToPhysicalAggregate::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                                    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                                    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  NOISEPAGE_ASSERT(input->GetChildren().size() == 1, "LogicalAggregateAndGroupBy should have 1 child");

  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));

  auto result =
      std::make_unique<OperatorNode>(Aggregate::Make().RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                     std::move(c), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(result));
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalInnerJoinToPhysicalInnerIndexJoin
///////////////////////////////////////////////////////////////////////////////
LogicalInnerJoinToPhysicalInnerIndexJoin::LogicalInnerJoinToPhysicalInnerIndexJoin() {
  type_ = RuleType::INNER_JOIN_TO_INDEX_JOIN;

  auto left_child = new Pattern(OpType::LEAF);
  auto right_child = new Pattern(OpType::LOGICALGET);

  // Initialize a pattern for optimizer to match
  match_pattern_ = new Pattern(OpType::LOGICALINNERJOIN);

  // Add node - we match join relation R and S
  match_pattern_->AddChild(left_child);
  match_pattern_->AddChild(right_child);
}

bool LogicalInnerJoinToPhysicalInnerIndexJoin::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                     OptimizationContext *context) const {
  (void)context;
  (void)plan;
  UNUSED_ATTRIBUTE const auto join = plan->Contents()->GetContentsAs<LogicalInnerJoin>();
  NOISEPAGE_ASSERT(join != nullptr, "Plan should be a Inner Join");
  NOISEPAGE_ASSERT(plan->GetChildren().size() == 2, "Inner Join should have two children");

  // Get the "right" inner child
  auto rchild = plan->GetChildren()[1];
  UNUSED_ATTRIBUTE auto get = rchild->Contents()->GetContentsAs<LogicalGet>();
  NOISEPAGE_ASSERT(get != nullptr, "Right child should be a GET");

  // Check whether the right child can be done as an index scan
  LogicalGetToPhysicalIndexScan idx_scan_check;
  return idx_scan_check.Check(rchild, context);
}

void LogicalInnerJoinToPhysicalInnerIndexJoin::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  // first build an expression representing hash join
  const auto inner_join = input->Contents()->GetContentsAs<LogicalInnerJoin>();
  const auto &children = input->GetChildren();
  NOISEPAGE_ASSERT(children.size() == 2, "Inner Join should have two children");

  // Get the "right" inner child and append the join predicate
  auto r_child = children[1]->Contents()->GetContentsAs<LogicalGet>();

  // Combine the index scan predicates
  std::vector<AnnotatedExpression> join_preds = r_child->GetPredicates();
  for (auto &pred : inner_join->GetJoinPredicates()) join_preds.push_back(pred);

  std::vector<std::unique_ptr<AbstractOptimizerNode>> empty;
  auto new_child =
      std::make_unique<OperatorNode>(LogicalGet::Make(r_child->GetDatabaseOid(), r_child->GetTableOid(), join_preds,
                                                      r_child->GetTableAlias(), r_child->GetIsForUpdate())
                                         .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
                                     std::move(empty), context->GetOptimizerContext()->GetTxn());

  std::vector<std::unique_ptr<AbstractOptimizerNode>> transform;
  LogicalGetToPhysicalIndexScan idx_scan_transform;
  idx_scan_transform.SetAllowCVEs(true);
  idx_scan_transform.Transform(common::ManagedPointer<AbstractOptimizerNode>(new_child.get()), &transform, context);

  for (const auto &idx_op : transform) {
    auto idx_scan = idx_op->Contents()->GetContentsAs<IndexScan>();
    NOISEPAGE_ASSERT(idx_scan != nullptr, "Transformation should have produced an IndexScan");

    if (!idx_scan->GetBounds().empty()) {
      std::vector<std::unique_ptr<AbstractOptimizerNode>> child;
      child.emplace_back(children[0]->Copy());

      auto result = std::make_unique<OperatorNode>(
          InnerIndexJoin::Make(idx_scan->GetTableOID(), idx_scan->GetIndexOID(), idx_scan->GetIndexScanType(),
                               idx_scan->GetBounds(), join_preds)
              .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
          std::move(child), context->GetOptimizerContext()->GetTxn());
      transformed->emplace_back(std::move(result));
    }
  }
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

bool LogicalInnerJoinToPhysicalInnerNLJoin::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                  OptimizationContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void LogicalInnerJoinToPhysicalInnerNLJoin::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                                      std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                                      UNUSED_ATTRIBUTE OptimizationContext *context) const {
  // first build an expression representing hash join
  const auto inner_join = input->Contents()->GetContentsAs<LogicalInnerJoin>();

  const auto &children = input->GetChildren();
  NOISEPAGE_ASSERT(children.size() == 2, "Inner Join should have two children");
  std::vector<AnnotatedExpression> join_preds = inner_join->GetJoinPredicates();

  std::vector<std::unique_ptr<AbstractOptimizerNode>> child;
  child.emplace_back(children[0]->Copy());
  child.emplace_back(children[1]->Copy());
  auto result = std::make_unique<OperatorNode>(
      InnerNLJoin::Make(std::move(join_preds)).RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::move(child), context->GetOptimizerContext()->GetTxn());
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

bool LogicalInnerJoinToPhysicalInnerHashJoin::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                    OptimizationContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void LogicalInnerJoinToPhysicalInnerHashJoin::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  // first build an expression representing hash join
  const auto inner_join = input->Contents()->GetContentsAs<LogicalInnerJoin>();

  auto children = input->GetChildren();
  NOISEPAGE_ASSERT(children.size() == 2, "Inner Join should have two children");
  auto left_group_id = children[0]->Contents()->GetContentsAs<LeafOperator>()->GetOriginGroup();
  auto right_group_id = children[1]->Contents()->GetContentsAs<LeafOperator>()->GetOriginGroup();
  auto &left_group_alias = context->GetOptimizerContext()->GetMemo().GetGroupByID(left_group_id)->GetTableAliases();
  auto &right_group_alias = context->GetOptimizerContext()->GetMemo().GetGroupByID(right_group_id)->GetTableAliases();
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys;

  std::vector<AnnotatedExpression> join_preds = inner_join->GetJoinPredicates();
  OptimizerUtil::ExtractEquiJoinKeys(join_preds, &left_keys, &right_keys, left_group_alias, right_group_alias);

  NOISEPAGE_ASSERT(right_keys.size() == left_keys.size(), "# left/right keys should equal");
  std::vector<std::unique_ptr<AbstractOptimizerNode>> child;
  child.emplace_back(children[0]->Copy());
  child.emplace_back(children[1]->Copy());
  if (!left_keys.empty()) {
    auto result = std::make_unique<OperatorNode>(
        InnerHashJoin::Make(std::move(join_preds), std::move(left_keys), std::move(right_keys))
            .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
        std::move(child), context->GetOptimizerContext()->GetTxn());
    transformed->emplace_back(std::move(result));
  }
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalSemiJoinToPhysicalSemiLeftHashJoin
///////////////////////////////////////////////////////////////////////////////
LogicalSemiJoinToPhysicalSemiLeftHashJoin::LogicalSemiJoinToPhysicalSemiLeftHashJoin() {
  type_ = RuleType::SEMI_JOIN_TO_HASH_JOIN;
  // Make three node types for pattern matching
  auto left_child(new Pattern(OpType::LEAF));
  auto right_child(new Pattern(OpType::LEAF));

  // Initialize a pattern for optimizer to match
  match_pattern_ = new Pattern(OpType::LOGICALSEMIJOIN);

  // Add node - we match join relation R and S as well as the predicate exp
  match_pattern_->AddChild(left_child);
  match_pattern_->AddChild(right_child);
}

bool LogicalSemiJoinToPhysicalSemiLeftHashJoin::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                      OptimizationContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void LogicalSemiJoinToPhysicalSemiLeftHashJoin::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  // first build an expression representing hash join
  const auto semi_join = input->Contents()->GetContentsAs<LogicalSemiJoin>();

  auto children = input->GetChildren();
  NOISEPAGE_ASSERT(children.size() == 2, "Left Semi Join should have two children");

  auto left_group_id = children[0]->Contents()->GetContentsAs<LeafOperator>()->GetOriginGroup();
  auto right_group_id = children[1]->Contents()->GetContentsAs<LeafOperator>()->GetOriginGroup();
  auto &left_group_alias = context->GetOptimizerContext()->GetMemo().GetGroupByID(left_group_id)->GetTableAliases();
  auto &right_group_alias = context->GetOptimizerContext()->GetMemo().GetGroupByID(right_group_id)->GetTableAliases();
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys;

  std::vector<AnnotatedExpression> join_preds = semi_join->GetJoinPredicates();
  OptimizerUtil::ExtractEquiJoinKeys(join_preds, &left_keys, &right_keys, left_group_alias, right_group_alias);

  NOISEPAGE_ASSERT(right_keys.size() == left_keys.size(), "# left/right keys should equal");
  std::vector<std::unique_ptr<AbstractOptimizerNode>> child;
  child.emplace_back(children[0]->Copy());
  child.emplace_back(children[1]->Copy());
  if (!left_keys.empty()) {
    auto result = std::make_unique<OperatorNode>(
        LeftSemiHashJoin::Make(std::move(join_preds), std::move(left_keys), std::move(right_keys))
            .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
        std::move(child), context->GetOptimizerContext()->GetTxn());
    transformed->emplace_back(std::move(result));
  }
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalLeftJoinToPhysicalLeftHashJoin
///////////////////////////////////////////////////////////////////////////////
LogicalLeftJoinToPhysicalLeftHashJoin::LogicalLeftJoinToPhysicalLeftHashJoin() {
  type_ = RuleType::LEFT_JOIN_TO_HASH_JOIN;

  // Make three node types for pattern matching
  auto left_child(new Pattern(OpType::LEAF));
  auto right_child(new Pattern(OpType::LEAF));

  // Initialize a pattern for optimizer to match
  match_pattern_ = new Pattern(OpType::LOGICALLEFTJOIN);

  // Add node - we match join relation R and S as well as the predicate exp
  match_pattern_->AddChild(left_child);
  match_pattern_->AddChild(right_child);
}

bool LogicalLeftJoinToPhysicalLeftHashJoin::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                  OptimizationContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void LogicalLeftJoinToPhysicalLeftHashJoin::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                                      std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                                      UNUSED_ATTRIBUTE OptimizationContext *context) const {
  // first build an expression representing hash join
  const auto left_join = input->Contents()->GetContentsAs<LogicalLeftJoin>();

  auto children = input->GetChildren();
  NOISEPAGE_ASSERT(children.size() == 2, "Left Join should have two child");

  auto left_group_id = children[0]->Contents()->GetContentsAs<LeafOperator>()->GetOriginGroup();
  auto right_group_id = children[1]->Contents()->GetContentsAs<LeafOperator>()->GetOriginGroup();
  auto &left_group_alias = context->GetOptimizerContext()->GetMemo().GetGroupByID(left_group_id)->GetTableAliases();
  auto &right_group_alias = context->GetOptimizerContext()->GetMemo().GetGroupByID(right_group_id)->GetTableAliases();
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys;

  std::vector<AnnotatedExpression> join_preds = left_join->GetJoinPredicates();

  OptimizerUtil::ExtractEquiJoinKeys(join_preds, &left_keys, &right_keys, left_group_alias, right_group_alias);

  NOISEPAGE_ASSERT(right_keys.size() == left_keys.size(), "# left/right keys should equal");
  std::vector<std::unique_ptr<AbstractOptimizerNode>> child;
  child.emplace_back(children[0]->Copy());
  child.emplace_back(children[1]->Copy());
  if (!left_keys.empty()) {
    auto result = std::make_unique<OperatorNode>(
        LeftHashJoin::Make(std::move(join_preds), std::move(left_keys), std::move(right_keys))
            .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
        std::move(child), context->GetOptimizerContext()->GetTxn());
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

bool LogicalLimitToPhysicalLimit::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                        OptimizationContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void LogicalLimitToPhysicalLimit::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                            std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                            OptimizationContext *context) const {
  (void)context;
  const auto limit_op = input->Contents()->GetContentsAs<LogicalLimit>();
  NOISEPAGE_ASSERT(input->GetChildren().size() == 1, "LogicalLimit should have 1 child");

  std::vector<common::ManagedPointer<parser::AbstractExpression>> sorts = limit_op->GetSortExpressions();
  std::vector<OrderByOrderingType> types = limit_op->GetSortDirections();
  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));

  auto result_plan = std::make_unique<OperatorNode>(
      Limit::Make(limit_op->GetOffset(), limit_op->GetLimit(), std::move(sorts), std::move(types))
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::move(c), context->GetOptimizerContext()->GetTxn());
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

bool LogicalExportToPhysicalExport::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                          OptimizationContext *context) const {
  return true;
}

void LogicalExportToPhysicalExport::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                              std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                              UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto export_op = input->Contents()->GetContentsAs<LogicalExportExternalFile>();
  NOISEPAGE_ASSERT(input->GetChildren().size() == 1, "LogicalExport should have 1 child");

  std::string file = std::string(export_op->GetFilename());
  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  auto child = input->GetChildren()[0]->Copy();
  c.emplace_back(std::move(child));

  auto result_plan = std::make_unique<OperatorNode>(
      ExportExternalFile::Make(export_op->GetFormat(), std::move(file), export_op->GetDelimiter(),
                               export_op->GetQuote(), export_op->GetEscape())
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::move(c), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(result_plan));
}

///////////////////////////////////////////////////////////////////////////////
/// Logical Create/Drop to Physical Create/Drop
///////////////////////////////////////////////////////////////////////////////
LogicalCreateDatabaseToPhysicalCreateDatabase::LogicalCreateDatabaseToPhysicalCreateDatabase() {
  type_ = RuleType::CREATE_DATABASE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALCREATEDATABASE);
}

bool LogicalCreateDatabaseToPhysicalCreateDatabase::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                          OptimizationContext *context) const {
  return true;
}

void LogicalCreateDatabaseToPhysicalCreateDatabase::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto cdb_op = input->Contents()->GetContentsAs<LogicalCreateDatabase>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalCreateDatabase should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      CreateDatabase::Make(cdb_op->GetDatabaseName()).RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(op));
}

LogicalCreateFunctionToPhysicalCreateFunction::LogicalCreateFunctionToPhysicalCreateFunction() {
  type_ = RuleType::CREATE_FUNCTION_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALCREATEFUNCTION);
}

bool LogicalCreateFunctionToPhysicalCreateFunction::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                          OptimizationContext *context) const {
  return true;
}

void LogicalCreateFunctionToPhysicalCreateFunction::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  const auto cf_op = input->Contents()->GetContentsAs<LogicalCreateFunction>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalCreateFunction should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      CreateFunction::Make(cf_op->GetDatabaseOid(), cf_op->GetNamespaceOid(), cf_op->GetFunctionName(),
                           cf_op->GetUDFLanguage(), cf_op->GetFunctionBody(), cf_op->GetFunctionParameterNames(),
                           cf_op->GetFunctionParameterTypes(), cf_op->GetReturnType(), cf_op->GetParamCount(),
                           cf_op->IsReplace())
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(op));
}

LogicalCreateIndexToPhysicalCreateIndex::LogicalCreateIndexToPhysicalCreateIndex() {
  type_ = RuleType::CREATE_INDEX_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALCREATEINDEX);
}

bool LogicalCreateIndexToPhysicalCreateIndex::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                    OptimizationContext *context) const {
  return true;
}

void LogicalCreateIndexToPhysicalCreateIndex::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto ci_op = input->Contents()->GetContentsAs<LogicalCreateIndex>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalCreateIndex should have 0 children");

  auto *accessor = context->GetOptimizerContext()->GetCatalogAccessor();
  const auto &tbl_schema = accessor->GetSchema(ci_op->GetTableOid());

  std::vector<catalog::IndexSchema::Column> cols;
  for (auto attr : ci_op->GetIndexAttr()) {
    // Information should already be derived
    auto name = attr->GetExpressionName();
    auto type = attr->GetReturnValueType();
    auto is_var = (type == type::TypeId::VARCHAR || type == type::TypeId::VARBINARY);

    // Need a catalog lookup to see if nullable
    bool nullable = false;
    uint16_t varlen_size = 0;
    if (attr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
      auto cve = attr.CastManagedPointerTo<parser::ColumnValueExpression>();
      auto &col = tbl_schema.GetColumn(cve->GetColumnOid());
      name = cve->GetColumnName();
      nullable = col.Nullable();
      if (is_var) varlen_size = col.TypeModifier();
    } else {
      // TODO(Matt): derive a unique name
      // TODO(wz2): Derive nullability/varlen from non ColumnValue
      nullable = true;
      varlen_size = UINT16_MAX;
    }

    if (is_var)
      cols.emplace_back(name, type, varlen_size, nullable, *attr);
    else
      cols.emplace_back(name, type, nullable, *attr);
  }

  storage::index::IndexType idx_type = storage::index::IndexType::BWTREE;
  switch (ci_op->GetIndexType()) {
    case parser::IndexType::BWTREE:
      idx_type = storage::index::IndexType::BWTREE;
      break;
    case parser::IndexType::HASH:
      idx_type = storage::index::IndexType::HASHMAP;
      break;
    default:
      NOISEPAGE_ASSERT(false, "Unsupported index type encountered");
      break;
  }

  auto schema = std::make_unique<catalog::IndexSchema>(std::move(cols), idx_type, ci_op->IsUnique(),
                                                       false,   // is_primary
                                                       false,   // is_exclusion
                                                       false);  // is_immediate

  auto op = std::make_unique<OperatorNode>(
      CreateIndex::Make(ci_op->GetNamespaceOid(), ci_op->GetTableOid(), ci_op->GetIndexName(), std::move(schema))
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(op));
}

LogicalCreateTableToPhysicalCreateTable::LogicalCreateTableToPhysicalCreateTable() {
  type_ = RuleType::CREATE_TABLE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALCREATETABLE);
}

bool LogicalCreateTableToPhysicalCreateTable::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                    OptimizationContext *context) const {
  return true;
}

void LogicalCreateTableToPhysicalCreateTable::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto ct_op = input->Contents()->GetContentsAs<LogicalCreateTable>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalCreateTable should have 0 children");

  std::vector<common::ManagedPointer<parser::ColumnDefinition>> cols;
  std::vector<common::ManagedPointer<parser::ColumnDefinition>> fks;
  for (const auto &col : ct_op->GetColumns()) {
    cols.push_back(col);
  }
  for (const auto &fk : ct_op->GetForeignKeys()) {
    fks.push_back(fk);
  }

  auto op = std::make_unique<OperatorNode>(
      CreateTable::Make(ct_op->GetNamespaceOid(), ct_op->GetTableName(), std::move(cols), std::move(fks))
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(op));
}

LogicalCreateNamespaceToPhysicalCreateNamespace::LogicalCreateNamespaceToPhysicalCreateNamespace() {
  type_ = RuleType::CREATE_NAMESPACE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALCREATENAMESPACE);
}

bool LogicalCreateNamespaceToPhysicalCreateNamespace::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                            OptimizationContext *context) const {
  return true;
}

void LogicalCreateNamespaceToPhysicalCreateNamespace::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto cn_op = input->Contents()->GetContentsAs<LogicalCreateNamespace>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalCreateNamespace should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      CreateNamespace::Make(cn_op->GetNamespaceName()).RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());

  transformed->emplace_back(std::move(op));
}

LogicalCreateTriggerToPhysicalCreateTrigger::LogicalCreateTriggerToPhysicalCreateTrigger() {
  type_ = RuleType::CREATE_TRIGGER_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALCREATETRIGGER);
}

bool LogicalCreateTriggerToPhysicalCreateTrigger::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                        OptimizationContext *context) const {
  return true;
}

void LogicalCreateTriggerToPhysicalCreateTrigger::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto ct_op = input->Contents()->GetContentsAs<LogicalCreateTrigger>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalCreateTrigger should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      CreateTrigger::Make(ct_op->GetDatabaseOid(), ct_op->GetNamespaceOid(), ct_op->GetTableOid(),
                          ct_op->GetTriggerName(), ct_op->GetTriggerFuncName(), ct_op->GetTriggerArgs(),
                          ct_op->GetTriggerColumns(), ct_op->GetTriggerWhen(), ct_op->GetTriggerType())
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());

  transformed->emplace_back(std::move(op));
}

LogicalCreateViewToPhysicalCreateView::LogicalCreateViewToPhysicalCreateView() {
  type_ = RuleType::CREATE_VIEW_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALCREATEVIEW);
}

bool LogicalCreateViewToPhysicalCreateView::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                  OptimizationContext *context) const {
  return true;
}

void LogicalCreateViewToPhysicalCreateView::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                                      std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                                      UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto cv_op = input->Contents()->GetContentsAs<LogicalCreateView>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalCreateView should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      CreateView::Make(cv_op->GetDatabaseOid(), cv_op->GetNamespaceOid(), cv_op->GetViewName(), cv_op->GetViewQuery())
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());

  transformed->emplace_back(std::move(op));
}

LogicalDropDatabaseToPhysicalDropDatabase::LogicalDropDatabaseToPhysicalDropDatabase() {
  type_ = RuleType::DROP_DATABASE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALDROPDATABASE);
}

bool LogicalDropDatabaseToPhysicalDropDatabase::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                      OptimizationContext *context) const {
  return true;
}

void LogicalDropDatabaseToPhysicalDropDatabase::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto dd_op = input->Contents()->GetContentsAs<LogicalDropDatabase>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalDropDatabase should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      DropDatabase::Make(dd_op->GetDatabaseOID()).RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(op));
}

LogicalDropTableToPhysicalDropTable::LogicalDropTableToPhysicalDropTable() {
  type_ = RuleType::DROP_TABLE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALDROPTABLE);
}

bool LogicalDropTableToPhysicalDropTable::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                OptimizationContext *context) const {
  return true;
}

void LogicalDropTableToPhysicalDropTable::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                                    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                                    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto dt_op = input->Contents()->GetContentsAs<LogicalDropTable>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalDropTable should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      DropTable::Make(dt_op->GetTableOID()).RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(op));
}

LogicalDropIndexToPhysicalDropIndex::LogicalDropIndexToPhysicalDropIndex() {
  type_ = RuleType::DROP_INDEX_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALDROPINDEX);
}

bool LogicalDropIndexToPhysicalDropIndex::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                OptimizationContext *context) const {
  return true;
}

void LogicalDropIndexToPhysicalDropIndex::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                                    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                                    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto di_op = input->Contents()->GetContentsAs<LogicalDropIndex>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalDropIndex should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      DropIndex::Make(di_op->GetIndexOID()).RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(op));
}

LogicalDropTriggerToPhysicalDropTrigger::LogicalDropTriggerToPhysicalDropTrigger() {
  type_ = RuleType::DROP_TRIGGER_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALDROPTRIGGER);
}

bool LogicalDropTriggerToPhysicalDropTrigger::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                    OptimizationContext *context) const {
  return true;
}

void LogicalDropTriggerToPhysicalDropTrigger::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto dt_op = input->Contents()->GetContentsAs<LogicalDropTrigger>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalDropTrigger should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      DropTrigger::Make(dt_op->GetDatabaseOid(), dt_op->GetTriggerOid(), dt_op->IsIfExists())
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(op));
}

LogicalDropNamespaceToPhysicalDropNamespace::LogicalDropNamespaceToPhysicalDropNamespace() {
  type_ = RuleType::DROP_NAMESPACE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALDROPNAMESPACE);
}

bool LogicalDropNamespaceToPhysicalDropNamespace::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                                        OptimizationContext *context) const {
  return true;
}

void LogicalDropNamespaceToPhysicalDropNamespace::Transform(
    common::ManagedPointer<AbstractOptimizerNode> input,
    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
    UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto dn_op = input->Contents()->GetContentsAs<LogicalDropNamespace>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalDropNamespace should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      DropNamespace::Make(dn_op->GetNamespaceOID()).RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(op));
}

LogicalDropViewToPhysicalDropView::LogicalDropViewToPhysicalDropView() {
  type_ = RuleType::DROP_VIEW_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALDROPVIEW);
}

bool LogicalDropViewToPhysicalDropView::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                              OptimizationContext *context) const {
  return true;
}

void LogicalDropViewToPhysicalDropView::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                                  std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                                  UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto dv_op = input->Contents()->GetContentsAs<LogicalDropView>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalDropView should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      DropView::Make(dv_op->GetDatabaseOid(), dv_op->GetViewOid(), dv_op->IsIfExists())
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(op));
}

LogicalAnalyzeToPhysicalAnalyze::LogicalAnalyzeToPhysicalAnalyze() {
  type_ = RuleType::ANALYZE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALANALYZE);
}

bool LogicalAnalyzeToPhysicalAnalyze::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                            OptimizationContext *context) const {
  return true;
}

void LogicalAnalyzeToPhysicalAnalyze::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                                std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                                UNUSED_ATTRIBUTE OptimizationContext *context) const {
  auto logical_op = input->Contents()->GetContentsAs<LogicalAnalyze>();
  NOISEPAGE_ASSERT(input->GetChildren().empty(), "LogicalAnalyze should have 0 children");

  auto op = std::make_unique<OperatorNode>(
      Analyze::Make(logical_op->GetDatabaseOid(), logical_op->GetTableOid(), logical_op->GetColumns())
          .RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()),
      std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context->GetOptimizerContext()->GetTxn());

  transformed->emplace_back(std::move(op));
}

}  // namespace noisepage::optimizer
