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
#include "optimizer/optimizer_defs.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/physical_operators.h"
#include "optimizer/properties.h"
#include "optimizer/rule_impls.h"
#include "optimizer/util.h"
#include "parser/expression_util.h"
#include "type/transient_value_factory.h"

namespace terrier::optimizer {

//===--------------------------------------------------------------------===//
// Transformation rules
//===--------------------------------------------------------------------===//

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinCommutativity
///////////////////////////////////////////////////////////////////////////////
InnerJoinCommutativity::InnerJoinCommutativity() {
  type_ = RuleType::INNER_JOIN_COMMUTE;

  auto left_child = new Pattern(OpType::LEAF);
  auto right_child = new Pattern(OpType::LEAF);
  match_pattern_ = new Pattern(OpType::LOGICALINNERJOIN);
  match_pattern_->AddChild(left_child);
  match_pattern_->AddChild(right_child);
}

bool InnerJoinCommutativity::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void InnerJoinCommutativity::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                       UNUSED_ATTRIBUTE OptimizeContext *context) const {
  auto join_op = input->GetOp().As<LogicalInnerJoin>();
  auto join_predicates = std::vector<AnnotatedExpression>(join_op->GetJoinPredicates());

  std::vector<OperatorExpression *> children = input->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "There should be two children");
  OPTIMIZER_LOG_TRACE("Reorder left child with op {0} and right child with op {1] for inner join",
                      children[0]->GetOp().GetName().c_str(), children[1]->GetOp().GetName().c_str());

  std::vector<OperatorExpression *> new_child{children[1]->Copy(), children[0]->Copy()};
  auto result_plan = new OperatorExpression(LogicalInnerJoin::Make(std::move(join_predicates)), std::move(new_child));
  transformed->push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinAssociativity
///////////////////////////////////////////////////////////////////////////////
InnerJoinAssociativity::InnerJoinAssociativity() {
  type_ = RuleType::INNER_JOIN_ASSOCIATE;

  // Create left nested join
  auto left_child = new Pattern(OpType::LOGICALINNERJOIN);
  left_child->AddChild(new Pattern(OpType::LEAF));
  left_child->AddChild(new Pattern(OpType::LEAF));

  auto right_child = new Pattern(OpType::LEAF);

  match_pattern_ = new Pattern(OpType::LOGICALINNERJOIN);
  match_pattern_->AddChild(left_child);
  match_pattern_->AddChild(right_child);
}

bool InnerJoinAssociativity::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void InnerJoinAssociativity::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                       OptimizeContext *context) const {
  // NOTE: Transforms (left JOIN middle) JOIN right -> left JOIN (middle JOIN
  // right) Variables are named accordingly to above transformation
  auto parent_join = input->GetOp().As<LogicalInnerJoin>();
  std::vector<OperatorExpression *> children = input->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "There should be 2 children");
  TERRIER_ASSERT(children[0]->GetOp().GetType() == OpType::LOGICALINNERJOIN, "Left should be join");
  TERRIER_ASSERT(children[0]->GetChildren().size() == 2, "Left join should have 2 children");

  auto child_join = children[0]->GetOp().As<LogicalInnerJoin>();
  auto left = children[0]->GetChildren()[0];
  auto middle = children[0]->GetChildren()[1];
  auto right = children[1];

  OPTIMIZER_LOG_DEBUG("Reordered join structured: (%s JOIN %s) JOIN %s", left->GetOp().GetName().c_str(),
                      middle->GetOp().GetName().c_str(), right->GetOp().GetName().c_str());

  // Get Alias sets
  auto &memo = context->GetMetadata()->GetMemo();
  auto middle_group_id = middle->GetOp().As<LeafOperator>()->GetOriginGroup();
  auto right_group_id = right->GetOp().As<LeafOperator>()->GetOriginGroup();

  const auto &middle_group_aliases_set = memo.GetGroupByID(middle_group_id)->GetTableAliases();
  const auto &right_group_aliases_set = memo.GetGroupByID(right_group_id)->GetTableAliases();

  // Union Predicates into single alias set for new child join
  std::unordered_set<std::string> right_join_aliases_set;
  right_join_aliases_set.insert(middle_group_aliases_set.begin(), middle_group_aliases_set.end());
  right_join_aliases_set.insert(right_group_aliases_set.begin(), right_group_aliases_set.end());

  // Redistribute predicates
  std::vector<AnnotatedExpression> predicates;
  auto parent_join_predicates = std::vector<AnnotatedExpression>(parent_join->GetJoinPredicates());
  auto child_join_predicates = std::vector<AnnotatedExpression>(child_join->GetJoinPredicates());
  predicates.insert(predicates.end(), parent_join_predicates.begin(), parent_join_predicates.end());
  predicates.insert(predicates.end(), child_join_predicates.begin(), child_join_predicates.end());

  std::vector<AnnotatedExpression> new_child_join_predicates;
  std::vector<AnnotatedExpression> new_parent_join_predicates;
  for (const auto &predicate : predicates) {
    if (Util::IsSubset(right_join_aliases_set, predicate.GetTableAliasSet())) {
      new_child_join_predicates.emplace_back(predicate);
    } else {
      new_parent_join_predicates.emplace_back(predicate);
    }
  }

  // Construct new child join operator
  std::vector<OperatorExpression *> child_children{middle->Copy(), right->Copy()};
  auto new_child_join =
      new OperatorExpression(LogicalInnerJoin::Make(std::move(new_child_join_predicates)), std::move(child_children));

  // Construct new parent join operator
  std::vector<OperatorExpression *> parent_children{left->Copy(), new_child_join};
  auto new_parent_join =
      new OperatorExpression(LogicalInnerJoin::Make(std::move(new_parent_join_predicates)), std::move(parent_children));

  transformed->push_back(new_parent_join);
}

//===--------------------------------------------------------------------===//
// Implementation rules
//===--------------------------------------------------------------------===//

///////////////////////////////////////////////////////////////////////////////
/// GetToTableFreeScan
///////////////////////////////////////////////////////////////////////////////
GetToTableFreeScan::GetToTableFreeScan() {
  type_ = RuleType::GET_TO_DUMMY_SCAN;
  match_pattern_ = new Pattern(OpType::LOGICALGET);
}

bool GetToTableFreeScan::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  const auto get = plan->GetOp().As<LogicalGet>();
  return get->GetTableOid() == catalog::INVALID_TABLE_OID;
}

void GetToTableFreeScan::Transform(UNUSED_ATTRIBUTE OperatorExpression *input,
                                   std::vector<OperatorExpression *> *transformed,
                                   UNUSED_ATTRIBUTE OptimizeContext *context) const {
  auto result_plan = new OperatorExpression(TableFreeScan::Make(), {});
  transformed->push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// GetToSeqScan
///////////////////////////////////////////////////////////////////////////////
GetToSeqScan::GetToSeqScan() {
  type_ = RuleType::GET_TO_SEQ_SCAN;
  match_pattern_ = new Pattern(OpType::LOGICALGET);
}

bool GetToSeqScan::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  const auto get = plan->GetOp().As<LogicalGet>();
  return get->GetTableOid() != catalog::INVALID_TABLE_OID;
}

void GetToSeqScan::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                             UNUSED_ATTRIBUTE OptimizeContext *context) const {
  TERRIER_ASSERT(input->GetChildren().empty(), "Get should have no children");
  const auto get = input->GetOp().As<LogicalGet>();

  // Need to copy because SeqScan uses std::move
  auto db_oid = get->GetDatabaseOid();
  auto ns_oid = get->GetNamespaceOid();
  auto tbl_oid = get->GetTableOid();
  auto tbl_alias = std::string(get->GetTableAlias());
  auto preds = std::vector<AnnotatedExpression>(get->GetPredicates());
  auto is_update = get->GetIsForUpdate();
  auto result_plan =
      new OperatorExpression(SeqScan::Make(db_oid, ns_oid, tbl_oid, std::move(preds), tbl_alias, is_update), {});
  transformed->push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// GetToIndexScan
///////////////////////////////////////////////////////////////////////////////
GetToIndexScan::GetToIndexScan() {
  type_ = RuleType::GET_TO_INDEX_SCAN;
  match_pattern_ = new Pattern(OpType::LOGICALGET);
}

bool GetToIndexScan::Check(OperatorExpression *plan, OptimizeContext *context) const {
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

  auto *accessor = context->GetMetadata()->GetCatalogAccessor();
  return !accessor->GetIndexOids(get->GetTableOid()).empty();
}

void GetToIndexScan::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                               UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const auto get = input->GetOp().As<LogicalGet>();
  TERRIER_ASSERT(input->GetChildren().empty(), "Get should have no children");

  auto db_oid = get->GetDatabaseOid();
  auto ns_oid = get->GetNamespaceOid();
  bool is_update = get->GetIsForUpdate();
  auto *accessor = context->GetMetadata()->GetCatalogAccessor();

  auto sort = context->GetRequiredProperties()->GetPropertyOfType(PropertyType::SORT);
  std::vector<catalog::col_oid_t> sort_col_ids;
  if (sort != nullptr) {
    // Check if can satisfy sort property with an index
    auto sort_prop = sort->As<PropertySort>();
    if (IndexUtil::CheckSortProperty(sort_prop)) {
      auto indexes = accessor->GetIndexOids(get->GetTableOid());
      for (auto index : indexes) {
        if (IndexUtil::SatisfiesSortWithIndex(sort_prop, get->GetTableOid(), index, accessor)) {
          std::vector<AnnotatedExpression> preds = get->GetPredicates();
          std::string tbl_alias = std::string(get->GetTableAlias());
          auto result = new OperatorExpression(
              IndexScan::Make(db_oid, ns_oid, index, std::move(preds), tbl_alias, is_update, {}, {}, {}), {});
          transformed->push_back(result);
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
      if (IndexUtil::SatisfiesPredicateWithIndex(get->GetTableOid(), index, &metadata, &output, accessor)) {
        std::vector<AnnotatedExpression> preds = get->GetPredicates();
        std::string tbl_alias = std::string(get->GetTableAlias());

        // Consider making IndexScan take in IndexUtilMetadata
        // instead to wrap all these vectors?
        auto result = new OperatorExpression(
            IndexScan::Make(db_oid, ns_oid, index, std::move(preds), std::move(tbl_alias), is_update,
                            std::move(output.GetPredicateColumnIds()), std::move(output.GetPredicateExprTypes()),
                            std::move(output.GetPredicateValues())),
            {});
        transformed->push_back(result);
      }
    }
  }
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalQueryDerivedGetToPhysical
///////////////////////////////////////////////////////////////////////////////
LogicalQueryDerivedGetToPhysical::LogicalQueryDerivedGetToPhysical() {
  type_ = RuleType::QUERY_DERIVED_GET_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALQUERYDERIVEDGET);

  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalQueryDerivedGetToPhysical::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void LogicalQueryDerivedGetToPhysical::Transform(OperatorExpression *input,
                                                 std::vector<OperatorExpression *> *transformed,
                                                 UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const auto get = input->GetOp().As<LogicalQueryDerivedGet>();

  auto tbl_alias = std::string(get->GetTableAlias());
  std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> expr_map;
  expr_map = get->GetAliasToExprMap();

  auto input_child = input->GetChildren()[0];
  auto result_plan = new OperatorExpression(QueryDerivedScan::Make(tbl_alias, std::move(expr_map)), {input_child});
  transformed->push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalExternalFileGetToPhysical
///////////////////////////////////////////////////////////////////////////////
LogicalExternalFileGetToPhysical::LogicalExternalFileGetToPhysical() {
  type_ = RuleType::EXTERNAL_FILE_GET_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALEXTERNALFILEGET);
}

bool LogicalExternalFileGetToPhysical::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalExternalFileGetToPhysical::Transform(OperatorExpression *input,
                                                 std::vector<OperatorExpression *> *transformed,
                                                 UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const auto *get = input->GetOp().As<LogicalExternalFileGet>();
  TERRIER_ASSERT(input->GetChildren().empty(), "ExternalFileScan should have no children");

  auto format = get->GetFormat();
  const auto &filename = get->GetFilename();
  auto delimiter = get->GetDelimiter();
  auto quote = get->GetQuote();
  auto escape = get->GetEscape();
  auto result_plan = new OperatorExpression(ExternalFileScan::Make(format, filename, delimiter, quote, escape), {});
  transformed->push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalDeleteToPhysical
///////////////////////////////////////////////////////////////////////////////
LogicalDeleteToPhysical::LogicalDeleteToPhysical() {
  type_ = RuleType::DELETE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALDELETE);
  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalDeleteToPhysical::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalDeleteToPhysical::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                        UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const auto del = input->GetOp().As<LogicalDelete>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalDelete should have 1 child");

  auto child = input->GetChildren()[0]->Copy();
  auto result = new OperatorExpression(
      Delete::Make(del->GetDatabaseOid(), del->GetNamespaceOid(), del->GetTableAlias(), del->GetTableOid()), {child});
  transformed->push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalUpdateToPhysical
///////////////////////////////////////////////////////////////////////////////
LogicalUpdateToPhysical::LogicalUpdateToPhysical() {
  type_ = RuleType::UPDATE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALUPDATE);

  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalUpdateToPhysical::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalUpdateToPhysical::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                        UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const auto update_op = input->GetOp().As<LogicalUpdate>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalUpdate should have 1 child");
  auto child = input->GetChildren()[0]->Copy();

  std::vector<common::ManagedPointer<parser::UpdateClause>> cls = update_op->GetUpdateClauses();
  auto result =
      new OperatorExpression(Update::Make(update_op->GetDatabaseOid(), update_op->GetNamespaceOid(),
                                          update_op->GetTableAlias(), update_op->GetTableOid(), std::move(cls)),
                             {child});
  transformed->push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalInsertToPhysical
///////////////////////////////////////////////////////////////////////////////
LogicalInsertToPhysical::LogicalInsertToPhysical() {
  type_ = RuleType::INSERT_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALINSERT);
}

bool LogicalInsertToPhysical::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalInsertToPhysical::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                        UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const auto insert_op = input->GetOp().As<LogicalInsert>();
  TERRIER_ASSERT(input->GetChildren().empty(), "LogicalInsert should have 0 children");

  // TODO(wz2): For now any insert will update all indexes
  auto *accessor = context->GetMetadata()->GetCatalogAccessor();
  auto tbl_oid = insert_op->GetTableOid();
  auto indexes = accessor->GetIndexOids(tbl_oid);

  std::vector<catalog::col_oid_t> cols(insert_op->GetColumns());
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> vals = *(insert_op->GetValues());
  auto result = new OperatorExpression(
      Insert::Make(insert_op->GetDatabaseOid(), insert_op->GetNamespaceOid(), insert_op->GetTableOid(), std::move(cols),
                   std::move(vals), std::move(indexes)),
      {});
  transformed->push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalInsertSelectToPhysical
///////////////////////////////////////////////////////////////////////////////
LogicalInsertSelectToPhysical::LogicalInsertSelectToPhysical() {
  type_ = RuleType::INSERT_SELECT_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALINSERTSELECT);

  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalInsertSelectToPhysical::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void LogicalInsertSelectToPhysical::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                              UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const auto insert_op = input->GetOp().As<LogicalInsertSelect>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalInsertSelect should have 1 child");

  // For now, insert any tuple will modify indexes
  auto *accessor = context->GetMetadata()->GetCatalogAccessor();
  auto tbl_oid = insert_op->GetTableOid();
  auto indexes = accessor->GetIndexOids(tbl_oid);

  auto child = input->GetChildren()[0]->Copy();
  auto op = new OperatorExpression(InsertSelect::Make(insert_op->GetDatabaseOid(), insert_op->GetNamespaceOid(),
                                                      insert_op->GetTableOid(), std::move(indexes)),
                                   {child});
  transformed->push_back(op);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalAggregateAndGroupByToHashGroupBy
///////////////////////////////////////////////////////////////////////////////
LogicalGroupByToHashGroupBy::LogicalGroupByToHashGroupBy() {
  type_ = RuleType::AGGREGATE_TO_HASH_AGGREGATE;
  match_pattern_ = new Pattern(OpType::LOGICALAGGREGATEANDGROUPBY);

  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalGroupByToHashGroupBy::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  const auto agg_op = plan->GetOp().As<LogicalAggregateAndGroupBy>();
  return !agg_op->GetColumns().empty();
}

void LogicalGroupByToHashGroupBy::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                            UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const auto agg_op = input->GetOp().As<LogicalAggregateAndGroupBy>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalAggregateAndGroupBy should have 1 child");

  std::vector<common::ManagedPointer<parser::AbstractExpression>> cols = agg_op->GetColumns();
  std::vector<AnnotatedExpression> having = agg_op->GetHaving();

  auto child = input->GetChildren()[0]->Copy();
  auto result = new OperatorExpression(HashGroupBy::Make(std::move(cols), std::move(having)), {child});
  transformed->push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalAggregateToPhysical
///////////////////////////////////////////////////////////////////////////////
LogicalAggregateToPhysical::LogicalAggregateToPhysical() {
  type_ = RuleType::AGGREGATE_TO_PLAIN_AGGREGATE;
  match_pattern_ = new Pattern(OpType::LOGICALAGGREGATEANDGROUPBY);

  auto child = new Pattern(OpType::LEAF);
  match_pattern_->AddChild(child);
}

bool LogicalAggregateToPhysical::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  const auto agg_op = plan->GetOp().As<LogicalAggregateAndGroupBy>();
  return agg_op->GetColumns().empty();
}

void LogicalAggregateToPhysical::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                           UNUSED_ATTRIBUTE OptimizeContext *context) const {
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalAggregateAndGroupBy should have 1 child");

  auto child = input->GetChildren()[0]->Copy();
  auto result = new OperatorExpression(Aggregate::Make(), {child});
  transformed->push_back(result);
}

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinToInnerNLJoin
///////////////////////////////////////////////////////////////////////////////
InnerJoinToInnerNLJoin::InnerJoinToInnerNLJoin() {
  type_ = RuleType::INNER_JOIN_TO_NL_JOIN;

  auto left_child = new Pattern(OpType::LEAF);
  auto right_child = new Pattern(OpType::LEAF);

  // Initialize a pattern for optimizer to match
  match_pattern_ = new Pattern(OpType::LOGICALINNERJOIN);

  // Add node - we match join relation R and S
  match_pattern_->AddChild(left_child);
  match_pattern_->AddChild(right_child);
}

bool InnerJoinToInnerNLJoin::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void InnerJoinToInnerNLJoin::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                       UNUSED_ATTRIBUTE OptimizeContext *context) const {
  // first build an expression representing hash join
  const auto inner_join = input->GetOp().As<LogicalInnerJoin>();

  const auto &children = input->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "Inner Join should have two child");
  auto left_group_id = children[0]->GetOp().As<LeafOperator>()->GetOriginGroup();
  auto right_group_id = children[1]->GetOp().As<LeafOperator>()->GetOriginGroup();
  auto &left_group_alias = context->GetMetadata()->GetMemo().GetGroupByID(left_group_id)->GetTableAliases();
  auto &right_group_alias = context->GetMetadata()->GetMemo().GetGroupByID(right_group_id)->GetTableAliases();
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys;

  std::vector<AnnotatedExpression> join_preds = inner_join->GetJoinPredicates();
  Util::ExtractEquiJoinKeys(join_preds, &left_keys, &right_keys, left_group_alias, right_group_alias);

  TERRIER_ASSERT(right_keys.size() == left_keys.size(), "# left/right keys should equal");
  std::vector<OperatorExpression *> child = {children[0]->Copy(), children[1]->Copy()};
  auto result_plan = new OperatorExpression(
      InnerNLJoin::Make(std::move(join_preds), std::move(left_keys), std::move(right_keys)), std::move(child));
  transformed->push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// InnerJoinToInnerHashJoin
///////////////////////////////////////////////////////////////////////////////
InnerJoinToInnerHashJoin::InnerJoinToInnerHashJoin() {
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

bool InnerJoinToInnerHashJoin::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void InnerJoinToInnerHashJoin::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                         UNUSED_ATTRIBUTE OptimizeContext *context) const {
  // first build an expression representing hash join
  const auto inner_join = input->GetOp().As<LogicalInnerJoin>();

  auto children = input->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "Inner Join should have two child");
  auto left_group_id = children[0]->GetOp().As<LeafOperator>()->GetOriginGroup();
  auto right_group_id = children[1]->GetOp().As<LeafOperator>()->GetOriginGroup();
  auto &left_group_alias = context->GetMetadata()->GetMemo().GetGroupByID(left_group_id)->GetTableAliases();
  auto &right_group_alias = context->GetMetadata()->GetMemo().GetGroupByID(right_group_id)->GetTableAliases();
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys;

  std::vector<AnnotatedExpression> join_preds = inner_join->GetJoinPredicates();
  Util::ExtractEquiJoinKeys(join_preds, &left_keys, &right_keys, left_group_alias, right_group_alias);

  TERRIER_ASSERT(right_keys.size() == left_keys.size(), "# left/right keys should equal");
  std::vector<OperatorExpression *> child = {children[0]->Copy(), children[1]->Copy()};
  if (!left_keys.empty()) {
    auto result_plan = new OperatorExpression(
        InnerHashJoin::Make(std::move(join_preds), std::move(left_keys), std::move(right_keys)), std::move(child));
    transformed->push_back(result_plan);
  }
}

///////////////////////////////////////////////////////////////////////////////
/// ImplementLimit
///////////////////////////////////////////////////////////////////////////////
ImplementLimit::ImplementLimit() {
  type_ = RuleType::IMPLEMENT_LIMIT;

  match_pattern_ = new Pattern(OpType::LOGICALLIMIT);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
}

bool ImplementLimit::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void ImplementLimit::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                               OptimizeContext *context) const {
  (void)context;
  const auto limit_op = input->GetOp().As<LogicalLimit>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalLimit should have 1 child");

  std::vector<common::ManagedPointer<parser::AbstractExpression>> sorts = limit_op->GetSortExpressions();
  std::vector<OrderByOrderingType> types = limit_op->GetSortDirections();
  auto child = input->GetChildren()[0]->Copy();
  auto result_plan = new OperatorExpression(
      Limit::Make(limit_op->GetOffset(), limit_op->GetLimit(), std::move(sorts), std::move(types)), {child});
  transformed->push_back(result_plan);
}

///////////////////////////////////////////////////////////////////////////////
/// LogicalExport to Physical Export
///////////////////////////////////////////////////////////////////////////////
LogicalExportToPhysicalExport::LogicalExportToPhysicalExport() {
  type_ = RuleType::EXPORT_EXTERNAL_FILE_TO_PHYSICAL;
  match_pattern_ = new Pattern(OpType::LOGICALEXPORTEXTERNALFILE);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
}

bool LogicalExportToPhysicalExport::Check(OperatorExpression *plan, OptimizeContext *context) const { return true; }

void LogicalExportToPhysicalExport::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                              UNUSED_ATTRIBUTE OptimizeContext *context) const {
  const auto *export_op = input->GetOp().As<LogicalExportExternalFile>();
  TERRIER_ASSERT(input->GetChildren().size() == 1, "LogicalExport should have 1 child");

  std::string file = std::string(export_op->GetFilename());
  auto child = input->GetChildren()[0]->Copy();
  auto result_plan = new OperatorExpression(
      ExportExternalFile::Make(export_op->GetFormat(), std::move(file), export_op->GetDelimiter(),
                               export_op->GetQuote(), export_op->GetEscape()),
      {child});
  transformed->push_back(result_plan);
}

//===--------------------------------------------------------------------===//
// Rewrite rules
//===--------------------------------------------------------------------===//

///////////////////////////////////////////////////////////////////////////////
/// PushFilterThroughJoin
///////////////////////////////////////////////////////////////////////////////
PushFilterThroughJoin::PushFilterThroughJoin() {
  type_ = RuleType::PUSH_FILTER_THROUGH_JOIN;

  // Make join for pattern matching
  match_pattern_ = new Pattern(OpType::LOGICALINNERJOIN);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
}

bool PushFilterThroughJoin::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void PushFilterThroughJoin::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                      UNUSED_ATTRIBUTE OptimizeContext *context) const {
  OPTIMIZER_LOG_TRACE("PushFilterThroughJoin::Transform");

  auto &memo = context->GetMetadata()->GetMemo();
  auto join_op_expr = input;
  auto &join_children = join_op_expr->GetChildren();
  auto left_group_id = join_children[0]->GetOp().As<LeafOperator>()->GetOriginGroup();
  auto right_group_id = join_children[1]->GetOp().As<LeafOperator>()->GetOriginGroup();

  const auto &left_group_aliases_set = memo.GetGroupByID(left_group_id)->GetTableAliases();
  const auto &right_group_aliases_set = memo.GetGroupByID(right_group_id)->GetTableAliases();
  auto &predicates = input->GetOp().As<LogicalInnerJoin>()->GetJoinPredicates();

  std::vector<AnnotatedExpression> left_predicates;
  std::vector<AnnotatedExpression> right_predicates;
  std::vector<AnnotatedExpression> join_predicates;

  // Loop over all predicates, check each of them if they can be pushed down to
  // either the left child or the right child to be evaluated
  // All predicates in this loop follow conjunction relationship because we
  // already extract these predicates from the original.
  // E.g. An expression (test.a = test1.b and test.a = 5) would become
  // {test.a = test1.b, test.a = 5}
  for (auto &predicate : predicates) {
    if (Util::IsSubset(left_group_aliases_set, predicate.GetTableAliasSet())) {
      left_predicates.emplace_back(predicate);
    } else if (Util::IsSubset(right_group_aliases_set, predicate.GetTableAliasSet())) {
      right_predicates.emplace_back(predicate);
    } else {
      join_predicates.emplace_back(predicate);
    }
  }

  OperatorExpression *left_branch = nullptr;
  OperatorExpression *right_branch = nullptr;
  bool pushed_down = false;

  // Construct left filter if any
  if (!left_predicates.empty()) {
    pushed_down = true;
    auto left_child = join_op_expr->GetChildren()[0]->Copy();
    left_branch = new OperatorExpression(LogicalFilter::Make(std::move(left_predicates)), {left_child});
  } else {
    left_branch = join_op_expr->GetChildren()[0]->Copy();
  }

  // Construct right filter if any
  if (!right_predicates.empty()) {
    pushed_down = true;
    auto right_child = join_op_expr->GetChildren()[1]->Copy();
    right_branch = new OperatorExpression(LogicalFilter::Make(std::move(right_predicates)), {right_child});
  } else {
    right_branch = join_op_expr->GetChildren()[1]->Copy();
  }

  // Only construct the output if either filter has been pushed down
  if (pushed_down) {
    auto pre_join_predicate = join_op_expr->GetOp().As<LogicalInnerJoin>()->GetJoinPredicates();
    join_predicates.insert(join_predicates.end(), pre_join_predicate.begin(), pre_join_predicate.end());
    auto output = new OperatorExpression(LogicalInnerJoin::Make(std::move(join_predicates)), {left_branch, right_branch});
    transformed->push_back(output);
  }
}

///////////////////////////////////////////////////////////////////////////////
/// PushFilterThroughAggregation
///////////////////////////////////////////////////////////////////////////////
PushFilterThroughAggregation::PushFilterThroughAggregation() {
  type_ = RuleType::PUSH_FILTER_THROUGH_AGGREGATION;

  auto child = new Pattern(OpType::LOGICALAGGREGATEANDGROUPBY);
  child->AddChild(new Pattern(OpType::LEAF));

  // Initialize a pattern for optimizer to match
  match_pattern_ = new Pattern(OpType::LOGICALFILTER);

  // Add node - we match (filter)->(aggregation)->(leaf)
  match_pattern_->AddChild(child);
}

bool PushFilterThroughAggregation::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void PushFilterThroughAggregation::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                             UNUSED_ATTRIBUTE OptimizeContext *context) const {
  OPTIMIZER_LOG_TRACE("PushFilterThroughAggregation::Transform");
  auto aggregation_op = input->GetChildren()[0]->GetOp().As<LogicalAggregateAndGroupBy>();

  auto &predicates = input->GetOp().As<LogicalFilter>()->GetPredicates();
  std::vector<AnnotatedExpression> embedded_predicates;
  std::vector<AnnotatedExpression> pushdown_predicates;

  for (auto &predicate : predicates) {
    std::vector<common::ManagedPointer<parser::AbstractExpression>> aggr_exprs;
    parser::ExpressionUtil::GetAggregateExprs(&aggr_exprs, predicate.GetExpr());

    // No aggr_expr in the predicate -- pushdown to evaluate
    if (aggr_exprs.empty()) {
      pushdown_predicates.emplace_back(predicate);
    } else {
      embedded_predicates.emplace_back(predicate);
    }
  }

  // Add original having predicates
  for (auto &predicate : aggregation_op->GetHaving()) {
    embedded_predicates.emplace_back(predicate);
  }

  // Original leaf
  auto leaf = input->GetChildren()[0]->GetChildren()[0]->Copy();
  OperatorExpression *pushdown = nullptr;

  // Construct filter if needed
  if (!pushdown_predicates.empty()) {
    pushdown = new OperatorExpression(LogicalFilter::Make(std::move(pushdown_predicates)), {leaf});
  }

  std::vector<common::ManagedPointer<parser::AbstractExpression>> cols = aggregation_op->GetColumns();

  auto agg_child = pushdown != nullptr ? pushdown : leaf;
  auto output = new OperatorExpression(
      LogicalAggregateAndGroupBy::Make(std::move(cols), std::move(embedded_predicates)), {agg_child});
  transformed->push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// CombineConsecutiveFilter
///////////////////////////////////////////////////////////////////////////////
CombineConsecutiveFilter::CombineConsecutiveFilter() {
  type_ = RuleType::COMBINE_CONSECUTIVE_FILTER;

  match_pattern_ = new Pattern(OpType::LOGICALFILTER);
  auto child = new Pattern(OpType::LOGICALFILTER);
  child->AddChild(new Pattern(OpType::LEAF));

  match_pattern_->AddChild(child);
}

bool CombineConsecutiveFilter::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void CombineConsecutiveFilter::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                         UNUSED_ATTRIBUTE OptimizeContext *context) const {
  auto child_filter = input->GetChildren()[0];
  std::vector<AnnotatedExpression> root_predicates = input->GetOp().As<LogicalFilter>()->GetPredicates();
  std::vector<AnnotatedExpression> child_predicates = child_filter->GetOp().As<LogicalFilter>()->GetPredicates();
  root_predicates.insert(root_predicates.end(), child_predicates.begin(), child_predicates.end());

  auto child = child_filter->GetChildren()[0]->Copy();
  auto output = new OperatorExpression(LogicalFilter::Make(std::move(root_predicates)), {child});
  transformed->push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// EmbedFilterIntoGet
///////////////////////////////////////////////////////////////////////////////
EmbedFilterIntoGet::EmbedFilterIntoGet() {
  type_ = RuleType::EMBED_FILTER_INTO_GET;

  match_pattern_ = new Pattern(OpType::LOGICALFILTER);
  auto child = new Pattern(OpType::LOGICALGET);

  match_pattern_->AddChild(child);
}

bool EmbedFilterIntoGet::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;
  return true;
}

void EmbedFilterIntoGet::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                   UNUSED_ATTRIBUTE OptimizeContext *context) const {
  auto get = input->GetChildren()[0]->GetOp().As<LogicalGet>();
  std::string tbl_alias = std::string(get->GetTableAlias());
  std::vector<AnnotatedExpression> predicates = input->GetOp().As<LogicalFilter>()->GetPredicates();
  auto output =
      new OperatorExpression(LogicalGet::Make(get->GetDatabaseOid(), get->GetNamespaceOid(), get->GetTableOid(),
                                              predicates, tbl_alias, get->GetIsForUpdate()),
                             {});
  transformed->push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// MarkJoinToInnerJoin
///////////////////////////////////////////////////////////////////////////////
MarkJoinToInnerJoin::MarkJoinToInnerJoin() {
  type_ = RuleType::MARK_JOIN_GET_TO_INNER_JOIN;

  match_pattern_ = new Pattern(OpType::LOGICALMARKJOIN);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
}

int MarkJoinToInnerJoin::Promise(GroupExpression *group_expr, OptimizeContext *context) const {
  (void)context;
  return static_cast<int>(UnnestPromise::Low);
}

bool MarkJoinToInnerJoin::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;

  UNUSED_ATTRIBUTE auto &children = plan->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "LogicalMarkJoin should have 2 children");
  return true;
}

void MarkJoinToInnerJoin::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                    UNUSED_ATTRIBUTE OptimizeContext *context) const {
  OPTIMIZER_LOG_TRACE("MarkJoinToInnerJoin::Transform");
  UNUSED_ATTRIBUTE auto mark_join = input->GetOp().As<LogicalMarkJoin>();
  TERRIER_ASSERT(mark_join->GetJoinPredicates().empty(), "MarkJoin should have 0 predicates");

  auto &join_children = input->GetChildren();
  std::vector<OperatorExpression *> child = {join_children[0]->Copy(), join_children[1]->Copy()};
  auto output = new OperatorExpression(LogicalInnerJoin::Make(), std::move(child));
  transformed->push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// SingleJoinGetToInnerJoin
///////////////////////////////////////////////////////////////////////////////
SingleJoinToInnerJoin::SingleJoinToInnerJoin() {
  type_ = RuleType::MARK_JOIN_GET_TO_INNER_JOIN;

  match_pattern_ = new Pattern(OpType::LOGICALSINGLEJOIN);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
}

int SingleJoinToInnerJoin::Promise(GroupExpression *group_expr, OptimizeContext *context) const {
  (void)context;
  return static_cast<int>(UnnestPromise::Low);
}

bool SingleJoinToInnerJoin::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;

  UNUSED_ATTRIBUTE auto &children = plan->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "SingleJoin should have 2 children");
  return true;
}

void SingleJoinToInnerJoin::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                      UNUSED_ATTRIBUTE OptimizeContext *context) const {
  OPTIMIZER_LOG_TRACE("SingleJoinToInnerJoin::Transform");
  UNUSED_ATTRIBUTE auto single_join = input->GetOp().As<LogicalSingleJoin>();
  TERRIER_ASSERT(single_join->GetJoinPredicates().empty(), "SingleJoin should have no predicates");

  auto &join_children = input->GetChildren();
  std::vector<OperatorExpression *> child = {join_children[0]->Copy(), join_children[1]->Copy()};
  auto output = new OperatorExpression(LogicalInnerJoin::Make(), std::move(child));
  transformed->push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// PullFilterThroughMarkJoin
///////////////////////////////////////////////////////////////////////////////
PullFilterThroughMarkJoin::PullFilterThroughMarkJoin() {
  type_ = RuleType::PULL_FILTER_THROUGH_MARK_JOIN;

  match_pattern_ = new Pattern(OpType::LOGICALMARKJOIN);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
  auto filter = new Pattern(OpType::LOGICALFILTER);
  filter->AddChild(new Pattern(OpType::LEAF));
  match_pattern_->AddChild(filter);
}

int PullFilterThroughMarkJoin::Promise(GroupExpression *group_expr, OptimizeContext *context) const {
  (void)context;
  return static_cast<int>(UnnestPromise::High);
}

bool PullFilterThroughMarkJoin::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;

  auto &children = plan->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "MarkJoin should have two children");

  UNUSED_ATTRIBUTE auto &r_grandchildren = children[0]->GetChildren();
  TERRIER_ASSERT(r_grandchildren.size() == 1, "Filter should have only 1 child");
  return true;
}

void PullFilterThroughMarkJoin::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                          UNUSED_ATTRIBUTE OptimizeContext *context) const {
  OPTIMIZER_LOG_TRACE("PullFilterThroughMarkJoin::Transform");
  UNUSED_ATTRIBUTE auto mark_join = input->GetOp().As<LogicalMarkJoin>();
  TERRIER_ASSERT(mark_join->GetJoinPredicates().empty(), "MarkJoin should have zero children");

  auto &join_children = input->GetChildren();
  auto &filter_children = join_children[1]->GetChildren();

  std::vector<OperatorExpression *> children{join_children[0]->Copy(), filter_children[0]->Copy()};
  auto join = new OperatorExpression(Operator(input->GetOp()), std::move(children));
  auto output = new OperatorExpression(Operator(join_children[1]->GetOp()), {join});
  transformed->push_back(output);
}

///////////////////////////////////////////////////////////////////////////////
/// PullFilterThroughAggregation
///////////////////////////////////////////////////////////////////////////////
PullFilterThroughAggregation::PullFilterThroughAggregation() {
  type_ = RuleType::PULL_FILTER_THROUGH_AGGREGATION;

  auto filter = new Pattern(OpType::LOGICALFILTER);
  filter->AddChild(new Pattern(OpType::LEAF));
  match_pattern_ = new Pattern(OpType::LOGICALAGGREGATEANDGROUPBY);
  match_pattern_->AddChild(filter);
}

int PullFilterThroughAggregation::Promise(GroupExpression *group_expr, OptimizeContext *context) const {
  (void)context;
  return static_cast<int>(UnnestPromise::High);
}

bool PullFilterThroughAggregation::Check(OperatorExpression *plan, OptimizeContext *context) const {
  (void)context;
  (void)plan;

  auto &children = plan->GetChildren();
  TERRIER_ASSERT(children.size() == 1, "AggregateAndGroupBy should have 1 child");

  UNUSED_ATTRIBUTE auto &r_grandchildren = children[1]->GetChildren();
  TERRIER_ASSERT(r_grandchildren.size() == 1, "Filter should have 1 child");
  return true;
}

void PullFilterThroughAggregation::Transform(OperatorExpression *input, std::vector<OperatorExpression *> *transformed,
                                             UNUSED_ATTRIBUTE OptimizeContext *context) const {
  OPTIMIZER_LOG_TRACE("PullFilterThroughAggregation::Transform");
  auto &memo = context->GetMetadata()->GetMemo();
  auto &filter_expr = input->GetChildren()[0];
  auto child_group_id = filter_expr->GetChildren()[0]->GetOp().As<LeafOperator>()->GetOriginGroup();
  const auto &child_group_aliases_set = memo.GetGroupByID(child_group_id)->GetTableAliases();
  auto &predicates = filter_expr->GetOp().As<LogicalFilter>()->GetPredicates();

  std::vector<AnnotatedExpression> correlated_predicates;
  std::vector<AnnotatedExpression> normal_predicates;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> new_groupby_cols;
  for (auto &predicate : predicates) {
    if (Util::IsSubset(child_group_aliases_set, predicate.GetTableAliasSet())) {
      normal_predicates.emplace_back(predicate);
    } else {
      // Correlated predicate, already in the form of
      // (outer_relation.a = (expr))
      correlated_predicates.emplace_back(predicate);
      auto root_expr = predicate.GetExpr();
      if (root_expr->GetChild(0)->GetDepth() < root_expr->GetDepth()) {
        new_groupby_cols.emplace_back(root_expr->GetChild(1).Get());
      } else {
        new_groupby_cols.emplace_back(root_expr->GetChild(0).Get());
      }
    }
  }

  if (correlated_predicates.empty()) {
    // No need to pull
    return;
  }

  auto aggregation = input->GetOp().As<LogicalAggregateAndGroupBy>();
  for (auto &col : aggregation->GetColumns()) {
    new_groupby_cols.emplace_back(col);
  }

  auto aggr_child = filter_expr->GetChildren()[0]->Copy();
  if (!normal_predicates.empty()) {
    auto new_filter = new OperatorExpression(LogicalFilter::Make(std::move(normal_predicates)), {aggr_child});
    aggr_child = new_filter;
  }

  std::vector<AnnotatedExpression> new_having = aggregation->GetHaving();
  auto new_aggr = new OperatorExpression(
      LogicalAggregateAndGroupBy::Make(std::move(new_groupby_cols), std::move(new_having)), {aggr_child});

  auto output = new OperatorExpression(LogicalFilter::Make(std::move(correlated_predicates)), {new_aggr});
  transformed->push_back(output);
}

}  // namespace terrier::optimizer
