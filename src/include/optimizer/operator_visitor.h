#pragma once

namespace noisepage::optimizer {

class LeafOperator;
class TableFreeScan;
class SeqScan;
class IndexScan;
class ExternalFileScan;
class QueryDerivedScan;
class OrderBy;
class Limit;
class InnerIndexJoin;
class InnerNLJoin;
class LeftNLJoin;
class RightNLJoin;
class OuterNLJoin;
class InnerHashJoin;
class LeftHashJoin;
class LeftSemiHashJoin;
class RightHashJoin;
class OuterHashJoin;
class Insert;
class InsertSelect;
class Delete;
class Update;
class HashGroupBy;
class SortGroupBy;
class Aggregate;
class ExportExternalFile;
class CreateDatabase;
class CreateFunction;
class CreateIndex;
class CreateTable;
class CreateNamespace;
class CreateTrigger;
class CreateView;
class DropDatabase;
class DropTable;
class DropIndex;
class DropNamespace;
class DropTrigger;
class DropView;
class Analyze;
class LogicalGet;
class LogicalExternalFileGet;
class LogicalQueryDerivedGet;
class LogicalFilter;
class LogicalProjection;
class LogicalMarkJoin;
class LogicalSingleJoin;
class LogicalDependentJoin;
class LogicalInnerJoin;
class LogicalLeftJoin;
class LogicalRightJoin;
class LogicalOuterJoin;
class LogicalSemiJoin;
class LogicalAggregateAndGroupBy;
class LogicalInsert;
class LogicalInsertSelect;
class LogicalDelete;
class LogicalUpdate;
class LogicalLimit;
class LogicalExportExternalFile;
class LogicalCreateDatabase;
class LogicalCreateFunction;
class LogicalCreateIndex;
class LogicalCreateTable;
class LogicalCreateNamespace;
class LogicalCreateTrigger;
class LogicalCreateView;
class LogicalDropDatabase;
class LogicalDropTable;
class LogicalDropIndex;
class LogicalDropNamespace;
class LogicalDropTrigger;
class LogicalDropView;
class LogicalAnalyze;

/**
 * Utility class for visitor pattern
 */
class OperatorVisitor {
 public:
  virtual ~OperatorVisitor() = default;

  /**
   * Visit a LeafOperator operator
   * @param leaf operator
   */
  virtual void Visit(const LeafOperator *leaf) {}

  /**
   * Visit a TableFreeScan operator
   * @param table_free_scan operator
   */
  virtual void Visit(const TableFreeScan *table_free_scan) {}

  /**
   * Visit a SeqScan operator
   * @param seq_scan operator
   */
  virtual void Visit(const SeqScan *seq_scan) {}

  /**
   * Visit a IndexScan operator
   * @param index_scan operator
   */
  virtual void Visit(const IndexScan *index_scan) {}

  /**
   * Visit a ExternalFileScan operator
   * @param ext_file_scan operator
   */
  virtual void Visit(const ExternalFileScan *ext_file_scan) {}

  /**
   * Visit a QueryDerivedScan operator
   * @param query_derived_scan operator
   */
  virtual void Visit(const QueryDerivedScan *query_derived_scan) {}

  /**
   * Visit a OrderBy operator
   * @param order_by operator
   */
  virtual void Visit(const OrderBy *order_by) {}

  /**
   * Visit a Limit operator
   * @param limit operator
   */
  virtual void Visit(const Limit *limit) {}

  /**
   * Visit a InnerNLJoin operator
   * @param index_join operator
   */
  virtual void Visit(const InnerIndexJoin *index_join) {}

  /**
   * Visit a InnerNLJoin operator
   * @param inner_join operator
   */
  virtual void Visit(const InnerNLJoin *inner_join) {}

  /**
   * Visit a LeftNLJoin operator
   * @param left_nl_join operator
   */
  virtual void Visit(const LeftNLJoin *left_nl_join) {}

  /**
   * Visit a RightNLJoin operator
   * @param right_nl_join operator
   */
  virtual void Visit(const RightNLJoin *right_nl_join) {}

  /**
   * Visit a OuterNLJoin operator
   * @param outer_nl_join operator
   */
  virtual void Visit(const OuterNLJoin *outer_nl_join) {}

  /**
   * Visit a InnerHashJoin operator
   * @param inner_hash_join operator
   */
  virtual void Visit(const InnerHashJoin *inner_hash_join) {}

  /**
   * Visit a LeftHashJoin operator
   * @param left_hash_join operator
   */
  virtual void Visit(const LeftHashJoin *left_hash_join) {}

  /**
   * Visit a RightHashJoin operator
   * @param right_hash_join operator
   */
  virtual void Visit(const RightHashJoin *right_hash_join) {}

  /**
   * Visit a OuterHashJoin operator
   * @param outer_hash_join operator
   */
  virtual void Visit(const OuterHashJoin *outer_hash_join) {}

  /**
   * Visit a LeftHashJoin operator
   * @param left_semi_hash_join operator
   */
  virtual void Visit(const LeftSemiHashJoin *left_semi_hash_join) {}

  /**
   * Visit a Insert operator
   * @param insert operator
   */
  virtual void Visit(const Insert *insert) {}

  /**
   * Visit a InsertSelect operator
   * @param insert_select operator
   */
  virtual void Visit(const InsertSelect *insert_select) {}

  /**
   * Visit a Delete operator
   * @param del operator
   */
  virtual void Visit(const Delete *del) {}

  /**
   * Visit a Update operator
   * @param update operator
   */
  virtual void Visit(const Update *update) {}

  /**
   * Visit a HashGroupBy operator
   * @param hash_group_by operator
   */
  virtual void Visit(const HashGroupBy *hash_group_by) {}

  /**
   * Visit a SortGroupBy operator
   * @param sort_group_by operator
   */
  virtual void Visit(const SortGroupBy *sort_group_by) {}

  /**
   * Visit a Aggregate operator
   * @param aggregate operator
   */
  virtual void Visit(const Aggregate *aggregate) {}

  /**
   * Visit a ExportExternalFile operator
   * @param export_ext_file operator
   */
  virtual void Visit(const ExportExternalFile *export_ext_file) {}

  /**
   * Visit a CreateDatabase operator
   * @param create_database operator
   */
  virtual void Visit(const CreateDatabase *create_database) {}

  /**
   * Visit a CreateFunction operator
   * @param create_function operator
   */
  virtual void Visit(const CreateFunction *create_function) {}

  /**
   * Visit a CreateIndex operator
   * @param create_index operator
   */
  virtual void Visit(const CreateIndex *create_index) {}

  /**
   * Visit a CreateTable operator
   * @param create_table operator
   */
  virtual void Visit(const CreateTable *create_table) {}

  /**
   * Visit a CreateNamespace operator
   * @param create_namespace operator
   */
  virtual void Visit(const CreateNamespace *create_namespace) {}  // NOLINT

  /**
   * Visit a CreateTrigger operator
   * @param create_trigger operator
   */
  virtual void Visit(const CreateTrigger *create_trigger) {}

  /**
   * Visit a CreateView operator
   * @param create_view operator
   */
  virtual void Visit(const CreateView *create_view) {}
  /**
   * Visit a DropDatabase operator
   * @param drop_database operator
   */
  virtual void Visit(const DropDatabase *drop_database) {}

  /**
   * Visit a DropTable operator
   * @param drop_table operator
   */
  virtual void Visit(const DropTable *drop_table) {}

  /**
   * Visit a DropIndex operator
   * @param drop_index operator
   */
  virtual void Visit(const DropIndex *drop_index) {}

  /**
   * Visit a DropNamespace operator
   * @param drop_namespace operator
   */
  virtual void Visit(const DropNamespace *drop_namespace) {}  // NOLINT

  /**
   * Visit a DropTrigger operator
   * @param drop_trigger operator
   */
  virtual void Visit(const DropTrigger *drop_trigger) {}

  /**
   * Visit a DropView operator
   * @param drop_view operator
   */
  virtual void Visit(const DropView *drop_view) {}

  /**
   * Visit a Analyze operator
   * @param analyze operator
   */
  virtual void Visit(const Analyze *analyze) {}

  /**
   * Visit a LogicalGet operator
   * @param logical_get operator
   */
  virtual void Visit(const LogicalGet *logical_get) {}

  /**
   * Visit a LogicalExternalFileGet operator
   * @param logical_external_file_get operator
   */
  virtual void Visit(const LogicalExternalFileGet *logical_external_file_get) {}

  /**
   * Visit a LogicalQueryDerivedGet operator
   * @param logical_query_derived_get operator
   */
  virtual void Visit(const LogicalQueryDerivedGet *logical_query_derived_get) {}

  /**
   * Visit a LogicalFilter operator
   * @param logical_filter operator
   */
  virtual void Visit(const LogicalFilter *logical_filter) {}

  /**
   * Visit a LogicalProjection operator
   * @param logical_projection operator
   */
  virtual void Visit(const LogicalProjection *logical_projection) {}

  /**
   * Visit a LogicalMardJoin operator
   * @param logical_mark_join operator
   */
  virtual void Visit(const LogicalMarkJoin *logical_mark_join) {}

  /**
   * Visit a LogicalSingleJoin operator
   * @param logical_single_join operator
   */
  virtual void Visit(const LogicalSingleJoin *logical_single_join) {}

  /**
   * Visit a LogicalDependentJoin operator
   * @param logical_dependent_join operator
   */
  virtual void Visit(const LogicalDependentJoin *logical_dependent_join) {}

  /**
   * Vusut a LogicalInnerJoin operator
   * @param logical_inner_join operator
   */
  virtual void Visit(const LogicalInnerJoin *logical_inner_join) {}

  /**
   * Visit a LogicalLeftJoin operator
   * @param logical_left_join operator
   */
  virtual void Visit(const LogicalLeftJoin *logical_left_join) {}

  /**
   * Visit a LogicalRightJoin operator
   * @param logical_right_join operator
   */
  virtual void Visit(const LogicalRightJoin *logical_right_join) {}

  /**
   * Visit a LogicalOuterJoin operator
   * @param logical_outer_join operator
   */
  virtual void Visit(const LogicalOuterJoin *logical_outer_join) {}

  /**
   * Visit a LogicalSemiJoin operator
   * @param logical_semi_join operator
   */
  virtual void Visit(const LogicalSemiJoin *logical_semi_join) {}

  /**
   * Visit a LogicalAggregateAndGroupBy operator
   * @param logical_aggregate_and_group_by operator
   */
  virtual void Visit(const LogicalAggregateAndGroupBy *logical_aggregate_and_group_by) {}

  /**
   * Visit a LogicalInsert operator
   * @param logical_insert operator
   */
  virtual void Visit(const LogicalInsert *logical_insert) {}

  /**
   * Visit a LogicalInsertSelect operator
   * @param logical_insert_select operator
   */
  virtual void Visit(const LogicalInsertSelect *logical_insert_select) {}

  /**
   * Visit a LogicalDelete operator
   * @param logical_delete operator
   */
  virtual void Visit(const LogicalDelete *logical_delete) {}

  /**
   * Visit a LogicalUpdata operator
   * @param logical_update operator
   */
  virtual void Visit(const LogicalUpdate *logical_update) {}

  /**
   * Visit a LogicalLimit operator
   * @param logical_limit operator
   */
  virtual void Visit(const LogicalLimit *logical_limit) {}

  /**
   * Visit a LogicalExportExternalFile operator
   * @param logical_export_external_file operator
   */
  virtual void Visit(const LogicalExportExternalFile *logical_export_external_file) {}

  /**
   * Visit a LogicalCreateDatabase operator
   * @param logical_create_database operator
   */
  virtual void Visit(const LogicalCreateDatabase *logical_create_database) {}

  /**
   * Visit a LogicalCreateFunction operator
   * @param logical_create_function operator
   */
  virtual void Visit(const LogicalCreateFunction *logical_create_function) {}

  /**
   * Visit a LogicalCreateIndex operator
   * @param logical_create_index operator
   */
  virtual void Visit(const LogicalCreateIndex *logical_create_index) {}

  /**
   * Visit a LogicalCreateTable operator
   * @param logical_create_table operator
   */
  virtual void Visit(const LogicalCreateTable *logical_create_table) {}

  /**
   * Visit a LogicalCreateNamespace operator
   * @param logical_create_namespace operator
   */
  virtual void Visit(const LogicalCreateNamespace *logical_create_namespace) {}  // NOLINT

  /**
   * Visit a LogicalCreateTrigger operator
   * @param logical_create_trigger operator
   */
  virtual void Visit(const LogicalCreateTrigger *logical_create_trigger) {}

  /**
   * Visit a LogicalCreateView operator
   * @param logical_create_view operator
   */
  virtual void Visit(const LogicalCreateView *logical_create_view) {}

  /**
   * Visit a LogicalDropDatabase operator
   * @param logical_drop_database operator
   */
  virtual void Visit(const LogicalDropDatabase *logical_drop_database) {}

  /**
   * Visit a LogicalDropTable operator
   * @param logical_drop_table operator
   */
  virtual void Visit(const LogicalDropTable *logical_drop_table) {}

  /**
   * Visit a LogicalDropIndex operator
   * @param logical_drop_index operator
   */
  virtual void Visit(const LogicalDropIndex *logical_drop_index) {}

  /**
   * Visit a LogicalDropNamespace operator
   * @param logical_drop_namespace operator
   */
  virtual void Visit(const LogicalDropNamespace *logical_drop_namespace) {}  // NOLINT

  /**
   * Visit a LogicalDropTrigger operator
   * @param logical_drop_trigger operator
   */
  virtual void Visit(const LogicalDropTrigger *logical_drop_trigger) {}

  /**
   * Visit a LogicalDropView operator
   * @param logical_drop_view operator
   */
  virtual void Visit(const LogicalDropView *logical_drop_view) {}

  /**
   * Visit a LogicalAnalyze operator
   * @param logical_analyze operator
   */
  virtual void Visit(const LogicalAnalyze *logical_analyze) {}
};

}  // namespace noisepage::optimizer
