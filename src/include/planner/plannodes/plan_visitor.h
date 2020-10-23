#pragma once

namespace noisepage::planner {

class AggregatePlanNode;
class AnalyzePlanNode;
class CreateDatabasePlanNode;
class CreateFunctionPlanNode;
class CreateIndexPlanNode;
class CreateNamespacePlanNode;
class CreateTablePlanNode;
class CreateTriggerPlanNode;
class CreateViewPlanNode;
class CSVScanPlanNode;
class DeletePlanNode;
class DropDatabasePlanNode;
class DropIndexPlanNode;
class DropNamespacePlanNode;
class DropTablePlanNode;
class DropTriggerPlanNode;
class DropViewPlanNode;
class ExportExternalFilePlanNode;
class HashJoinPlanNode;
class IndexJoinPlanNode;
class IndexScanPlanNode;
class InsertPlanNode;
class LimitPlanNode;
class NestedLoopJoinPlanNode;
class OrderByPlanNode;
class ProjectionPlanNode;
class SeqScanPlanNode;
class UpdatePlanNode;
class SetOpPlanNode;
class ResultPlanNode;

/**
 * Utility class for visitor pattern for plan nodes
 */
class PlanVisitor {
 public:
  virtual ~PlanVisitor() = default;

  /**
   * Visit an AggregatePlanNode
   * @param plan AggregatePlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const AggregatePlanNode *plan) {}

  /**
   * Visit an AnalyzePlanNode
   * @param plan AnalyzePlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const AnalyzePlanNode *plan) {}

  /**
   * Visit an CreateDatabasePlanNode
   * @param plan CreateDatabasePlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const CreateDatabasePlanNode *plan) {}

  /**
   * Visit an CreateFunctionPlanNode
   * @param plan CreateFunctionPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const CreateFunctionPlanNode *plan) {}

  /**
   * Visit an CreateIndexPlanNode
   * @param plan CreateIndexPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const CreateIndexPlanNode *plan) {}

  /**
   * Visit an CreateNamespacePlanNode
   * @param plan CreateNamespacePlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const CreateNamespacePlanNode *plan) {}

  /**
   * Visit an CreateTablePlanNode
   * @param plan CreateTablePlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const CreateTablePlanNode *plan) {}

  /**
   * Visit an CreateTriggerPlanNode
   * @param plan CreateTriggerPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const CreateTriggerPlanNode *plan) {}

  /**
   * Visit an CreateViewPlanNode
   * @param plan CreateViewPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const CreateViewPlanNode *plan) {}

  /**
   * Visit an CSVScanPlanNode
   * @param plan CSVScanPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const CSVScanPlanNode *plan) {}

  /**
   * Visit an DeletePlanNode
   * @param plan DeletePlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const DeletePlanNode *plan) {}

  /**
   * Visit an DropDatabasePlanNode
   * @param plan DropDatabasePlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const DropDatabasePlanNode *plan) {}

  /**
   * Visit an DropIndexPlanNode
   * @param plan DropIndexPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const DropIndexPlanNode *plan) {}

  /**
   * Visit an DropNamespacePlanNode
   * @param plan DropNamespacePlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const DropNamespacePlanNode *plan) {}

  /**
   * Visit an DropTablePlanNode
   * @param plan DropTablePlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const DropTablePlanNode *plan) {}

  /**
   * Visit an DropTriggerPlanNode
   * @param plan DropTriggerPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const DropTriggerPlanNode *plan) {}

  /**
   * Visit an DropViewPlanNode
   * @param plan DropViewPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const DropViewPlanNode *plan) {}

  /**
   * Visit an ExportExternalFilePlanNode
   * @param plan ExportExternalFilePlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const ExportExternalFilePlanNode *plan) {}

  /**
   * Visit an HashJoinPlanNode
   * @param plan HashJoinPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const HashJoinPlanNode *plan) {}

  /**
   * Visit an IndexJoinPlanNode
   * @param plan IndexJoinPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const IndexJoinPlanNode *plan) {}

  /**
   * Visit an IndexScanPlanNode
   * @param plan IndexScanPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const IndexScanPlanNode *plan) {}

  /**
   * Visit an InsertPlanNode
   * @param plan InsertPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const InsertPlanNode *plan) {}

  /**
   * Visit an LimitPlanNode
   * @param plan LimitPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const LimitPlanNode *plan) {}

  /**
   * Visit an NestedLoopJoinPlanNode
   * @param plan NestedLoopJoinPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const NestedLoopJoinPlanNode *plan) {}

  /**
   * Visit an OrderByPlanNode
   * @param plan OrderByPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const OrderByPlanNode *plan) {}

  /**
   * Visit an ProjectionPlanNode
   * @param plan ProjectionPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const ProjectionPlanNode *plan) {}

  /**
   * Visit an SeqScanPlanNode
   * @param plan SeqScanPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const SeqScanPlanNode *plan) {}

  /**
   * Visit an UpdatePlanNode
   * @param plan UpdatePlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const UpdatePlanNode *plan) {}

  /**
   * Visit an SetOpPlanNode
   * @param plan SetOpPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const SetOpPlanNode *plan) {}

  /**
   * Visit an ResultPlanNode
   * @param plan ResultPlanNode
   */
  virtual void Visit(UNUSED_ATTRIBUTE const ResultPlanNode *plan) {}
};

}  // namespace noisepage::planner
