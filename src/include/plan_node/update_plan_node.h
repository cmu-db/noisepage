#pragma once

#include <memory>
#include <string>
#include "parser/parameter.h"
#include "parser/update_statement.h"
#include "plan_node/abstract_plan_node.h"
#include "plan_node/output_schema.h"

namespace terrier {

namespace storage {
class SqlTable;
}
// TODO(Gus,Wen) Add back VisitParameters, SetParamaterValues, and PerformBinding

namespace plan_node {

class UpdatePlanNode : public AbstractPlanNode {
 public:
  UpdatePlanNode() = delete;

  /**
   * Instantiate an UpdatePlanNode
   * @param target_table_oid the OID of the target table to operate on
   * @param output_schema the output columns and mapping information
   */
  UpdatePlanNode(catalog::table_oid_t target_table_oid, std::shared_ptr<OutputSchema> output_schema);

  /**
   * Default destructor
   */
  ~UpdatePlanNode() override;

  /**
   * @return the OID of the target table to operate on
   */
  catalog::table_oid_t GetTargetTableOid() const { return target_table_oid_; }

  /**
   * @return whether to update primary key
   */
  bool GetUpdatePrimaryKey() const { return update_primary_key_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::UPDATE; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

  DISALLOW_COPY_AND_MOVE(UpdatePlanNode);

 private:
  // The OID of the target table to operate on
  catalog::table_oid_t target_table_oid_;

  // Whether to update primary key
  bool update_primary_key_;
};

}  // namespace plan_node
}  // namespace terrier
