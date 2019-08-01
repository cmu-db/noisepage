#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "parser/insert_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/abstract_scan_plan_node.h"
#include "type/transient_value.h"
#include "type/transient_value_peeker.h"

namespace terrier::planner {

/**
 * Plan node for insert
 */
class InsertPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for an insert plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param database_oid OID of the database
     * @return builder object
     */
    Builder &SetDatabaseOid(catalog::db_oid_t database_oid) {
      database_oid_ = database_oid;
      return *this;
    }

    /**
     * @param namespace_oid OID of the namespace
     * @return builder object
     */
    Builder &SetNamespaceOid(catalog::namespace_oid_t namespace_oid) {
      namespace_oid_ = namespace_oid;
      return *this;
    }

    /**
     * @param table_oid the OID of the target SQL table
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

    /**
     * @param values values to insert
     * @return builder object
     */
    Builder &AddValues(std::vector<type::TransientValue> &&values) {
      values_.emplace_back(std::move(values));
      return *this;
    }

    /**
     * @param col_oid oid of column where value at value_idx should be inserted
     * @return builder object
     * @warning The caller must push column index in order. The ith call to AddParameterInfo means for a value tuple
     * values_[t], values_[t][i] will be inserted into the column indicated by the input col_oid.
     */
    Builder &AddParameterInfo(catalog::col_oid_t col_oid) {
      parameter_info_.emplace_back(col_oid);
      return *this;
    }

    /**
     * Build the delete plan node
     * @return plan node
     */
    std::shared_ptr<InsertPlanNode> Build() {
      TERRIER_ASSERT(!values_.empty(), "Can't have an empty insert plan");
      TERRIER_ASSERT(values_[0].size() == parameter_info_.size(), "Must have parameter info for each value");
      return std::shared_ptr<InsertPlanNode>(new InsertPlanNode(std::move(children_), std::move(output_schema_),
                                                                database_oid_, namespace_oid_, table_oid_,
                                                                std::move(values_), std::move(parameter_info_)));
    }

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

    /**
     * OID of namespace
     */
    catalog::namespace_oid_t namespace_oid_;

    /**
     * OID of the table to insert into
     */
    catalog::table_oid_t table_oid_;

    /**
     * vector of values to insert. Multiple vector of values corresponds to a bulk insert. Values for each tuple are
     * ordered the same across tuples. Parameter info provides column mapping of values
     */
    std::vector<std::vector<type::TransientValue>> values_;

    /**
     * parameter information. Provides which column a value should be inserted into. For example, for a tuple t at
     * values_[t], the value at index i (values_[t][i]) should be inserted into column parameter_info_[i]
     * @warning This relies on the assumption that values are ordered the same for every tuple in the bulk insert
     */
    std::vector<catalog::col_oid_t> parameter_info_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid the OID of the target SQL table
   * @param values values to insert
   * @param parameter_info parameters information
   */
  InsertPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                 catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
                 std::vector<std::vector<type::TransientValue>> &&values,
                 std::vector<catalog::col_oid_t> &&parameter_info)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        namespace_oid_(namespace_oid),
        table_oid_(table_oid),
        values_(std::move(values)),
        parameter_info_(std::move(parameter_info)) {}

 public:
  DISALLOW_COPY_AND_MOVE(InsertPlanNode)

  /**
   * Default constructor used for deserialization
   */
  InsertPlanNode() = default;
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::INSERT; }

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return the OID of the table to insert into
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @param idx index of tuple in values vecor
   * @return values to be inserted
   */
  const std::vector<type::TransientValue> &GetValues(uint32_t idx) const { return values_[idx]; }

  /**
   * @return the information of insert parameters
   */
  const std::vector<catalog::col_oid_t> &GetParameterInfo() const { return parameter_info_; }

  /**
   * @param value_idx index of value being inserted
   * @return OID of column where value should be inserted
   */
  const catalog::col_oid_t GetColumnOidForValue(uint32_t value_idx) const { return parameter_info_.at(value_idx); }

  /**
   * @return number of tuples to insert
   */
  size_t GetBulkInsertCount() const { return values_.size(); }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * OID of the table to insert into
   */
  catalog::table_oid_t table_oid_;

  // TODO(Gus, Wan): As an optimization, we can flatten this 2d vector because each inner vector is the same size.
  /**
   * vector of values to insert. Multiple vector of values corresponds to a bulk insert. Values for each tuple are
   * ordered the same across tuples. Parameter info provides column mapping of values
   */
  std::vector<std::vector<type::TransientValue>> values_;

  /**
   * parameter information. Provides which column a value should be inserted into. For example, for a tuple t at
   * values_[t], the value at index i (values_[t][i]) should be inserted into column parameter_info_[i]
   * @warning This relies on the assumption that values are ordered the same for every tuple in the bulk insert
   */
  std::vector<catalog::col_oid_t> parameter_info_;
};

DEFINE_JSON_DECLARATIONS(InsertPlanNode);

}  // namespace terrier::planner
