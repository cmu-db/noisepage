#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::planner {

/**
 * Base class for sql scans
 */
class AbstractScanPlanNode : public AbstractPlanNode {
 protected:
  /**
   * Base builder class for scan plan nodes
   * @tparam ConcreteType
   */
  template <class ConcreteType>
  class Builder : public AbstractPlanNode::Builder<ConcreteType> {
   public:
    /**
     * @param predicate predicate to use for scan
     * @return builder object
     */
    ConcreteType &SetScanPredicate(const std::shared_ptr<parser::AbstractExpression> &predicate) {
      scan_predicate_ = predicate;
      return *dynamic_cast<ConcreteType *>(this);
    }

    /**
     * @param flag is for update flag
     * @return builder object
     */
    ConcreteType &SetIsForUpdateFlag(bool flag) {
      is_for_update_ = flag;
      return *dynamic_cast<ConcreteType *>(this);
    }

    /**
     * @param flag is parallel scan flag
     * @return builder object
     */
    ConcreteType &SetIsParallelFlag(bool flag) {
      is_parallel_ = flag;
      return *dynamic_cast<ConcreteType *>(this);
    }

    /**
     * @param database_oid database OID of table/index beind scanned
     * @return builder object
     */
    ConcreteType &SetDatabaseOid(catalog::db_oid_t database_oid) {
      database_oid_ = database_oid;
      return *dynamic_cast<ConcreteType *>(this);
    }

    /**
     * @param namespace_oid namespace OID of table/index beind scanned
     * @return builder object
     */
    ConcreteType &SetNamespaceOid(catalog::namespace_oid_t namespace_oid) {
      namespace_oid_ = namespace_oid;
      return *dynamic_cast<ConcreteType *>(this);
    }

   protected:
    /**
     * Scan predicate
     */
    std::shared_ptr<parser::AbstractExpression> scan_predicate_;
    /**
     * Is scan for update
     */
    bool is_for_update_ = false;
    /**
     * Is this a parallel scan
     */
    bool is_parallel_ = false;

    /**
     * Database OID for scan
     */
    catalog::db_oid_t database_oid_;

    /**
     * OID of namespace
     */
    catalog::namespace_oid_t namespace_oid_;
  };

  /**
   * Base constructor for scans. Derived scan plans should call this constructor
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param predicate predicate used for performing scan
   * @param is_for_update scan is used for an update
   * @param is_parallel parallel scan flag
   * @param database_oid database oid for scan
   * @param namespace_oid OID of the namespace
   */
  AbstractScanPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                       std::shared_ptr<OutputSchema> output_schema,
                       std::shared_ptr<parser::AbstractExpression> predicate, bool is_for_update, bool is_parallel,
                       catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        scan_predicate_(std::move(predicate)),
        is_for_update_(is_for_update),
        is_parallel_(is_parallel),
        database_oid_(database_oid),
        namespace_oid_(namespace_oid) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  AbstractScanPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(AbstractScanPlanNode)

  /**
   * @return predicate used for performing scan
   */
  const std::shared_ptr<parser::AbstractExpression> &GetScanPredicate() const { return scan_predicate_; }

  /**
   * @return for update flag
   */
  bool IsForUpdate() const { return is_for_update_; }

  /**
   * @return parallel scan flag
   */
  bool IsParallel() const { return is_parallel_; }

  /**
   * @return database OID of index/table being scanned
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return namespace OID of index/table being scanned
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;
  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  /**
   * Selection predicate. We remove const to make it used when deserialization
   */
  std::shared_ptr<parser::AbstractExpression> scan_predicate_;

  /**
   * Are the tuples produced by this plan intended for update?
   */
  bool is_for_update_ = false;

  /**
   * Should this scan be performed in parallel?
   */
  bool is_parallel_;

  /**
   * Database OID for scan
   */
  catalog::db_oid_t database_oid_;

  /**
   * Namespace OID for scan
   */
  catalog::namespace_oid_t namespace_oid_;
};

}  // namespace terrier::planner
