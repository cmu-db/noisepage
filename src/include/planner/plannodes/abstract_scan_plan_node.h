#pragma once

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/hash_util.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "nlohmann/json.hpp"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"

namespace terrier {
namespace parser {
class AbstractExpression;
}  // namespace parser
}  // namespace terrier

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
    ConcreteType &SetScanPredicate(common::ManagedPointer<parser::AbstractExpression> predicate) {
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
    common::ManagedPointer<parser::AbstractExpression> scan_predicate_;
    /**
     * Is scan for update
     */
    bool is_for_update_ = false;
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
   * @param database_oid database oid for scan
   * @param namespace_oid OID of the namespace
   */
  AbstractScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                       std::unique_ptr<OutputSchema> output_schema,
                       common::ManagedPointer<parser::AbstractExpression> predicate, bool is_for_update,
                       catalog::db_oid_t database_oid, catalog::namespace_oid_t namespace_oid)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        scan_predicate_(predicate),
        is_for_update_(is_for_update),
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
  common::ManagedPointer<parser::AbstractExpression> GetScanPredicate() const {
    return common::ManagedPointer(scan_predicate_);
  }

  /**
   * @return for update flag
   */
  bool IsForUpdate() const { return is_for_update_; }

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
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * Selection predicate.
   */
  common::ManagedPointer<parser::AbstractExpression> scan_predicate_;

  /**
   * Are the tuples produced by this plan intended for update?
   */
  bool is_for_update_ = false;

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
