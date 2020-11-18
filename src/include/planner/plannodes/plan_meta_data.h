#pragma once

#include <unordered_map>

#include "plan_node_defs.h"

namespace noisepage::planner {

/**
 * Meta data for a plan tree
 */
class PlanMetaData {
 public:
   /**
    * Meta data for every plan node
    */
   class PlanNodeMetaData {
    public:
     PlanNodeMetaData() = default;

     PlanNodeMetaData(int cardinality): cardinality_(cardinality) {}

     int GetCardinality() {
       return cardinality_;
     }

    private:
     int cardinality_ = -1;
   };

   /**
    * Add meta data for a plan node
    * @param plan_node_id plan node id
    * @param meta_data plan node meta data
    */
   void AddPlanNodeMetaData(plan_node_id_t plan_node_id, PlanNodeMetaData meta_data) {
     NOISEPAGE_ASSERT(plan_node_meta_data_.count(plan_node_id) == 0, "already exists meta data for the plan node");
     plan_node_meta_data_[plan_node_id] = meta_data;
   }

   /**
    * Get the meta data for a plan node
    * @param plan_node_id plan node id
    * @return plan node meta data
    */
   PlanNodeMetaData GetPlanNodeMetaData(plan_node_id_t plan_node_id) {
     NOISEPAGE_ASSERT(plan_node_meta_data_.count(plan_node_id) != 0, "there is no meta data for the plan node");
     return plan_node_meta_data_[plan_node_id];
   }

 private:
  std::unordered_map<plan_node_id_t, PlanNodeMetaData> plan_node_meta_data_;
};
}