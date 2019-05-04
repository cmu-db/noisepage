#pragma once

#include <string>
namespace terrier::planner {
class AbstractPlanNode;
}

namespace tpl::util {
class Region;
}

namespace tpl::compiler {

class Query {
 public:
  const terrier::planner::AbstractPlanNode &GetPlan();
  util::Region *GetRegion();
  std::string GetQueryStateName();
  std::string GetQueryInitName();
};

}