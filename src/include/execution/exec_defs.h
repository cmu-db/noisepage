#pragma once

#include "common/strong_typedef.h"

namespace terrier::execution {

STRONG_TYPEDEF(query_id_t, uint64_t);
STRONG_TYPEDEF(pipeline_id_t, uint64_t);

/**
 * Use for represent different Alter commands type
 */
enum class ChangeType { Add, DropNoCascade, ChangeDefault, ChangeType };
using ChangeMap = std::unordered_map<std::string, std::vector<ChangeType>>;

}  // namespace terrier::execution
