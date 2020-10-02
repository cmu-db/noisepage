#include "common/strong_typedef.h"

#include "catalog/catalog_defs.h"
#include "common/action_context.h"
#include "common/json.h"
#include "execution/exec_defs.h"
#include "network/network_defs.h"
#include "optimizer/optimizer_defs.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_defs.h"
#include "type/type_id.h"

namespace terrier::common {

template <class Tag, typename IntType>
nlohmann::json StrongTypeAlias<Tag, IntType>::ToJson() const {
  nlohmann::json j = val_;
  return j;
}

template <class Tag, typename IntType>
void StrongTypeAlias<Tag, IntType>::FromJson(const nlohmann::json &j) {
  val_ = j.get<IntType>();
}

/*
 * Explicit template instantiations - this exists, because the above template functions
 * need to exist inside a cpp file, in order to prevent using the json library in too many
 * header files. Normally, you cannot define template functions in the cpp file, but by
 * explicitly declaring the template class here - you can.
 *
 */

template class StrongTypeAlias<terrier::optimizer::tags::group_id_t_typedef_tag, int32_t>;
template class StrongTypeAlias<terrier::catalog::tags::col_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::constraint_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::db_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::index_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::indexkeycol_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::namespace_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::language_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::proc_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::settings_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::table_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::tablespace_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::trigger_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::type_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::catalog::tags::view_oid_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::common::tags::action_id_t_typedef_tag, uint64_t>;
template class StrongTypeAlias<terrier::execution::tags::feature_id_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::execution::tags::pipeline_id_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::execution::tags::query_id_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::execution::tags::translator_id_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::network::tags::connection_id_t_typedef_tag, uint16_t>;
template class StrongTypeAlias<terrier::storage::tags::col_id_t_typedef_tag, uint16_t>;
template class StrongTypeAlias<terrier::storage::tags::layout_version_t_typedef_tag, uint16_t>;
template class StrongTypeAlias<terrier::transaction::tags::timestamp_t_typedef_tag, uint64_t>;
template class StrongTypeAlias<terrier::type::tags::date_t_typedef_tag, uint32_t>;
template class StrongTypeAlias<terrier::type::tags::timestamp_t_typedef_tag, uint64_t>;

}  // namespace terrier::common
