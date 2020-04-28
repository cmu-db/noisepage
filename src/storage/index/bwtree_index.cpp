#include "storage/index/bwtree_index.h"

#include "storage/index/compact_ints_key.h"
#include "storage/index/generic_key.h"

namespace terrier::storage::index {

template class BwTreeIndex<CompactIntsKey<8>>;
template class BwTreeIndex<CompactIntsKey<16>>;
template class BwTreeIndex<CompactIntsKey<24>>;
template class BwTreeIndex<CompactIntsKey<32>>;

template class BwTreeIndex<GenericKey<64>>;
template class BwTreeIndex<GenericKey<128>>;
template class BwTreeIndex<GenericKey<256>>;

}  // namespace terrier::storage::index
