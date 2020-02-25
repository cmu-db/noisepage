#include "storage/index/bplustree_index.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/generic_key.h"

namespace terrier::storage::index {

template class BPlusTreeIndex<CompactIntsKey<8>>;
template class BPlusTreeIndex<CompactIntsKey<16>>;
template class BPlusTreeIndex<CompactIntsKey<24>>;
template class BPlusTreeIndex<CompactIntsKey<32>>;

template class BPlusTreeIndex<GenericKey<64>>;
template class BPlusTreeIndex<GenericKey<128>>;
template class BPlusTreeIndex<GenericKey<256>>;

}  // namespace terrier::storage::index
