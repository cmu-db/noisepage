#include "storage/index/hash_index.h"

#include "storage/index/generic_key.h"
#include "storage/index/hash_key.h"

namespace terrier::storage::index {

template class HashIndex<HashKey<8>>;
template class HashIndex<HashKey<16>>;
template class HashIndex<HashKey<32>>;
template class HashIndex<HashKey<64>>;
template class HashIndex<HashKey<128>>;
template class HashIndex<HashKey<256>>;

template class HashIndex<GenericKey<64>>;
template class HashIndex<GenericKey<128>>;
template class HashIndex<GenericKey<256>>;

}  // namespace terrier::storage::index
