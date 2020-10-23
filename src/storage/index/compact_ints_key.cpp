#include "storage/index/compact_ints_key.h"

namespace noisepage::storage::index {

template class CompactIntsKey<8>;
template class CompactIntsKey<16>;
template class CompactIntsKey<24>;
template class CompactIntsKey<32>;

}  // namespace noisepage::storage::index
