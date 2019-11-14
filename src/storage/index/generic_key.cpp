#include "storage/index/generic_key.h"

namespace terrier::storage::index {

template class GenericKey<64>;
template class GenericKey<128>;
template class GenericKey<256>;

}  // namespace terrier::storage::index
