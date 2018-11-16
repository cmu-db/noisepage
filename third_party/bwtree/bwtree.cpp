#include "bwtree/bwtree.h"

namespace third_party::index {

bool print_flag = true;

// This will be initialized when thread is initialized and in a per-thread
// basis, i.e. each thread will get the same initialization image and then
// is free to change them
thread_local int third_party::index::BwTreeBase::gc_id = -1;

std::atomic<size_t> third_party::index::BwTreeBase::total_thread_num{0UL};

}
