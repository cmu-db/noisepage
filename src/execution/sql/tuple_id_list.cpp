#include "execution/sql/tuple_id_list.h"

#include <iostream>
#include <string>

#include "execution/util/vector_util.h"

namespace noisepage::execution::sql {

void TupleIdList::BuildFromSelectionVector(const sel_t *sel_vector, uint32_t size) {
  for (uint32_t i = 0; i < size; i++) {
    bit_vector_.Set(sel_vector[i]);
  }
}

uint32_t TupleIdList::ToSelectionVector(sel_t *sel_vec) const {
  return util::VectorUtil::BitVectorToSelectionVector(bit_vector_.GetWords(), bit_vector_.GetNumBits(), sel_vec);
}

std::string TupleIdList::ToString() const {
  std::string result = "TIDs(" + std::to_string(GetTupleCount()) + "/" + std::to_string(GetCapacity()) + ")=[";
  bool first = true;
  ForEach([&](const uint64_t i) {
    if (!first) result += ",";
    first = false;
    result += std::to_string(i);
  });
  result += "]";
  return result;
}

void TupleIdList::Dump(std::ostream &stream) const { stream << ToString() << std::endl; }

}  // namespace noisepage::execution::sql
