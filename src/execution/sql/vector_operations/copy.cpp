#include "common/error/exception.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::sql {

namespace {

template <typename T>
void TemplatedCopyOperation(const Vector &source, void *target, uint64_t offset, uint64_t count) {
  auto *src_data = reinterpret_cast<T *>(source.GetData());
  auto *target_data = reinterpret_cast<T *>(target);
  VectorOps::Exec(
      source, [&](uint64_t i, uint64_t k) { target_data[k - offset] = src_data[i]; }, offset, count);
}

void GenericCopyOperation(const Vector &source, void *target, uint64_t offset, uint64_t element_count) {
  if (source.GetCount() == 0) {
    return;
  }

  switch (source.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedCopyOperation<bool>(source, target, offset, element_count);
      break;
    case TypeId::TinyInt:
      TemplatedCopyOperation<int8_t>(source, target, offset, element_count);
      break;
    case TypeId::SmallInt:
      TemplatedCopyOperation<int16_t>(source, target, offset, element_count);
      break;
    case TypeId::Integer:
      TemplatedCopyOperation<int32_t>(source, target, offset, element_count);
      break;
    case TypeId::BigInt:
      TemplatedCopyOperation<int64_t>(source, target, offset, element_count);
      break;
    case TypeId::Hash:
      TemplatedCopyOperation<hash_t>(source, target, offset, element_count);
      break;
    case TypeId::Pointer:
      TemplatedCopyOperation<uintptr_t>(source, target, offset, element_count);
      break;
    case TypeId::Float:
      TemplatedCopyOperation<float>(source, target, offset, element_count);
      break;
    case TypeId::Double:
      TemplatedCopyOperation<double>(source, target, offset, element_count);
      break;
    case TypeId::Date:
      TemplatedCopyOperation<Date>(source, target, offset, element_count);
      break;
    case TypeId::Timestamp:
      TemplatedCopyOperation<Timestamp>(source, target, offset, element_count);
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(
          fmt::format("copying vector of type '{}' not supported", TypeIdToString(source.GetTypeId())).data());
  }
}

}  // namespace

void VectorOps::Copy(const Vector &source, void *target, uint64_t offset, uint64_t element_count) {
  NOISEPAGE_ASSERT(IsTypeFixedSize(source.type_), "Copy should only be used for fixed-length types");
  GenericCopyOperation(source, target, offset, element_count);
}

void VectorOps::Copy(const Vector &source, Vector *target, uint64_t offset) {
  NOISEPAGE_ASSERT(offset < source.count_, "Out-of-bounds offset");
  NOISEPAGE_ASSERT(target->GetFilteredTupleIdList() == nullptr, "Cannot copy into filtered vector");

  // Resize the target vector to accommodate count-offset elements from the source vector
  target->Resize(source.GetCount() - offset);

  // Copy NULLs
  Exec(
      source, [&](uint64_t i, uint64_t k) { target->null_mask_[k - offset] = source.null_mask_[i]; }, offset);

  // Copy data
  Copy(source, target->GetData(), offset, target->GetCount());
}

}  // namespace noisepage::execution::sql
