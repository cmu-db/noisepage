#include "execution/sql/value_util.h"

#include "common/allocator.h"
#include "execution/sql/value.h"

namespace noisepage::execution::sql {

std::pair<StringVal, std::unique_ptr<byte[]>> ValueUtil::CreateStringVal(
    const common::ManagedPointer<const char> string, const uint32_t length) {
  if (length <= StringVal::InlineThreshold()) {
    return {StringVal(string.Get(), length), nullptr};
  }
  // TODO(Matt): smarter allocation?
  auto buffer = std::unique_ptr<byte[]>(common::AllocationUtil::AllocateAligned(length));
  std::memcpy(buffer.get(), string.Get(), length);
  return {StringVal(reinterpret_cast<const char *>(buffer.get()), length), std::move(buffer)};
}

std::pair<StringVal, std::unique_ptr<byte[]>> ValueUtil::CreateStringVal(const std::string &string) {
  return CreateStringVal(common::ManagedPointer(string.data()), string.length());
}

std::pair<StringVal, std::unique_ptr<byte[]>> ValueUtil::CreateStringVal(const std::string_view string) {
  return CreateStringVal(common::ManagedPointer(string.data()), string.length());
}

std::pair<StringVal, std::unique_ptr<byte[]>> ValueUtil::CreateStringVal(const StringVal string) {
  return CreateStringVal(common::ManagedPointer(string.GetContent()), string.GetLength());
}

}  // namespace noisepage::execution::sql
