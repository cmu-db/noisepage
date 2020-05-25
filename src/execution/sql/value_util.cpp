#include "execution/sql/value_util.h"

#include "common/allocator.h"
#include "execution/sql/value.h"

namespace terrier::execution::sql {

std::pair<std::unique_ptr<StringVal>, std::unique_ptr<byte>> ValueUtil::CreateStringVal(
    const common::ManagedPointer<const char> string, const uint32_t length) {
  std::unique_ptr<StringVal> value = nullptr;
  if (length <= StringVal::InlineThreshold()) {
    value = std::make_unique<StringVal>(string.Get(), length);
    return {std::move(value), nullptr};
  }
  // TODO(Matt): smarter allocation?
  std::unique_ptr<byte> buffer = std::unique_ptr<byte>(common::AllocationUtil::AllocateAligned(length));
  std::memcpy(buffer.get(), string.Get(), length);
  value = std::make_unique<StringVal>(reinterpret_cast<const char *>(buffer.get()), length);
  return {std::move(value), std::move(buffer)};
}

std::pair<std::unique_ptr<StringVal>, std::unique_ptr<byte>> ValueUtil::CreateStringVal(const std::string &string) {
  return CreateStringVal(common::ManagedPointer(string.data()), string.length());
}

std::pair<std::unique_ptr<StringVal>, std::unique_ptr<byte>> ValueUtil::CreateStringVal(const std::string_view string) {
  return CreateStringVal(common::ManagedPointer(string.data()), string.length());
}

std::pair<std::unique_ptr<StringVal>, std::unique_ptr<byte>> ValueUtil::CreateStringVal(
    common::ManagedPointer<StringVal> string) {
  return CreateStringVal(common::ManagedPointer(string->Content()), string->len_);
}

}  // namespace terrier::execution::sql
