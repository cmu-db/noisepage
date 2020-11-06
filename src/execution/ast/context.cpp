#include "execution/ast/context.h"

#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/StringMap.h>

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "brain/operating_unit.h"
#include "common/math_util.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/builtins.h"
#include "execution/ast/type.h"
#include "execution/sql/aggregation_hash_table.h"
#include "execution/sql/aggregators.h"
#include "execution/sql/filter_manager.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/join_hash_table_vector_probe.h"
#include "execution/sql/sorter.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql/thread_state_container.h"
#include "execution/sql/value.h"
// #include "execution/util/csv_reader.h" Fix later.
#include "execution/util/execution_common.h"

namespace noisepage::execution::ast {

// ---------------------------------------------------------
// Key type used in the cache for struct types in the context
// ---------------------------------------------------------

/**
 * Compute a hash_code for a field
 */
llvm::hash_code hash_value(const Field &field) {  // NOLINT
  return llvm::hash_combine(field.name_.GetData(), field.type_);
}

/*
 * Struct required to store TPL struct types in LLVM DenseMaps.
 */
struct StructTypeKeyInfo {
  struct KeyTy {
    const util::RegionVector<Field> &elements_;

    explicit KeyTy(const util::RegionVector<Field> &es) : elements_(es) {}

    explicit KeyTy(const StructType *struct_type) : elements_(struct_type->GetFieldsWithoutPadding()) {}

    bool operator==(const KeyTy &that) const { return elements_ == that.elements_; }

    bool operator!=(const KeyTy &that) const { return !this->operator==(that); }
  };

  // NOLINTNEXTLINE (LLVM DenseMap expects these names)
  static inline StructType *getEmptyKey() { return llvm::DenseMapInfo<StructType *>::getEmptyKey(); }

  // NOLINTNEXTLINE (LLVM DenseMap expects these names)
  static inline StructType *getTombstoneKey() { return llvm::DenseMapInfo<StructType *>::getTombstoneKey(); }

  // NOLINTNEXTLINE (LLVM DenseMap expects these names)
  static std::size_t getHashValue(const KeyTy &key) {
    return llvm::hash_combine_range(key.elements_.begin(), key.elements_.end());
  }

  // NOLINTNEXTLINE (LLVM DenseMap expects these names)
  static std::size_t getHashValue(const StructType *struct_type) { return getHashValue(KeyTy(struct_type)); }

  // NOLINTNEXTLINE (LLVM DenseMap expects these names)
  static bool isEqual(const KeyTy &lhs, const StructType *rhs) {
    if (rhs == getEmptyKey() || rhs == getTombstoneKey()) return false;
    return lhs == KeyTy(rhs);
  }

  // NOLINTNEXTLINE (LLVM DenseMap expects these names)
  static bool isEqual(const StructType *lhs, const StructType *rhs) { return lhs == rhs; }
};

// ---------------------------------------------------------
// Key type used in the cache for function types in the context
// ---------------------------------------------------------

/*
 * Struct required to store TPL function types in LLVM DenseMaps.
 */
struct FunctionTypeKeyInfo {
  struct KeyTy {
    Type *const ret_type_;
    const util::RegionVector<Field> &params_;

    explicit KeyTy(Type *ret_type, const util::RegionVector<Field> &ps) : ret_type_(ret_type), params_(ps) {}

    explicit KeyTy(const FunctionType *func_type)
        : ret_type_(func_type->GetReturnType()), params_(func_type->GetParams()) {}

    bool operator==(const KeyTy &that) const { return ret_type_ == that.ret_type_ && params_ == that.params_; }

    bool operator!=(const KeyTy &that) const { return !this->operator==(that); }
  };

  // NOLINTNEXTLINE
  static FunctionType *getEmptyKey() { return llvm::DenseMapInfo<FunctionType *>::getEmptyKey(); }

  // NOLINTNEXTLINE
  static FunctionType *getTombstoneKey() { return llvm::DenseMapInfo<FunctionType *>::getTombstoneKey(); }

  // NOLINTNEXTLINE
  static std::size_t getHashValue(const KeyTy &key) {
    return llvm::hash_combine(key.ret_type_, llvm::hash_combine_range(key.params_.begin(), key.params_.end()));
  }

  // NOLINTNEXTLINE
  static std::size_t getHashValue(const FunctionType *func_type) { return getHashValue(KeyTy(func_type)); }

  // NOLINTNEXTLINE
  static bool isEqual(const KeyTy &lhs, const FunctionType *rhs) {
    if (rhs == getEmptyKey() || rhs == getTombstoneKey()) return false;
    return lhs == KeyTy(rhs);
  }

  // NOLINTNEXTLINE
  static bool isEqual(const FunctionType *lhs, const FunctionType *rhs) { return lhs == rhs; }
};

struct Context::Implementation {
  static constexpr const uint32_t K_DEFAULT_STRING_TABLE_CAPACITY = 32;

  // -------------------------------------------------------
  // Builtin types
  // -------------------------------------------------------

#define F(BKind, ...) BuiltinType *BKind##Type;
  BUILTIN_TYPE_LIST(F, F, F)
#undef F
  StringType *string_type_;

  // -------------------------------------------------------
  // Type caches
  // -------------------------------------------------------

  llvm::StringMap<char, util::LLVMRegionAllocator> string_table_;
  std::vector<BuiltinType *> builtin_types_list_;
  llvm::DenseMap<Identifier, Type *> builtin_types_;
  llvm::DenseMap<Identifier, Builtin> builtin_funcs_;
  llvm::DenseMap<Type *, PointerType *> pointer_types_;
  llvm::DenseMap<std::pair<Type *, uint64_t>, ArrayType *> array_types_;
  llvm::DenseMap<std::pair<Type *, Type *>, MapType *> map_types_;
  llvm::DenseSet<StructType *, StructTypeKeyInfo> struct_types_;
  llvm::DenseSet<FunctionType *, FunctionTypeKeyInfo> func_types_;

  explicit Implementation(Context *ctx)
      : string_table_(K_DEFAULT_STRING_TABLE_CAPACITY, util::LLVMRegionAllocator(ctx->GetRegion())) {
    // Instantiate all the builtins
#define F(BKind, CppType, ...) \
  BKind##Type = new (ctx->GetRegion()) BuiltinType(ctx, sizeof(CppType), alignof(CppType), BuiltinType::BKind);
    BUILTIN_TYPE_LIST(F, F, F)
#undef F

    string_type_ = new (ctx->GetRegion()) StringType(ctx);
  }
};

Context::Context(util::Region *region, sema::ErrorReporter *error_reporter)
    : region_(region),
      error_reporter_(error_reporter),
      node_factory_(std::make_unique<AstNodeFactory>(region)),
      impl_(std::make_unique<Implementation>(this)) {
  // Put all builtins into list
#define F(BKind, ...) Impl()->builtin_types_list_.push_back(Impl()->BKind##Type);
  BUILTIN_TYPE_LIST(F, F, F)
#undef F

  // Put all builtins into cache by name
#define PRIM(BKind, CppType, TplName) Impl()->builtin_types_[GetIdentifier(TplName)] = Impl()->BKind##Type;
#define OTHERS(BKind, CppType) Impl()->builtin_types_[GetIdentifier(#BKind)] = Impl()->BKind##Type;
  BUILTIN_TYPE_LIST(PRIM, OTHERS, OTHERS)
#undef OTHERS
#undef PRIM

  // Builtin aliases
  Impl()->builtin_types_[GetIdentifier("int")] = Impl()->Int32Type;
  Impl()->builtin_types_[GetIdentifier("float")] = Impl()->Float32Type;
  Impl()->builtin_types_[GetIdentifier("void")] = Impl()->NilType;

  // Initialize builtin functions
#define BUILTIN_FUNC(Name, ...) \
  Impl()->builtin_funcs_[GetIdentifier(Builtins::GetFunctionName(Builtin::Name))] = Builtin::Name;
  BUILTINS_LIST(BUILTIN_FUNC)
#undef BUILTIN_FUNC
}

Context::~Context() = default;

Identifier Context::GetIdentifier(llvm::StringRef str) {
  if (str.empty()) {
    auto iter = Impl()->string_table_.insert(std::make_pair("", static_cast<char>(0))).first;
    return Identifier(iter->getKeyData());
  }

  auto iter = Impl()->string_table_.insert(std::make_pair(str, static_cast<char>(0))).first;
  return Identifier(iter->getKeyData());
}

Type *Context::LookupBuiltinType(Identifier name) const {
  auto iter = Impl()->builtin_types_.find(name);
  return (iter == Impl()->builtin_types_.end() ? nullptr : iter->second);
}

bool Context::IsBuiltinFunction(Identifier name, Builtin *builtin) const {
  if (auto iter = Impl()->builtin_funcs_.find(name); iter != Impl()->builtin_funcs_.end()) {
    if (builtin != nullptr) {
      *builtin = iter->second;
    }
    return true;
  }

  return false;
}

Identifier Context::GetBuiltinFunction(Builtin builtin) { return GetIdentifier(Builtins::GetFunctionName(builtin)); }

Identifier Context::GetBuiltinType(BuiltinType::Kind kind) {
  return GetIdentifier(Impl()->builtin_types_list_[kind]->GetTplName());
}

PointerType *Type::PointerTo() { return PointerType::Get(this); }

// static
BuiltinType *BuiltinType::Get(Context *ctx, BuiltinType::Kind kind) { return ctx->Impl()->builtin_types_list_[kind]; }

// static
StringType *StringType::Get(Context *ctx) { return ctx->Impl()->string_type_; }

// static
PointerType *PointerType::Get(Type *base) {
  Context *ctx = base->GetContext();

  PointerType *&pointer_type = ctx->Impl()->pointer_types_[base];

  if (pointer_type == nullptr) {
    pointer_type = new (ctx->GetRegion()) PointerType(base);
  }

  return pointer_type;
}

// static
ArrayType *ArrayType::Get(uint64_t length, Type *elem_type) {
  Context *ctx = elem_type->GetContext();

  ArrayType *&array_type = ctx->Impl()->array_types_[{elem_type, length}];

  if (array_type == nullptr) {
    array_type = new (ctx->GetRegion()) ArrayType(length, elem_type);
  }

  return array_type;
}

// static
MapType *MapType::Get(Type *key_type, Type *value_type) {
  Context *ctx = key_type->GetContext();

  MapType *&map_type = ctx->Impl()->map_types_[{key_type, value_type}];

  if (map_type == nullptr) {
    map_type = new (ctx->GetRegion()) MapType(key_type, value_type);
  }

  return map_type;
}

namespace {

Field CreatePaddingElement(uint32_t id, uint32_t size, Context *ctx) {
  Identifier name = ctx->GetIdentifier("__pad$" + std::to_string(id) + "$");
  auto *pad_type = BuiltinType::Get(ctx, BuiltinType::Int8);
  return Field(name, ArrayType::Get(size, pad_type));
}

};  // namespace

// static
StructType *StructType::Get(Context *ctx, util::RegionVector<Field> &&fields) {
  // Empty structs get an artificial element
  if (fields.empty()) {
    // Empty structs get an artificial byte field to ensure non-zero size
    ast::Identifier name = ctx->GetIdentifier("__field$0$");
    ast::Type *byte_type = ast::BuiltinType::Get(ctx, ast::BuiltinType::Int8);
    fields.emplace_back(name, byte_type);
  }

  const StructTypeKeyInfo::KeyTy key(fields);

  auto insert_res = ctx->Impl()->struct_types_.insert_as(nullptr, key);
  auto iter = insert_res.first;
  auto inserted = insert_res.second;

  StructType *struct_type = nullptr;

  if (inserted) {
    // Compute size and alignment. Alignment of struct is alignment of largest
    // struct element.
    uint32_t size = 0;
    uint32_t alignment = 0;
    util::RegionVector<Field> all_fields(ctx->GetRegion());
    util::RegionVector<uint32_t> field_offsets(ctx->GetRegion());
    all_fields.reserve(fields.size());
    field_offsets.reserve(fields.size());
    for (const auto &field : fields) {
      auto *field_type = field.type_;

      // Check if the type needs to be padded
      const uint32_t field_align = field_type->GetAlignment();
      if (!common::MathUtil::IsAligned(size, field_align)) {
        auto new_size = static_cast<uint32_t>(common::MathUtil::AlignTo(size, field_align));
        all_fields.emplace_back(CreatePaddingElement(size, new_size - size, ctx));
        field_offsets.push_back(size);
        size = new_size;
      }

      // Update size and calculate alignment
      field_offsets.push_back(size);
      all_fields.emplace_back(field);
      size += field_type->GetSize();
      alignment = std::max(alignment, field_align);
    }

    // Empty structs have an alignment of 1 byte
    if (alignment == 0) {
      alignment = 1;
    }

    // Add padding at end so that these structs can be placed compactly in an
    // array and still respect alignment
    if (!common::MathUtil::IsAligned(size, alignment)) {
      auto new_size = static_cast<uint32_t>(common::MathUtil::AlignTo(size, alignment));
      all_fields.emplace_back(CreatePaddingElement(size, new_size - size, ctx));
      field_offsets.push_back(size);
      size = new_size;
    }

    // Create type
    struct_type = new (ctx->GetRegion())
        StructType(ctx, size, alignment, std::move(all_fields), std::move(fields), std::move(field_offsets));
    // Set in cache
    *iter = struct_type;
  } else {
    struct_type = *iter;
  }

  return struct_type;
}

// static
StructType *StructType::Get(util::RegionVector<Field> &&fields) {
  NOISEPAGE_ASSERT(!fields.empty(), "Cannot use StructType::Get(fields) with an empty list of fields");
  return StructType::Get(fields[0].type_->GetContext(), std::move(fields));
}

// static
FunctionType *FunctionType::Get(util::RegionVector<Field> &&params, Type *ret) {
  Context *ctx = ret->GetContext();

  const FunctionTypeKeyInfo::KeyTy key(ret, params);

  auto insert_res = ctx->Impl()->func_types_.insert_as(nullptr, key);
  auto iter = insert_res.first;
  auto inserted = insert_res.second;

  FunctionType *func_type = nullptr;

  if (inserted) {
    // The function type was not in the cache, create the type now and insert it
    // into the cache
    func_type = new (ctx->GetRegion()) FunctionType(std::move(params), ret);
    *iter = func_type;
  } else {
    func_type = *iter;
  }

  return func_type;
}

}  // namespace noisepage::execution::ast
