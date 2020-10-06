#include "execution/ast/type.h"

#include <unordered_map>
#include <utility>

#include "brain/operating_unit.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/aggregation_hash_table.h"
#include "execution/sql/aggregators.h"
#include "execution/sql/filter_manager.h"
#include "execution/sql/hash_table_entry.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/join_hash_table_vector_probe.h"
#include "execution/sql/sorter.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql/thread_state_container.h"
#include "execution/sql/value.h"
#include "execution/sql/vector_projection_iterator.h"
// #include "execution/util/csv_reader.h" Fix later.

namespace terrier::execution::ast {

// ---------------------------------------------------------
// Type
// ---------------------------------------------------------

// TODO(pmenon): Fix me
bool Type::IsArithmetic() const {
  return IsIntegerType() ||                          // Primitive TPL integers
         IsFloatType() ||                            // Primitive TPL floats
         IsSpecificBuiltin(BuiltinType::Integer) ||  // SQL integer
         IsSpecificBuiltin(BuiltinType::Real) ||     // SQL reals
         IsSpecificBuiltin(BuiltinType::Decimal);    // SQL decimals
}

// ---------------------------------------------------------
// Builtin Type
// ---------------------------------------------------------

const char *BuiltinType::tpl_names[] = {
#define PRIM(BKind, CppType, Name, ...) Name,
#define OTHERS(BKind, ...) #BKind,
    BUILTIN_TYPE_LIST(PRIM, OTHERS, OTHERS)
#undef F
};

const char *BuiltinType::cpp_names[] = {
#define F(BKind, CppType, ...) #CppType,
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

const uint64_t BuiltinType::SIZES[] = {
#define F(BKind, CppType, ...) sizeof(CppType),
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

const uint64_t BuiltinType::ALIGNMENTS[] = {
#define F(Kind, CppType, ...) std::alignment_of_v<CppType>,
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

const bool BuiltinType::PRIMITIVE_FLAGS[] = {
#define F(Kind, CppType, ...) std::is_fundamental_v<CppType>,
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

const bool BuiltinType::FLOATING_POINT_FLAGS[] = {
#define F(Kind, CppType, ...) std::is_floating_point_v<CppType>,
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

const bool BuiltinType::SIGNED_FLAGS[] = {
#define F(Kind, CppType, ...) std::is_signed_v<CppType>,
    BUILTIN_TYPE_LIST(F, F, F)
#undef F
};

// ---------------------------------------------------------
// Function Type
// ---------------------------------------------------------

FunctionType::FunctionType(util::RegionVector<Field> &&params, Type *ret)
    : Type(ret->GetContext(), sizeof(void *), alignof(void *), TypeId::FunctionType),
      params_(std::move(params)),
      ret_(ret) {}

// ---------------------------------------------------------
// Map Type
// ---------------------------------------------------------

MapType::MapType(Type *key_type, Type *val_type)
    : Type(key_type->GetContext(), sizeof(std::unordered_map<int32_t, int32_t>),
           alignof(std::unordered_map<int32_t, int32_t>), TypeId::MapType),
      key_type_(key_type),
      val_type_(val_type) {}

// ---------------------------------------------------------
// Struct Type
// ---------------------------------------------------------

StructType::StructType(Context *ctx, uint32_t size, uint32_t alignment, util::RegionVector<Field> &&fields,
                       util::RegionVector<Field> &&unpadded_fields, util::RegionVector<uint32_t> &&field_offsets)
    : Type(ctx, size, alignment, TypeId::StructType),
      fields_(std::move(fields)),
      unpadded_fields_(std::move(unpadded_fields)),
      field_offsets_(std::move(field_offsets)) {}

}  // namespace terrier::execution::ast
