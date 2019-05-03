#pragma once

#include <cstdint>
#include <string>

#include "llvm/Support/Casting.h"

#include "execution/ast/identifier.h"
#include "execution/util/region.h"
#include "execution/util/region_containers.h"

namespace tpl::ast {

class Context;

// List of all concrete types
#define TYPE_LIST(F) \
  F(BuiltinType)     \
  F(StringType)      \
  F(PointerType)     \
  F(ArrayType)       \
  F(MapType)         \
  F(StructType)      \
  F(FunctionType)

// Macro listing all builtin types. Accepts multiple callback functions to
// handle the different kinds of builtins.
//
// PRIM:     A primitive builtin type (e.g., bool, int32_t etc.)
//           Args: Kind, C++ type, TPL name (i.e., as it appears in TPL code)
// NON_PRIM: A builtin type that isn't primitive. These are pre-compiled C++
//           classes that can be created and manipulated from TPL code.
//           Args: Kind, C++ type
// SQL:      These are full-blown SQL types. SQL types have backing C++
//           implementations, but can also be created and manipulated from TPL
//           code. We specialize these because we also want to add SQL-level
//           type information to these builtins.
#define BUILTIN_TYPE_LIST(PRIM, NON_PRIM, SQL)                           \
  /* Primitive types */                                                  \
  PRIM(Nil, u8, "nil")                                                   \
  PRIM(Bool, bool, "bool")                                               \
  PRIM(Int8, i8, "int8")                                                 \
  PRIM(Int16, i16, "int16")                                              \
  PRIM(Int32, i32, "int32")                                              \
  PRIM(Int64, i64, "int64")                                              \
  PRIM(Uint8, u8, "uint8")                                               \
  PRIM(Uint16, u16, "uint16")                                            \
  PRIM(Uint32, u32, "uint32")                                            \
  PRIM(Uint64, u64, "uint64")                                            \
  PRIM(Int128, i128, "int128")                                           \
  PRIM(Uint128, u128, "uint128")                                         \
  PRIM(Float32, f32, "float32")                                          \
  PRIM(Float64, f64, "float64")                                          \
                                                                         \
  /* Non-primitive builtins */                                           \
  NON_PRIM(AggregationHashTable, tpl::sql::AggregationHashTable)         \
  NON_PRIM(BloomFilter, tpl::sql::BloomFilter)                           \
  NON_PRIM(CountAggregate, tpl::sql::CountAggregate)                     \
  NON_PRIM(CountStarAggregate, tpl::sql::CountStarAggregate)             \
  NON_PRIM(HashTableEntry, tpl::sql::HashTableEntry)                     \
  NON_PRIM(IntegerSumAggregate, tpl::sql::IntegerSumAggregate)           \
  NON_PRIM(JoinHashTable, tpl::sql::JoinHashTable)                       \
  NON_PRIM(RegionAlloc, tpl::util::Region)                               \
  NON_PRIM(Sorter, tpl::sql::Sorter)                                     \
  NON_PRIM(SorterIterator, tpl::sql::SorterIterator)                     \
  NON_PRIM(TableVectorIterator, tpl::sql::TableVectorIterator)           \
  NON_PRIM(VectorProjectionIterator, tpl::sql::VectorProjectionIterator) \
                                                                         \
  /* Non-primitive SQL Runtime Values */                                 \
  SQL(Boolean, tpl::sql::BoolVal)                                        \
  SQL(Integer, tpl::sql::Integer)                                        \
  SQL(Real, tpl::sql::Real)                                              \
  SQL(Decimal, tpl::sql::Decimal)                                        \
  SQL(VarBuffer, tpl::sql::VarBuffer)                                    \
  SQL(Date, tpl::sql::Date)                                              \
  SQL(Timestamp, tpl::sql::Timestamp)

// Ignore a builtin
#define IGNORE_BUILTIN_TYPE (...)

// Only consider the primitive builtin types
#define PRIMIMITIVE_BUILTIN_TYPE_LIST(F) BUILTIN_TYPE_LIST(F, IGNORE_BUILTIN_TYPE, IGNORE_BUILTIN_TYPE)

// Only consider the non-primitive builtin types
#define NON_PRIMITIVE_BUILTIN_TYPE_LIST(F) BUILTIN_TYPE_LIST(IGNORE_BUILTIN_TYPE, F, IGNORE_BUILTIN_TYPE)

// Only consider the SQL builtin types
#define SQL_BUILTIN_TYPE_LIST(F) BUILTIN_TYPE_LIST(IGNORE_BUILTIN_TYPE, IGNORE_BUILTIN_TYPE, F)

// Forward declare everything first
#define F(TypeClass) class TypeClass;
TYPE_LIST(F)
#undef F

/// The base of the TPL type hierarchy. Types, once created, are immutable. Only
/// one instance of a particular type is ever created, and all instances are
/// owned by the Context object that created it. Thus, one can use pointer
/// equality to determine if two types are equal, but only if they were created
/// within the same Context.
class Type : public util::RegionObject {
 public:
  /// The enumeration of all concrete types
  enum class TypeId : u8 {
#define F(TypeId) TypeId,
    TYPE_LIST(F)
#undef F
  };

  /// Return the context this type was allocated in
  Context *context() const { return ctx_; }

  /// Return the size of this type in bytes
  u32 size() const { return size_; }

  /// Return the alignment of this type in bytes
  u32 alignment() const { return align_; }

  /// Return the unique type ID of this type (e.g., int16, Array, Struct etc.)
  TypeId type_id() const { return type_id_; }

  /// Perform an "checked cast" to convert an instance of this base Type class
  /// into one of its derived types. If the target class isn't a subclass of
  /// Type, an assertion failure is thrown in debug mode. In release mode, such
  /// a call will fail.
  ///
  /// You should use this function when you have reasonable certainty that you
  /// know the concrete type. Example:
  ///
  /// \code
  /// if (!type->IsBuiltinType()) {
  ///   return;
  /// }
  /// ...
  /// auto *builtin_type = type->As<ast::BuiltinType>();
  /// ...
  /// \endcode
  ///
  template <typename T>
  const T *As() const {
    return llvm::cast<const T>(this);
  }

  template <typename T>
  T *As() {
    return llvm::cast<T>(this);
  }

  /// Perform a "checking cast". This function checks to see if the target type
  /// is a subclass of Type, returning a pointer to the subclass if so, or
  /// returning a null pointer otherwise.
  ///
  /// You should use this in conditional or control-flow statements when you
  /// want to check if a type is a specific subtype **AND** get a pointer to the
  /// subtype, like so:
  ///
  /// \code
  /// if (auto *builtin_type = SafeAs<ast::BuiltinType>()) {
  ///   // ...
  /// }
  /// \endcode
  template <typename T>
  const T *SafeAs() const {
    return llvm::dyn_cast<const T>(this);
  }

  template <typename T>
  T *SafeAs() {
    return llvm::dyn_cast<T>(this);
  }

  /// Type checks
#define F(TypeClass) \
  bool Is##TypeClass() const { return llvm::isa<TypeClass>(this); }
  TYPE_LIST(F)
#undef F

  bool IsArithmetic() const;
  bool IsSpecificBuiltin(u16 kind) const;
  bool IsNilType() const;
  bool IsBoolType() const;
  bool IsIntegerType() const;
  bool IsFloatType() const;

  /// Return a new type that is a pointer to the current type
  PointerType *PointerTo();

  /// If this is a pointer type, return the type it points to, returning null
  /// otherwise.
  Type *GetPointeeType() const;

  /// Get a string representation of this type
  std::string ToString() const { return ToString(this); }

  /// Get a string representation of the input type
  static std::string ToString(const Type *type);

 protected:
  // Protected to indicate abstract base
  Type(Context *ctx, u32 size, u32 alignment, TypeId type_id)
      : ctx_(ctx), size_(size), align_(alignment), type_id_(type_id) {}

 private:
  // The context this type was created/unique'd in
  Context *ctx_;
  // The size of this type in bytes
  u32 size_;
  // The alignment of this type in bytes
  u32 align_;
  // The unique ID of this type
  TypeId type_id_;
};

/// A builtin type
class BuiltinType : public Type {
 public:
#define F(BKind, ...) BKind,
  enum Kind : u16 { BUILTIN_TYPE_LIST(F, F, F) };
#undef F

  /// Get the name of the builtin as it appears in TPL code
  const char *tpl_name() const { return kTplNames[static_cast<u16>(kind_)]; }

  /// Get the name of the C++ type that backs this builtin. For primitive
  /// types like 32-bit integers, this will be 'int32'. For non-primitive types
  /// this will be the fully-qualified name of the class (i.e., the class name
  /// along with the namespace).
  const char *cpp_name() const { return kCppNames[static_cast<u16>(kind_)]; }

  /// Get the size of this builtin in bytes
  u64 size() const { return kSizes[static_cast<u16>(kind_)]; }

  /// Get the required alignment of this builtin in bytes
  u64 alignment() const { return kAlignments[static_cast<u16>(kind_)]; }

  /// Is this builtin a primitive?
  bool is_primitive() const { return kPrimitiveFlags[static_cast<u16>(kind_)]; }

  /// Is this builtin a primitive integer?
  bool is_integer() const { return Kind::Int8 <= kind() && kind() <= Kind::Uint128; }

  /// Is this builtin a primitive floating point number?
  bool is_floating_point() const { return kFloatingPointFlags[static_cast<u16>(kind_)]; }

  /// Return the kind of this builtin
  Kind kind() const { return kind_; }

  static BuiltinType *Get(Context *ctx, Kind kind);

  static bool classof(const Type *type) { return type->type_id() == TypeId::BuiltinType; }

 private:
  friend class Context;
  BuiltinType(Context *ctx, u32 size, u32 alignment, Kind kind)
      : Type(ctx, size, alignment, TypeId::BuiltinType), kind_(kind) {}

 private:
  Kind kind_;

 private:
  static const char *kCppNames[];
  static const char *kTplNames[];
  static const u64 kSizes[];
  static const u64 kAlignments[];
  static const bool kPrimitiveFlags[];
  static const bool kFloatingPointFlags[];
  static const bool kSignedFlags[];
};

/// String type
class StringType : public Type {
 public:
  static StringType *Get(Context *ctx);

  static bool classof(const Type *type) { return type->type_id() == TypeId::StringType; }

 private:
  friend class Context;
  explicit StringType(Context *ctx) : Type(ctx, sizeof(i8 *), alignof(i8 *), TypeId::StringType) {}
};

/// Pointer type
class PointerType : public Type {
 public:
  Type *base() const { return base_; }

  static PointerType *Get(Type *base);

  static bool classof(const Type *type) { return type->type_id() == TypeId::PointerType; }

 private:
  explicit PointerType(Type *base)
      : Type(base->context(), sizeof(i8 *), alignof(i8 *), TypeId::PointerType), base_(base) {}

 private:
  Type *base_;
};

/// Array type
class ArrayType : public Type {
 public:
  u64 length() const { return length_; }

  Type *element_type() const { return elem_type_; }

  static ArrayType *Get(u64 length, Type *elem_type);

  static bool classof(const Type *type) { return type->type_id() == TypeId::ArrayType; }

 private:
  explicit ArrayType(u64 length, Type *elem_type)
      : Type(elem_type->context(), elem_type->size() * length, elem_type->alignment(), TypeId::ArrayType),
        length_(length),
        elem_type_(elem_type) {}

 private:
  u64 length_;
  Type *elem_type_;
};

/// A field is a pair containing a name and a type. It is used to represent both
/// fields within a struct, and parameters to a function.
struct Field {
  Identifier name;
  Type *type;

  Field(const Identifier &name, Type *type) : name(name), type(type) {}

  bool operator==(const Field &other) const noexcept { return name == other.name && type == other.type; }
};

/// Function type
class FunctionType : public Type {
 public:
  const util::RegionVector<Field> &params() const { return params_; }

  u32 num_params() const { return static_cast<u32>(params().size()); }

  Type *return_type() const { return ret_; }

  static FunctionType *Get(util::RegionVector<Field> &&params, Type *ret);

  static bool classof(const Type *type) { return type->type_id() == TypeId::FunctionType; }

 private:
  explicit FunctionType(util::RegionVector<Field> &&params, Type *ret);

 private:
  util::RegionVector<Field> params_;
  Type *ret_;
};

/// Hash-map type
class MapType : public Type {
 public:
  Type *key_type() const { return key_type_; }

  Type *value_type() const { return val_type_; }

  static MapType *Get(Type *key_type, Type *value_type);

  static bool classof(const Type *type) { return type->type_id() == TypeId::MapType; }

 private:
  MapType(Type *key_type, Type *val_type);

 private:
  Type *key_type_;
  Type *val_type_;
};

/// Struct type
class StructType : public Type {
 public:
  const util::RegionVector<Field> &fields() const { return fields_; }

  Type *LookupFieldByName(Identifier name) const {
    for (const auto &field : fields()) {
      if (field.name == name) {
        return field.type;
      }
    }
    return nullptr;
  }

  u32 GetOffsetOfFieldByName(Identifier name) const {
    for (u32 i = 0; i < fields_.size(); i++) {
      if (fields_[i].name == name) {
        return field_offsets_[i];
      }
    }
    return 0;
  }

  bool IsLayoutIdentical(const StructType &other) const { return (this == &other || fields() == other.fields()); }

  static StructType *Get(Context *ctx, util::RegionVector<Field> &&fields);
  static StructType *Get(util::RegionVector<Field> &&fields);

  static bool classof(const Type *type) { return type->type_id() == TypeId::StructType; }

 private:
  explicit StructType(Context *ctx, u32 size, u32 alignment, util::RegionVector<Field> &&fields,
                      util::RegionVector<u32> &&field_offsets);

 private:
  util::RegionVector<Field> fields_;
  util::RegionVector<u32> field_offsets_;
};

// ---------------------------------------------------------
// Type implementation below
// ---------------------------------------------------------

inline Type *Type::GetPointeeType() const {
  if (auto *ptr_type = SafeAs<PointerType>()) {
    return ptr_type->base();
  }
  return nullptr;
}

inline bool Type::IsSpecificBuiltin(u16 kind) const {
  if (auto *builtin_type = SafeAs<BuiltinType>()) {
    return builtin_type->kind() == static_cast<BuiltinType::Kind>(kind);
  }
  return false;
}

inline bool Type::IsNilType() const { return IsSpecificBuiltin(BuiltinType::Nil); }

inline bool Type::IsBoolType() const { return IsSpecificBuiltin(BuiltinType::Bool); }

inline bool Type::IsIntegerType() const {
  if (auto *builtin_type = SafeAs<BuiltinType>()) {
    return builtin_type->is_integer();
  }
  return false;
}

inline bool Type::IsFloatType() const {
  if (auto *builtin_type = SafeAs<BuiltinType>()) {
    return builtin_type->is_floating_point();
  }
  return false;
}

}  // namespace tpl::ast
