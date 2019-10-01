#pragma once

#include <cstdint>
#include <string>

#include "llvm/Support/Casting.h"

#include "common/strong_typedef.h"
#include "execution/ast/identifier.h"
#include "execution/sql/deleter.h"
#include "execution/sql/inserter.h"
#include "execution/sql/projected_row_wrapper.h"
#include "execution/sql/updater.h"
#include "execution/util/region.h"
#include "execution/util/region_containers.h"

namespace terrier::execution::ast {

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
#define BUILTIN_TYPE_LIST(PRIM, NON_PRIM, SQL)                                                  \
  /* Primitive types */                                                                         \
  PRIM(Nil, uint8_t, "nil")                                                                     \
  PRIM(Bool, bool, "bool")                                                                      \
  PRIM(Int8, int8_t, "int8")                                                                    \
  PRIM(Int16, int16_t, "int16")                                                                 \
  PRIM(Int32, int32_t, "int32")                                                                 \
  PRIM(Int64, int64_t, "int64")                                                                 \
  PRIM(Uint8, uint8_t, "uint8")                                                                 \
  PRIM(Uint16, uint16_t, "uint16")                                                              \
  PRIM(Uint32, uint32_t, "uint32")                                                              \
  PRIM(Uint64, uint64_t, "uint64")                                                              \
  PRIM(Int128, int128_t, "int128")                                                              \
  PRIM(Uint128, uint128_t, "uint128")                                                           \
  PRIM(Float32, float, "float32")                                                               \
  PRIM(Float64, double, "float64")                                                              \
                                                                                                \
  /* Non-primitive builtins */                                                                  \
  NON_PRIM(AggregationHashTable, terrier::execution::sql::AggregationHashTable)                 \
  NON_PRIM(AggregationHashTableIterator, terrier::execution::sql::AggregationHashTableIterator) \
  NON_PRIM(AggOverflowPartIter, terrier::execution::sql::AggregationOverflowPartitionIterator)  \
  NON_PRIM(BloomFilter, terrier::execution::sql::BloomFilter)                                   \
  NON_PRIM(ExecutionContext, terrier::execution::exec::ExecutionContext)                        \
  NON_PRIM(FilterManager, terrier::execution::sql::FilterManager)                               \
  NON_PRIM(HashTableEntry, terrier::execution::sql::HashTableEntry)                             \
  NON_PRIM(JoinHashTable, terrier::execution::sql::JoinHashTable)                               \
  NON_PRIM(JoinHashTableVectorProbe, terrier::execution::sql::JoinHashTableVectorProbe)         \
  NON_PRIM(JoinHashTableIterator, terrier::execution::sql::JoinHashTableIterator)               \
  NON_PRIM(MemoryPool, terrier::execution::sql::MemoryPool)                                     \
  NON_PRIM(Sorter, terrier::execution::sql::Sorter)                                             \
  NON_PRIM(SorterIterator, terrier::execution::sql::SorterIterator)                             \
  NON_PRIM(TableVectorIterator, terrier::execution::sql::TableVectorIterator)                   \
  NON_PRIM(ThreadStateContainer, terrier::execution::sql::ThreadStateContainer)                 \
  NON_PRIM(ProjectedColumnsIterator, terrier::execution::sql::ProjectedColumnsIterator)         \
  NON_PRIM(IndexIterator, terrier::execution::sql::IndexIterator)                               \
                                                                                                \
  /* SQL Aggregate types (if you add, remember to update BuiltinType) */                        \
  NON_PRIM(CountAggregate, terrier::execution::sql::CountAggregate)                             \
  NON_PRIM(CountStarAggregate, terrier::execution::sql::CountStarAggregate)                     \
  NON_PRIM(IntegerAvgAggregate, terrier::execution::sql::AvgAggregate)                          \
  NON_PRIM(IntegerMaxAggregate, terrier::execution::sql::IntegerMaxAggregate)                   \
  NON_PRIM(IntegerMinAggregate, terrier::execution::sql::IntegerMinAggregate)                   \
  NON_PRIM(IntegerSumAggregate, terrier::execution::sql::IntegerSumAggregate)                   \
  NON_PRIM(RealAvgAggregate, terrier::execution::sql::AvgAggregate)                             \
  NON_PRIM(RealMaxAggregate, terrier::execution::sql::RealMaxAggregate)                         \
  NON_PRIM(RealMinAggregate, terrier::execution::sql::RealMinAggregate)                         \
  NON_PRIM(RealSumAggregate, terrier::execution::sql::RealSumAggregate)                         \
                                                                                                \
  /* SQL Table operations */                                                                    \
  NON_PRIM(ProjectedRow, terrier::execution::sql::ProjectedRowWrapper)                          \
  NON_PRIM(TupleSlot, terrier::storage::TupleSlot)                                              \
  NON_PRIM(Inserter, terrier::execution::sql::Inserter)                                         \
  NON_PRIM(Deleter, terrier::execution::sql::Deleter)                                           \
  NON_PRIM(Updater, terrier::execution::sql::Updater)                                           \
                                                                                                \
  /* Non-primitive SQL Runtime Values */                                                        \
  SQL(Boolean, terrier::execution::sql::BoolVal)                                                \
  SQL(Integer, terrier::execution::sql::Integer)                                                \
  SQL(Real, terrier::execution::sql::Real)                                                      \
  SQL(Decimal, terrier::execution::sql::Decimal)                                                \
  SQL(StringVal, terrier::execution::sql::StringVal)                                            \
  SQL(Date, terrier::execution::sql::Date)                                                      \
  SQL(Timestamp, terrier::execution::sql::Timestamp)

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

/**
 * The base of the TPL type hierarchy. Types, once created, are immutable. Only
 * one instance of a particular type is ever created, and all instances are
 * owned by the Context object that created it. Thus, one can use pointer
 * equality to determine if two types are equal, but only if they were created
 * within the same Context.
 */
class Type : public util::RegionObject {
 public:
  /**
   * The enumeration of all concrete types
   */
  enum class TypeId : uint8_t {
#define F(TypeId) TypeId,
    TYPE_LIST(F)
#undef F
  };

  /**
   * @return the context this type was allocated in
   */
  Context *GetContext() const { return ctx_; }

  /**
   * @return the size of this type in bytes
   */
  uint32_t Size() const { return size_; }

  /**
   * @return the alignment of this type in bytes
   */
  uint32_t Alignment() const { return align_; }

  /**
   * @return the unique type ID of this type (e.g., int16, Array, Struct etc.)
   */
  TypeId GetTypeId() const { return type_id_; }

  /**
   * Perform an "checked cast" to convert an instance of this base Type class
   * into one of its derived types. If the target class isn't a subclass of
   * Type, an assertion failure is thrown in debug mode. In release mode, such
   * a call will fail.
   *
   * You should use this function when you have reasonable certainty that you
   * know the concrete type. Example:
   *
   * @code
   * if (!type->IsBuiltinType()) {
   *   return;
   * }
   * ...
   * auto *builtin_type = type->As<ast::BuiltinType>();
   * ...
   * @endcode
   *
   * @tparam T type to cast to
   * @return casted pointer
   */
  template <typename T>
  const T *As() const {
    return llvm::cast<const T>(this);
  }

  /**
   * Perform an "checked cast" to convert an instance of this base Type class
   * into one of its derived types.
   * @tparam T type to cast to
   * @return casted pointer
   */
  template <typename T>
  T *As() {
    return llvm::cast<T>(this);
  }

  /**
   * Perform a "checking cast". This function checks to see if the target type
   * is a subclass of Type, returning a pointer to the subclass if so, or
   * returning a null pointer otherwise.
   *
   * You should use this in conditional or control-flow statements when you
   * want to check if a type is a specific subtype **AND** get a pointer to the
   * subtype, like so:
   *
   * @code
   * if (auto *builtin_type = SafeAs<ast::BuiltinType>()) {
   *   // ...
   * }
   * @endcode
   *
   * @tparam T type to cast to.
   * @return casted pointer.
   */
  template <typename T>
  const T *SafeAs() const {
    return llvm::dyn_cast<const T>(this);
  }

  /**
   * Perform a "checking cast". This function checks to see if the target type
   * is a subclass of Type, returning a pointer to the subclass if so, or
   * returning a null pointer otherwise.
   * @tparam T type to cast to.
   * @return casted pointer.
   */
  template <typename T>
  T *SafeAs() {
    return llvm::dyn_cast<T>(this);
  }

  /*
   * Type checks
   */
#define F(TypeClass) \
  bool Is##TypeClass() const { return llvm::isa<TypeClass>(this); }
  TYPE_LIST(F)
#undef F

  /**
   * Checks whether this is an arithmetic type
   * @return true iff this is an arithmetic type.
   */
  bool IsArithmetic() const;

  /**
   * Checks whether this is of the given builtin kind
   * @param kind The kind to check
   * @return true iff this is of the given kind.
   */
  bool IsSpecificBuiltin(uint16_t kind) const;

  /**
   * Checks whether this is a nil type
   * @return true iff this is a nil type.
   */
  bool IsNilType() const;

  /**
   * Checks whether this is a bool type
   * @return true iff this is a bool type.
   */
  bool IsBoolType() const;

  /**
   * Checks whether this is an integer type
   * @return true iff this is an integer type.
   */
  bool IsIntegerType() const;

  /**
   * Checks whether this is a float type
   * @return true iff this is a float type.
   */
  bool IsFloatType() const;

  /**
   * Checks whether this is a sql value type
   * @return true iff this is a sql value type.
   */
  bool IsSqlValueType() const;

  /**
   * Checks whether this is a sql aggregator type
   * @return true iff this is a sql aggregator type.
   */
  bool IsSqlAggregatorType() const;

  /**
   * @return a type that is a pointer to the current type
   */
  PointerType *PointerTo();

  /**
   * @return If this is a pointer type, return the type it points to, returning null otherwise.
   */
  Type *GetPointeeType() const;

  /**
   * @return a string representation of the given type
   */
  std::string ToString() const { return ToString(this); }

  /**
   * Get a string representation of the input type
   * @param type type to represent
   * @return a string representation of the fiven type
   */
  static std::string ToString(const Type *type);

 protected:
  /**
   * Protected constructor to indicate abstract base
   * @param ctx ast context to use
   * @param size size of the type
   * @param alignment alignment of the type
   * @param type_id id of the type
   */
  Type(Context *ctx, uint32_t size, uint32_t alignment, TypeId type_id)
      : ctx_(ctx), size_(size), align_(alignment), type_id_(type_id) {}

 private:
  // The context this type was created/unique'd in
  Context *ctx_;
  // The size of this type in bytes
  uint32_t size_;
  // The alignment of this type in bytes
  uint32_t align_;
  // The unique ID of this type
  TypeId type_id_;
};

/**
 * A builtin type (int32, float32, Integer, JoinHashTable etc.)
 */
class BuiltinType : public Type {
 public:
#define F(BKind, ...) BKind,
  /**
   * Enum of builtin types
   */
  enum Kind : uint16_t { BUILTIN_TYPE_LIST(F, F, F) };
#undef F

  /**
   * Get the name of the builtin as it appears in TPL code
   * @return name of the builtin
   */
  const char *TplName() const { return tpl_names[static_cast<uint16_t>(kind_)]; }

  /**
   * Get the name of the C++ type that backs this builtin. For primitive
   * types like 32-bit integers, this will be 'int32'. For non-primitive types
   * this will be the fully-qualified name of the class (i.e., the class name
   * along with the namespace).
   * @return the C++ type name
   */
  const char *CppName() const { return cpp_names[static_cast<uint16_t>(kind_)]; }

  /**
   * Get the size of this builtin in bytes
   * @return size of the builtin type
   */
  uint64_t Size() const { return SIZES[static_cast<uint16_t>(kind_)]; }

  /**
   * Get the required alignment of this builtin in bytes
   * @return alignment of this builtin type
   */
  uint64_t Alignment() const { return ALIGNMENTS[static_cast<uint16_t>(kind_)]; }

  /**
   * @return Is this builtin a primitive?
   */
  bool IsPrimitive() const { return PRIMITIVE_FLAGS[static_cast<uint16_t>(kind_)]; }

  /**
   * @return Is this builtin a primitive integer?
   */
  bool IsInteger() const { return Kind::Int8 <= GetKind() && GetKind() <= Kind::Uint128; }

  /**
   * @return Is this builtin a primitive floating point number?
   */
  bool IsFloatingPoint() const { return FLOATING_POINT_FLAGS[static_cast<uint16_t>(kind_)]; }

  /**
   * Is this type a SQL value type?
   */
  bool IsSqlValue() const { return Kind::Boolean <= GetKind() && GetKind() <= Kind::Timestamp; }

  /**
   * Is this type a SQL aggregator type? IntegerSumAggregate, CountAggregate ...
   */
  bool IsSqlAggregatorType() const { return Kind::CountAggregate <= GetKind() && GetKind() <= Kind::RealSumAggregate; }

  /**
   * @return the kind of this builtin
   */
  Kind GetKind() const { return kind_; }

  /**
   * Return a builtin of the given kind.
   * @param ctx ast context to use
   * @param kind kind to get
   * @return a builtin of the given kind.
   */
  static BuiltinType *Get(Context *ctx, Kind kind);

  /**
   * Checks if this is the same as the given type
   * @param type type to check
   * @return true iff this is the same as the given type
   */
  static bool classof(const Type *type) { return type->GetTypeId() == TypeId::BuiltinType; }  // NOLINT

 private:
  friend class Context;
  // Private constructor
  BuiltinType(Context *ctx, uint32_t size, uint32_t alignment, Kind kind)
      : Type(ctx, size, alignment, TypeId::BuiltinType), kind_(kind) {}

 private:
  Kind kind_;

 private:
  static const char *cpp_names[];
  static const char *tpl_names[];
  static const uint64_t SIZES[];
  static const uint64_t ALIGNMENTS[];
  static const bool PRIMITIVE_FLAGS[];
  static const bool FLOATING_POINT_FLAGS[];
  static const bool SIGNED_FLAGS[];
};

/**
 * String type
 */
class StringType : public Type {
 public:
  /**
   * Static Constructor
   * @param ctx ast context to use
   * @return a string type
   */
  static StringType *Get(Context *ctx);

  /**
   * @param type checked type
   * @return whether type is a string type.
   */
  static bool classof(const Type *type) { return type->GetTypeId() == TypeId::StringType; }  // NOLINT

 private:
  friend class Context;
  // Private constructor
  explicit StringType(Context *ctx) : Type(ctx, sizeof(int8_t *), alignof(int8_t *), TypeId::StringType) {}
};

/**
 * Pointer type
 */
class PointerType : public Type {
 public:
  /**
   * @return base type
   */
  Type *Base() const { return base_; }

  /**
   * Static Constructor
   * @param base type
   * @return pointer to base type
   */
  static PointerType *Get(Type *base);

  /**
   * @param type checked type
   * @return whether type is a pointer type.
   */
  static bool classof(const Type *type) { return type->GetTypeId() == TypeId::PointerType; }  // NOLINT

 private:
  // Private constructor
  explicit PointerType(Type *base)
      : Type(base->GetContext(), sizeof(int8_t *), alignof(int8_t *), TypeId::PointerType), base_(base) {}

 private:
  Type *base_;
};

/**
 * Array type
 */
class ArrayType : public Type {
 public:
  /**
   * @return length of the array
   */
  uint64_t Length() const { return length_; }

  /**
   * @return element type
   */
  Type *ElementType() const { return elem_type_; }

  /**
   * @return whether the length is known
   */
  bool HasKnownLength() const { return length_ != 0; }

  /**
   * @return whether the length is unknown
   */
  bool HasUnknownLength() const { return !HasKnownLength(); }

  /**
   * Construction
   * @param length of the array
   * @param elem_type element type
   * @return the array type
   */
  static ArrayType *Get(uint64_t length, Type *elem_type);

  /**
   * @return whether type is an array type.
   */
  static bool classof(const Type *type) { return type->GetTypeId() == TypeId::ArrayType; }  // NOLINT

 private:
  // Private constructor
  explicit ArrayType(uint64_t length, Type *elem_type)
      : Type(elem_type->GetContext(),
             (length == 0 ? sizeof(uint8_t *) : elem_type->Size() * static_cast<uint32_t>(length)),
             (length == 0 ? alignof(uint8_t *) : elem_type->Alignment()), TypeId::ArrayType),
        length_(length),
        elem_type_(elem_type) {}

 private:
  uint64_t length_;
  Type *elem_type_;
};

/**
 * A field is a pair containing a name and a type. It is used to represent both fields within a struct, and parameters
 * to a function.
 */
struct Field {
  /**
   * Name of the field
   */
  Identifier name_;

  /**
   * Type of the field
   */
  Type *type_;

  /**
   * Constructor
   * @param name of the field
   * @param type of the field
   */
  Field(const Identifier &name, Type *type) : name_(name), type_(type) {}

  /**
   * @param other rhs of the comparison
   * @return whether this == other
   */
  bool operator==(const Field &other) const noexcept { return name_ == other.name_ && type_ == other.type_; }
};

/**
 * Function type
 */
class FunctionType : public Type {
 public:
  /**
   * @return list of parameters
   */
  const util::RegionVector<Field> &Params() const { return params_; }

  /**
   * @return numner of parameters
   */
  uint32_t NumParams() const { return static_cast<uint32_t>(Params().size()); }

  /**
   * @return return type of the function
   */
  Type *ReturnType() const { return ret_; }

  /**
   * Static Constructor
   * @param params list of parameters
   * @param ret return type
   * @return the function type
   */
  static FunctionType *Get(util::RegionVector<Field> &&params, Type *ret);

  /**
   * @param type type to compare with
   * @return whether type is of function type
   */
  static bool classof(const Type *type) { return type->GetTypeId() == TypeId::FunctionType; }  // NOLINT

 private:
  // Private constructor
  explicit FunctionType(util::RegionVector<Field> &&params, Type *ret);

 private:
  util::RegionVector<Field> params_;
  Type *ret_;
};

/**
 * Hash-map type
 */
class MapType : public Type {
 public:
  /**
   * @return type of the keys
   */
  Type *KeyType() const { return key_type_; }

  /**
   * @return type of values
   */
  Type *ValueType() const { return val_type_; }

  /**
   * Static Constructor
   * @param key_type type of the keys
   * @param value_type type of the values
   * @return constructed map type
   */
  static MapType *Get(Type *key_type, Type *value_type);

  /**
   * @param type to compare with
   * @return whether type is of map type.
   */
  static bool classof(const Type *type) { return type->GetTypeId() == TypeId::MapType; }  // NOLINT

 private:
  // Private Constructor
  MapType(Type *key_type, Type *val_type);

 private:
  Type *key_type_;
  Type *val_type_;
};

/**
 * Struct type
 */
class StructType : public Type {
 public:
  /**
   * @return list of fields
   */
  const util::RegionVector<Field> &Fields() const { return fields_; }

  /**
   * @param name field to lookup
   * @return type of the field
   */
  Type *LookupFieldByName(Identifier name) const {
    for (const auto &field : Fields()) {
      if (field.name_ == name) {
        return field.type_;
      }
    }
    return nullptr;
  }

  /**
   * @param name field of to lookup
   * @return offset of the field
   */
  uint32_t GetOffsetOfFieldByName(Identifier name) const {
    for (uint32_t i = 0; i < fields_.size(); i++) {
      if (fields_[i].name_ == name) {
        return field_offsets_[i];
      }
    }
    return 0;
  }

  /**
   * @param other struct to compare to
   * @return whether this and other are identical
   */
  bool IsLayoutIdentical(const StructType &other) const { return (this == &other || Fields() == other.Fields()); }

  /**
   * Note: fields cannot be empty!
   * Static constructor
   * @param ctx ast context
   * @param fields list of types
   * @return constructor type
   */
  static StructType *Get(Context *ctx, util::RegionVector<Field> &&fields);

  /**
   * Static constructor
   * @param fields list of types
   * @return constructor type
   */
  static StructType *Get(util::RegionVector<Field> &&fields);

  /**
   * @param type checked type
   * @return whether type is a struct type.
   */
  static bool classof(const Type *type) { return type->GetTypeId() == TypeId::StructType; }  // NOLINT

 private:
  // Private constructor
  explicit StructType(Context *ctx, uint32_t size, uint32_t alignment, util::RegionVector<Field> &&fields,
                      util::RegionVector<uint32_t> &&field_offsets);

 private:
  util::RegionVector<Field> fields_;
  util::RegionVector<uint32_t> field_offsets_;
};

// ---------------------------------------------------------
// Type implementation below
// ---------------------------------------------------------

inline Type *Type::GetPointeeType() const {
  if (auto *ptr_type = SafeAs<PointerType>()) {
    return ptr_type->Base();
  }
  return nullptr;
}

inline bool Type::IsSpecificBuiltin(uint16_t kind) const {
  if (auto *builtin_type = SafeAs<BuiltinType>()) {
    return builtin_type->GetKind() == static_cast<BuiltinType::Kind>(kind);
  }
  return false;
}

inline bool Type::IsNilType() const { return IsSpecificBuiltin(BuiltinType::Nil); }

inline bool Type::IsBoolType() const { return IsSpecificBuiltin(BuiltinType::Bool); }

inline bool Type::IsIntegerType() const {
  if (auto *builtin_type = SafeAs<BuiltinType>()) {
    return builtin_type->IsInteger();
  }
  return false;
}

inline bool Type::IsFloatType() const {
  if (auto *builtin_type = SafeAs<BuiltinType>()) {
    return builtin_type->IsFloatingPoint();
  }
  return false;
}

inline bool Type::IsSqlValueType() const {
  if (auto *builtin_type = SafeAs<BuiltinType>()) {
    return builtin_type->IsSqlValue();
  }
  return false;
}

inline bool Type::IsSqlAggregatorType() const {
  if (auto *builtin_type = SafeAs<BuiltinType>()) {
    return builtin_type->IsSqlAggregatorType();
  }
  return false;
}

}  // namespace terrier::execution::ast
