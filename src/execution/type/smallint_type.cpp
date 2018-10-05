//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// smallint_type.cpp
//
// Identification: src/execution/type/smallint_type.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/type/smallint_type.h"

#include "common/exception.h"
#include "execution/lang/if.h"
#include "execution/proxy/numeric_functions_proxy.h"
#include "execution/proxy/values_runtime_proxy.h"
#include "execution/type/boolean_type.h"
#include "execution/type/decimal_type.h"
#include "execution/type/integer_type.h"
#include "execution/value.h"
#include "runtime/string_util.h"
#include "type/limits.h"

namespace terrier::execution {

namespace type {

namespace {

/** Forward declarations:
 *  Subtraction
 */
struct Sub : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type, const Type &right_type) const override;

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type, UNUSED_ATTRIBUTE const Type &right_type) const override;

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Function tables
///
/// We do SMALLINT -> {INTEGRAL_TYPE, DECIMAL, VARCHAR, BOOLEAN}
///
////////////////////////////////////////////////////////////////////////////////

struct CastSmallInt : public TypeSystem::CastHandleNull {
  bool SupportsTypes(const Type &from_type, const Type &to_type) const override {
    if (from_type.GetSqlType() != SmallInt::Instance()) {
      return false;
    }
    switch (to_type.GetSqlType().TypeId()) {
      case ::terrier::type::TypeId::BOOLEAN:
      case ::terrier::type::TypeId::TINYINT:
      case ::terrier::type::TypeId::SMALLINT:
      case ::terrier::type::TypeId::INTEGER:
      case ::terrier::type::TypeId::BIGINT:
      case ::terrier::type::TypeId::DECIMAL:
      case ::terrier::type::TypeId::VARCHAR:
        return true;
      default:
        return false;
    }
  }

  Value Impl(CodeGen &codegen, const Value &value, const Type &to_type) const override {
    llvm::Value *result = nullptr;
    switch (to_type.GetSqlType().TypeId()) {
      case ::terrier::type::TypeId::BOOLEAN: {
        result = codegen->CreateTrunc(value.GetValue(), codegen.BoolType());
        break;
      }
      case ::terrier::type::TypeId::TINYINT: {
        result = codegen->CreateTrunc(value.GetValue(), codegen.Int8Type());
        break;
      }
      case ::terrier::type::TypeId::SMALLINT: {
        result = value.GetValue();
        break;
      }
      case ::terrier::type::TypeId::INTEGER: {
        result = codegen->CreateSExt(value.GetValue(), codegen.Int32Type());
        break;
      }
      case ::terrier::type::TypeId::BIGINT: {
        result = codegen->CreateSExt(value.GetValue(), codegen.Int64Type());
        break;
      }
      case ::terrier::type::TypeId::DECIMAL: {
        result = codegen->CreateSIToFP(value.GetValue(), codegen.DoubleType());
        break;
      }
      case ::terrier::type::TypeId::VARCHAR: {
        result = codegen->CreateTrunc(value.GetValue(), codegen.BoolType());
        break;
      }
      default: {
        throw Exception{StringUtil::Format("Cannot cast %s to %s", TypeIdToString(value.GetType().type_id).c_str(),
                                           TypeIdToString(to_type.type_id).c_str())};
      }
    }

    // We could be casting this non-nullable value to a nullable type
    llvm::Value *null = to_type.nullable ? codegen.ConstBool(false) : nullptr;

    // Return the result
    return Value{to_type, result, nullptr, null};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Comparisons
///
////////////////////////////////////////////////////////////////////////////////

struct CompareSmallInt : public TypeSystem::SimpleComparisonHandleNull {
  bool SupportsTypes(const Type &left_type, const Type &right_type) const override {
    return left_type == SmallInt::Instance() && left_type == right_type;
  }

  Value CompareLtImpl(CodeGen &codegen, const Value &left, const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSLT(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareLteImpl(CodeGen &codegen, const Value &left, const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSLE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareEqImpl(CodeGen &codegen, const Value &left, const Value &right) const override {
    auto *raw_val = codegen->CreateICmpEQ(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareNeImpl(CodeGen &codegen, const Value &left, const Value &right) const override {
    auto *raw_val = codegen->CreateICmpNE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareGtImpl(CodeGen &codegen, const Value &left, const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSGT(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareGteImpl(CodeGen &codegen, const Value &left, const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSGE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareForSortImpl(CodeGen &codegen, const Value &left, const Value &right) const override {
    // For integer comparisons, just subtract left from right and cast the
    // result to a 32-bit value
    llvm::Value *diff = codegen->CreateSub(left.GetValue(), right.GetValue());
    return Value{Integer::Instance(), codegen->CreateSExt(diff, codegen.Int32Type()), nullptr, nullptr};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Unary operations
///
////////////////////////////////////////////////////////////////////////////////

// Abs
struct Abs : public TypeSystem::UnaryOperatorHandleNull {
  bool SupportsType(const Type &type) const override { return type.GetSqlType() == SmallInt::Instance(); }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override { return Type{SmallInt::Instance()}; }

  Value Impl(CodeGen &codegen, const Value &val, const TypeSystem::InvocationContext &ctx) const override {
    // The smallint subtraction implementation
    Sub sub;
    TERRIER_ASSERT(SupportsType(val.GetType()), "We must support the desired type.");
    // Zero place-holder
    auto zero = Value{type::SmallInt::Instance(), codegen.Const16(0)};

    // We want: raw_ret = (val < 0 ? 0 - val : val)
    auto sub_result = sub.Impl(codegen, zero, val, ctx);
    auto *lt_zero = codegen->CreateICmpSLT(val.GetValue(), zero.GetValue());
    auto *raw_ret = codegen->CreateSelect(lt_zero, sub_result.GetValue(), val.GetValue());
    return Value{SmallInt::Instance(), raw_ret};
  }
};

// Negation
struct Negate : public TypeSystem::UnaryOperatorHandleNull {
  bool SupportsType(const Type &type) const override { return type.GetSqlType() == SmallInt::Instance(); }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override { return Type{SmallInt::Instance()}; }

  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx) const override {
    TERRIER_ASSERT(SupportsType(val.GetType()), "We must support the desired type.");

    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallSubWithOverflow(codegen.Const16(0), val.GetValue(), overflow_bit);

    codegen.ThrowIfOverflow(overflow_bit);

    // Return result
    return Value{SmallInt::Instance(), result, nullptr, nullptr};
  }
};

// Floor
struct Floor : public TypeSystem::UnaryOperatorHandleNull {
  CastSmallInt cast;

  bool SupportsType(const Type &type) const override { return type.GetSqlType() == SmallInt::Instance(); }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override { return Type{Decimal::Instance()}; }

  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx) const override {
    TERRIER_ASSERT(SupportsType(val.GetType()), "We must support the desired type.");
    return cast.Impl(codegen, val, Decimal::Instance());
  }
};

// Ceiling
struct Ceil : public TypeSystem::UnaryOperatorHandleNull {
  CastSmallInt cast;

  bool SupportsType(const Type &type) const override { return type.GetSqlType() == SmallInt::Instance(); }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override { return Type{Decimal::Instance()}; }

  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx) const override {
    TERRIER_ASSERT(SupportsType(val.GetType()), "We must support the desired type.");
    return cast.Impl(codegen, val, Decimal::Instance());
  }
};

// Sqrt
struct Sqrt : public TypeSystem::UnaryOperatorHandleNull {
  CastSmallInt cast;

  bool SupportsType(const Type &type) const override { return type.GetSqlType() == SmallInt::Instance(); }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override { return Decimal::Instance(); }

 protected:
  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx) const override {
    auto casted = cast.Impl(codegen, val, Decimal::Instance());
    auto *raw_ret = codegen.Sqrt(casted.GetValue());
    return Value{Decimal::Instance(), raw_ret};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Binary operations
///
////////////////////////////////////////////////////////////////////////////////

// Addition
struct Add : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type, const Type &right_type) const override {
    return left_type.GetSqlType() == SmallInt::Instance() && left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type, UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{SmallInt::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    TERRIER_ASSERT(SupportsTypes(left.GetType(), right.GetType()), "We must support the desired types.");

    // Do addition
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallAddWithOverflow(left.GetValue(), right.GetValue(), overflow_bit);

    if (ctx.on_error == OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{SmallInt::Instance(), result, nullptr, nullptr};
  }
};

// Subtraction

bool Sub::SupportsTypes(const Type &left_type, const Type &right_type) const {
  return left_type.GetSqlType() == SmallInt::Instance() && left_type == right_type;
}

Type Sub::ResultType(UNUSED_ATTRIBUTE const Type &left_type, UNUSED_ATTRIBUTE const Type &right_type) const {
  return Type{SmallInt::Instance()};
}

Value Sub::Impl(CodeGen &codegen, const Value &left, const Value &right,
                const TypeSystem::InvocationContext &ctx) const {
  TERRIER_ASSERT(SupportsTypes(left.GetType(), right.GetType()), "We must support the desired types.");

  // Do subtraction
  llvm::Value *overflow_bit = nullptr;
  llvm::Value *result = codegen.CallSubWithOverflow(left.GetValue(), right.GetValue(), overflow_bit);

  if (ctx.on_error == OnError::Exception) {
    codegen.ThrowIfOverflow(overflow_bit);
  }

  // Return result
  return Value{SmallInt::Instance(), result};
};

// Multiplication
struct Mul : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type, const Type &right_type) const override {
    return left_type.GetSqlType() == SmallInt::Instance() && left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type, UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{SmallInt::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    TERRIER_ASSERT(SupportsTypes(left.GetType(), right.GetType()), "We must support the desired types.");

    // Do multiplication
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallMulWithOverflow(left.GetValue(), right.GetValue(), overflow_bit);

    if (ctx.on_error == OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{SmallInt::Instance(), result};
  }
};

// Division
struct Div : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type, const Type &right_type) const override {
    return left_type.GetSqlType() == SmallInt::Instance() && left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type, UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{SmallInt::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    TERRIER_ASSERT(SupportsTypes(left.GetType(), right.GetType()), "We must support the desired types.");

    // First, check if the divisor is zero
    auto *div0 = codegen->CreateICmpEQ(right.GetValue(), codegen.Const16(0));

    // Check if the caller cares about division-by-zero errors

    auto result = Value{SmallInt::Instance()};

    if (ctx.on_error == OnError::ReturnNull) {
      Value default_val, division_result;
      lang::If is_div0{codegen, div0, "div0"};
      {
        // The divisor is 0, return NULL because that's what the caller wants
        default_val = SmallInt::Instance().GetNullValue(codegen);
      }
      is_div0.ElseBlock();
      {
        // The divisor isn't 0, do the division
        auto *raw_val = codegen->CreateSDiv(left.GetValue(), right.GetValue());
        division_result = Value{SmallInt::Instance(), raw_val, nullptr, nullptr};
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);

    } else if (ctx.on_error == OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0);

      // Do division
      auto *raw_val = codegen->CreateSDiv(left.GetValue(), right.GetValue());
      result = Value{SmallInt::Instance(), raw_val, nullptr, nullptr};
    }

    // Return result
    return result;
  }
};

// Modulo
struct Modulo : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type, const Type &right_type) const override {
    return left_type.GetSqlType() == SmallInt::Instance() && left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type, UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{SmallInt::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    TERRIER_ASSERT(SupportsTypes(left.GetType(), right.GetType()), "We must support the desired types.");

    // First, check if the divisor is zero
    auto *div0 = codegen->CreateICmpEQ(right.GetValue(), codegen.Const16(0));

    // Check if the caller cares about division-by-zero errors

    auto result = Value{SmallInt::Instance()};

    if (ctx.on_error == OnError::ReturnNull) {
      Value default_val, division_result;
      lang::If is_div0{codegen, div0, "div0"};
      {
        // The divisor is 0, return NULL because that's what the caller wants
        default_val = SmallInt::Instance().GetNullValue(codegen);
      }
      is_div0.ElseBlock();
      {
        // The divisor isn't 0, do the division
        auto *raw_val = codegen->CreateSRem(left.GetValue(), right.GetValue());
        division_result = Value{SmallInt::Instance(), raw_val, nullptr, nullptr};
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);

    } else if (ctx.on_error == OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0);

      // Do division
      auto *raw_val = codegen->CreateSRem(left.GetValue(), right.GetValue());
      result = Value{SmallInt::Instance(), raw_val, nullptr, nullptr};
    }

    // Return result
    return result;
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Function tables
///
////////////////////////////////////////////////////////////////////////////////

// Implicit casts
std::vector<::terrier::type::TypeId> kImplicitCastingTable = {
    ::terrier::type::TypeId::SMALLINT, ::terrier::type::TypeId::INTEGER, ::terrier::type::TypeId::BIGINT,
    ::terrier::type::TypeId::DECIMAL};

// clang-format off
// Explicit casting rules
CastSmallInt kCastSmallInt;
std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {
    {::terrier::type::TypeId::SMALLINT, ::terrier::type::TypeId::BOOLEAN, kCastSmallInt},
    {::terrier::type::TypeId::SMALLINT, ::terrier::type::TypeId::TINYINT, kCastSmallInt},
    {::terrier::type::TypeId::SMALLINT, ::terrier::type::TypeId::SMALLINT, kCastSmallInt},
    {::terrier::type::TypeId::SMALLINT, ::terrier::type::TypeId::INTEGER, kCastSmallInt},
    {::terrier::type::TypeId::SMALLINT, ::terrier::type::TypeId::BIGINT, kCastSmallInt},
    {::terrier::type::TypeId::SMALLINT, ::terrier::type::TypeId::DECIMAL, kCastSmallInt}};
// clang-format on

// Comparison operations
CompareSmallInt kCompareSmallInt;
std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {{kCompareSmallInt}};

// Unary operators
Negate kNegOp;
Abs kAbsOp;
Ceil kCeilOp;
Floor kFloorOp;
Sqrt kSqrtOp;
std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {{OperatorId::Negation, kNegOp},
                                                            {OperatorId::Abs, kAbsOp},
                                                            {OperatorId::Ceil, kCeilOp},
                                                            {OperatorId::Floor, kFloorOp},
                                                            {OperatorId::Sqrt, kSqrtOp}};

// Binary operations
Add kAddOp;
Sub kSubOp;
Mul kMulOp;
Div kDivOp;
Modulo kModuloOp;
std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {{OperatorId::Add, kAddOp},
                                                              {OperatorId::Sub, kSubOp},
                                                              {OperatorId::Mul, kMulOp},
                                                              {OperatorId::Div, kDivOp},
                                                              {OperatorId::Mod, kModuloOp}};

// Nary operations
std::vector<TypeSystem::NaryOpInfo> kNaryOperatorTable = {};

// NoArg operators
std::vector<TypeSystem::NoArgOpInfo> kNoArgOperatorTable = {};

}  // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
///
/// SMALLINT type initialization and configuration
///
////////////////////////////////////////////////////////////////////////////////

SmallInt::SmallInt()
    : SqlType(::terrier::type::TypeId::SMALLINT),
      type_system_(kImplicitCastingTable, kExplicitCastingTable, kComparisonTable, kUnaryOperatorTable,
                   kBinaryOperatorTable, kNaryOperatorTable, kNoArgOperatorTable) {}

Value SmallInt::GetMinValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const16(peloton::type::PELOTON_INT16_MIN);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value SmallInt::GetMaxValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const16(peloton::type::PELOTON_INT16_MAX);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value SmallInt::GetNullValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const16(peloton::type::PELOTON_INT16_NULL);
  return Value{Type{TypeId(), true}, raw_val, nullptr, codegen.ConstBool(true)};
}

void SmallInt::GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type, llvm::Type *&len_type) const {
  val_type = codegen.Int16Type();
  len_type = nullptr;
}

llvm::Function *SmallInt::GetInputFunction(CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const {
  return NumericFunctionsProxy::InputSmallInt.GetFunction(codegen);
}

llvm::Function *SmallInt::GetOutputFunction(CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const {
  return ValuesRuntimeProxy::OutputSmallInt.GetFunction(codegen);
}

}  // namespace type

}  // namespace terrier::execution
