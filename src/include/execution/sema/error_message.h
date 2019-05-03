#pragma once

#include <cstdint>

#include "execution/ast/identifier.h"
#include "execution/util/common.h"

namespace tpl {

namespace ast {
class Type;
}  // namespace ast

namespace sema {

// The following macro lists all the semantic and syntactic error messages in
// TPL. Each macro has three parts: a unique message ID, a templated string
// error message that will be displayed, and the types of each template argument
#define MESSAGE_LIST(F)                                                                                               \
  F(UnexpectedToken, "unexpected token '%0', expecting '%1'", (parsing::Token::Type, parsing::Token::Type))           \
  F(DuplicateArgName, "duplicate named argument '%0' in function '%0'", (ast::Identifier))                            \
  F(DuplicateStructFieldName, "duplicate field name '%0' in struct '%1'", (ast::Identifier, ast::Identifier))         \
  F(AssignmentUsedAsValue, "assignment '%0' = '%1' used as value", (ast::Identifier, ast::Identifier))                \
  F(ExpectingExpression, "expecting expression", ())                                                                  \
  F(ExpectingType, "expecting type", ())                                                                              \
  F(InvalidOperation, "invalid operation: '%0' on type '%1'", (parsing::Token::Type, ast::Type *))                    \
  F(VariableRedeclared, "'%0' redeclared in this block", (ast::Identifier))                                           \
  F(UndefinedVariable, "undefined: '%0'", (ast::Identifier))                                                          \
  F(NonFunction, "cannot call non-function '%0'", ())                                                                 \
  F(MismatchedCallArgs, "Wrong number of arguments in call to '%0'. Expected %1, received %2.",                       \
    (ast::Identifier, u32, u32))                                                                                      \
  F(IncorrectCallArgType, "cannot use a '%0' as type '%1' in argument to '%2'",                                       \
    (ast::Type *, ast::Type *, ast::Identifier))                                                                      \
  F(NonBoolIfCondition, "non-bool used as if condition", ())                                                          \
  F(NonBoolForCondition, "non-bool used as for condition", ())                                                        \
  F(NonIntegerArrayLength, "non-integer literal used as array size", ())                                              \
  F(NegativeArrayLength, "array bound must be non-negative", ())                                                      \
  F(ReturnOutsideFunction, "return outside function", ())                                                             \
  F(MissingTypeAndInitialValue, "variable '%0' must have either a declared type or an initial value",                 \
    (ast::Identifier))                                                                                                \
  F(IllegalTypesForBinary, "binary operation '%0' does not support types '%1' and '%2'",                              \
    (parsing::Token::Type, ast::Type *, ast::Type *))                                                                 \
  F(MismatchedTypesToBinary, "mismatched types '%0' and '%1' to binary operation '%2'",                               \
    (ast::Type *, ast::Type *, parsing::Token::Type))                                                                 \
  F(MismatchedReturnType,                                                                                             \
    "type of 'return' expression '%0' incompatible with function's declared "                                         \
    "return type '%1'",                                                                                               \
    (ast::Type *, ast::Type *))                                                                                       \
  F(NonIdentifierIterator, "expected identifier for table iteration variable", ())                                    \
  F(NonIdentifierTargetInForInLoop, "target of for-in loop must be an identifier", ())                                \
  F(NonExistingTable, "table with name '%0' does not exist", (ast::Identifier))                                       \
  F(ExpectedIdentifierForMember, "expected identifier for member expression", ())                                     \
  F(MemberObjectNotComposite, "object of member expression has type ('%0') which is not a composite", (ast::Type *))  \
  F(FieldObjectDoesNotExist, "no field with name '%0' exists in composite type '%1'", (ast::Identifier, ast::Type *)) \
  F(InvalidIndexOperation, "invalid operation: type '%0' does not support indexing", (ast::Type *))                   \
  F(InvalidArrayIndexValue, "non-integer array index", ())                                                            \
  F(InvalidCastToSqlInt, "invalid cast of %0 to SQL integer", (ast::Type *))                                          \
  F(InvalidCastToSqlDecimal, "invalid cast of %0 to SQL decimal", (ast::Type *))                                      \
  F(InvalidSqlCastToBool, "invalid input to cast to native boolean: expected SQL boolean, got %0", (ast::Type *))     \
  F(MissingReturn, "missing return at end of function", ())                                                           \
  F(InvalidDeclaration, "non-declaration outside function", ())                                                       \
  F(BadArgToBuiltin,                                                                                                  \
    "builtin '%0' expects argument of type '%1' in position '%2'. Received "                                          \
    "type '%3'",                                                                                                      \
    (ast::Identifier, ast::Type *, u32, ast::Type *))                                                                 \
  F(BadComparisonFunctionForSorter,                                                                                   \
    "sorterInit requires a comparison function of type (*,*)->int32. "                                                \
    "Received type '%0'",                                                                                             \
    (ast::Type *))                                                                                                    \
  F(BadArgToPtrCast,                                                                                                  \
    "ptrCast() expects (compile-time *DestType, *T) arguments.  Received "                                            \
    "type '%0' in position %1",                                                                                       \
    (ast::Type *, u32))

/// Define the ErrorMessageId enumeration
enum class ErrorMessageId : u16 {
#define F(id, str, arg_types) id,
  MESSAGE_LIST(F)
#undef F
};

/// A templated struct that captures the ID of an error message and the C++
/// argument types that must be supplied when the error is reported. The
/// template arguments allow us to ensure not only that all arguments to the
/// given error messages are provided, but also that they have the expected
/// types
template <typename... ArgTypes>
struct ErrorMessage {
  const ErrorMessageId id;
};

/// A container for all TPL error messages
class ErrorMessages {
  template <typename T>
  struct ReflectErrorMessageWithDetails;

  template <typename... ArgTypes>
  struct ReflectErrorMessageWithDetails<void(ArgTypes...)> {
    using type = ErrorMessage<ArgTypes...>;
  };

 public:
#define MSG(kind, str, arg_types) \
  static constexpr const ReflectErrorMessageWithDetails<void(arg_types)>::type k##kind = {ErrorMessageId::kind};
  MESSAGE_LIST(MSG)
#undef MSG
};

}  // namespace sema
}  // namespace tpl
