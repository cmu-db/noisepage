#include <llvm/ADT/SmallString.h>
#include <llvm/Support/raw_ostream.h>

#include <string>

#include "execution/ast/type.h"
#include "execution/ast/type_visitor.h"

namespace terrier::execution::ast {

namespace {

/**
 * Visitor class that walks a type hierarchy tree with the purpose of
 * pretty-printing to an injected output stream.
 */
class TypePrinter : public TypeVisitor<TypePrinter> {
 public:
  explicit TypePrinter(llvm::raw_ostream &out) : out_(out) {}

#define DECLARE_VISIT_TYPE(Type) void Visit##Type(const Type *type);
  TYPE_LIST(DECLARE_VISIT_TYPE)
#undef DECLARE_VISIT_TYPE

  void Print(const Type *type) { Visit(type); }

 private:
  llvm::raw_ostream &Os() { return out_; }

 private:
  llvm::raw_ostream &out_;
};

void execution::ast::TypePrinter::VisitBuiltinType(const BuiltinType *type) { Os() << type->GetTplName(); }

void TypePrinter::VisitFunctionType(const FunctionType *type) {
  Os() << "(";
  bool first = true;
  for (const auto &param : type->GetParams()) {
    if (!first) {
      Os() << ",";
    }
    first = false;
    Visit(param.type_);
  }
  Os() << ")->";
  Visit(type->GetReturnType());
}

void TypePrinter::VisitStringType(const StringType *type) { Os() << "string"; }

void TypePrinter::VisitPointerType(const PointerType *type) {
  Os() << "*";
  Visit(type->GetBase());
}

void TypePrinter::VisitStructType(const StructType *type) {
  Os() << "struct{";
  bool first = true;
  for (const auto &field : type->GetAllFields()) {
    if (!first) {
      Os() << ",";
    }
    first = false;
    Visit(field.type_);
  }
  Os() << "}";
}

void TypePrinter::VisitArrayType(const ArrayType *type) {
  Os() << "[";
  if (type->HasUnknownLength()) {
    Os() << "*";
  } else {
    Os() << type->GetLength();
  }
  Os() << "]";
  Visit(type->GetElementType());
}

void execution::ast::TypePrinter::VisitMapType(const MapType *type) {
  Os() << "map[";
  Visit(type->GetKeyType());
  Os() << "]";
  Visit(type->GetValueType());
}

}  // namespace

// static
std::string Type::ToString(const Type *type) {
  llvm::SmallString<256> buffer;
  llvm::raw_svector_ostream stream(buffer);

  TypePrinter printer(stream);
  printer.Print(type);

  return buffer.str();
}

}  // namespace terrier::execution::ast
