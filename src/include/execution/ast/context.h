#pragma once

#include <memory>

#include "llvm/ADT/StringRef.h"

#include "execution/ast/builtins.h"
#include "execution/ast/identifier.h"
#include "type/type_id.h"
#include "execution/util/region.h"

namespace tpl {

namespace sema {
class ErrorReporter;
}  // namespace sema

namespace sql {
class Type;
}  // namespace sql

namespace ast {

class AstNodeFactory;
class Type;

class Context {
 public:
  /// Constructor
  Context(util::Region *region, sema::ErrorReporter *error_reporter);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(Context);

  /// Destructor
  ~Context();

  /// Return \a str as a unique string in this context
  Identifier GetIdentifier(llvm::StringRef str);

  /// Is the type with name \a identifier a builtin type?
  /// \return A non-null pointer to the Type if a valid builtin; null otherwise
  Type *LookupBuiltinType(Identifier identifier) const;

  /// Convert the SQL type into the equivalent TPL type
  Type *GetTplTypeFromSqlType(const terrier::type::TypeId &sql_type);

  /// Is the function with name \a identifier a builtin function?
  /// \param[in] identifier The name of the function to check
  /// \param[out] builtin If non-null, set to the appropriate builtin
  ///                     enumeration \return True if the function name is that
  ///                     of a builtin; false otherwise
  bool IsBuiltinFunction(Identifier identifier,
                         Builtin *builtin = nullptr) const;

  // -------------------------------------------------------
  // Simple accessors
  // -------------------------------------------------------

  struct Implementation;
  Implementation *impl() const { return impl_.get(); }

  AstNodeFactory *node_factory() const { return node_factory_.get(); }

  sema::ErrorReporter *error_reporter() const { return error_reporter_; }

  util::Region *region() const { return region_; }

 private:
  // Region allocator for all Ast objects this context needs
  util::Region *region_;

  // Error reporter
  sema::ErrorReporter *error_reporter_;

  // The factory used for Ast nodes
  std::unique_ptr<AstNodeFactory> node_factory_;

  // Pimpl
  std::unique_ptr<Implementation> impl_;
};

}  // namespace ast
}  // namespace tpl
