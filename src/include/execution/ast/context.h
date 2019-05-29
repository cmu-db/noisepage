#pragma once

#include <memory>

#include "llvm/ADT/StringRef.h"

#include "execution/ast/builtins.h"
#include "execution/ast/identifier.h"
#include "execution/util/region.h"
#include "type/type_id.h"

namespace tpl {

namespace sema {
class ErrorReporter;
}  // namespace sema

namespace ast {

class AstNodeFactory;
class Type;

/**
 * Ast Context. Stores info about current ast.
 */
class Context {
 public:
  /**
   * Constructor
   * @param region region to use for allocation
   * @param error_reporter reporter for errors
   */
  Context(util::Region *region, sema::ErrorReporter *error_reporter);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(Context);

  /**
   * Destructor
   */
  ~Context();

  /**
   * Return an identifier as a unique string in this context.
   * If an identifier with the same name already exists, that name is reused.
   * @param str name of the identifier
   * @return an identifier with a unique string.
   */
  Identifier GetIdentifier(llvm::StringRef str);

  /**
   * Is the type with name identifier a builtin type?
   * @param identifier name of the type
   * @return whether the type is a builtin type
   */
  Type *LookupBuiltinType(Identifier identifier) const;

  /**
   * Convert the SQL type into the equivalent TPL type
   * @param sql_type SQL type to convert
   * @return equivalent TPL type
   */
  Type *GetTplTypeFromSqlType(const terrier::type::TypeId &sql_type);

  /**
   * Is the function with name identifier a builtin function?
   * @param identifier The name of the function to check
   * @param builtin If non-null, set to the appropriate builtin enumeration
   * @return True if the function name is that of a builtin; false otherwise
   */
  bool IsBuiltinFunction(Identifier identifier, Builtin *builtin = nullptr) const;

  // -------------------------------------------------------
  // Simple accessors
  // -------------------------------------------------------

  struct Implementation;
  /**
   * @return the implementation
   */
  Implementation *impl() const { return impl_.get(); }

  /**
   * @return the ast node factory
   */
  AstNodeFactory *node_factory() const { return node_factory_.get(); }

  /**
   * @return the error reporter
   */
  sema::ErrorReporter *error_reporter() const { return error_reporter_; }

  /**
   * @return the region used for allocation
   */
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
