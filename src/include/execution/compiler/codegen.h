#pragma once

#include <string>
#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "type/transient_value.h"
#include "type/type_id.h"

namespace tpl::util {
class Region;
}

namespace tpl::compiler {

class CodeContext;
class FunctionBuilder;

/**
 * Bundles convenience methods needed by other classes during code generation.
 */
class CodeGen {
 private:
  friend class FunctionBuilder;
  friend class QueryState;

 public:
  /**
   * @return region used for allocation
   */
  util::Region *GetRegion();

  /**
   * @return code context used for code generation
   */
  CodeContext *GetCodeContext() const { return ctx_; }

  /**
   * Constructor
   * @param ctx code context to use
   */
  explicit CodeGen(CodeContext *ctx);

  /// Prevent copy and mve
  DISALLOW_COPY_AND_MOVE(CodeGen);

  /**
   * TODO(Amadou): why node just define GetFactory()?
   * @return the ast node factory
   */
  ast::AstNodeFactory *operator->() { return factory_; }

  /**
   * @return the current function begin generated
   */
  FunctionBuilder *GetCurrentFunction();


  /*
   * Convenience methods.
   */

  /**
   * @param transient_val the transient value to read
   * @return the value stored in transient_val
   */
  ast::Expr *PeekValue(const terrier::type::TransientValue &transient_val) const;

  /// Convert from terrier type to TPL expr type
  ast::Expr *TyConvert(terrier::type::TypeId type) const;
  /// Get a nil type
  ast::Expr *TyNil() const;
  /// Get a bool type
  ast::Expr *TyBool() const;
  /// Get an integer type
  ast::Expr *TyInteger() const;
  /// Get an i8 type
  ast::Expr *TyInt8() const;
  /// Get an i16 type
  ast::Expr *TyInt16() const;
  /// Get an i32 type
  ast::Expr *TyInt32() const;
  /// Get an i64 type
  ast::Expr *TyInt64() const;
  /// Get an i128 type
  ast::Expr *TyInt128() const;
  /// Get a u8 type
  ast::Expr *TyUInt8() const;
  /// Get a u16 type
  ast::Expr *TyUInt16() const;
  /// Get a u32 type
  ast::Expr *TyUInt32() const;
  /// Get a u64 type
  ast::Expr *TyUInt64() const;
  /// Get a u128 type
  ast::Expr *TyUInt128() const;
  /// Get an f32 type
  ast::Expr *TyFloat32() const;
  /// Get a f64 type
  ast::Expr *TyFloat64() const;

  /**
   * @return an empty BlockStmt
   */
  ast::BlockStmt *EmptyBlock() const;

  /**
   * @return a new identifier
   */
  ast::Identifier NewIdentifier();

  /**
   * @param fn function to call
   * @param args arguments
   * @return a new call stmt
   */
  ast::Stmt *Call(ast::FunctionDecl *fn, util::RegionVector<ast::Expr *> &&args);

  // TODO(WAN): how to handle builtins?
  /**
   * @return Builtin ptrcast identifier
   */
  ast::IdentifierExpr *BptrCast();

  /**
   * @return Builtin outputAlloc identifier
   */
  ast::IdentifierExpr *BoutputAlloc();

  /**
   * @return Builtin outputAdvance identifier
   */
  ast::IdentifierExpr *BoutputAdvance();

  /**
   * @return Builtin outputFinalize identifier
   */
  ast::IdentifierExpr *BoutputFinalize();

  /**
   * @return Builting insert identifier
   */
  ast::IdentifierExpr *Binsert();

 private:
  const std::string ptrCast = "ptrCast";
  const std::string outputAlloc = "outputAlloc";
  const std::string outputAdvance = "outputAdvance";
  const std::string outputFinalize = "outputFinalize";
  const std::string insert = "insert";

  u64 id_count_;
  CodeContext *ctx_;
  ast::AstNodeFactory *factory_;
};

}  // namespace tpl::compiler
