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

class CodeGen {
 private:
  friend class FunctionBuilder;
  friend class QueryState;

 public:
  util::Region *GetRegion();

  CodeContext *GetCodeContext() const { return ctx_; }
  explicit CodeGen(CodeContext *ctx);

  DISALLOW_COPY_AND_MOVE(CodeGen);

  ast::AstNodeFactory *operator->() { return factory_; }

  FunctionBuilder *GetCurrentFunction();

  ast::Expr *PeekValue(const terrier::type::TransientValue &transient_val) const;

  ast::Expr *TyConvert(terrier::type::TypeId type) const;
  ast::Expr *TyNil() const;
  ast::Expr *TyBool() const;
  ast::Expr *TyInteger() const;
  ast::Expr *TyInt8() const;
  ast::Expr *TyInt16() const;
  ast::Expr *TyInt32() const;
  ast::Expr *TyInt64() const;
  ast::Expr *TyInt128() const;
  ast::Expr *TyUInt8() const;
  ast::Expr *TyUInt16() const;
  ast::Expr *TyUInt32() const;
  ast::Expr *TyUInt64() const;
  ast::Expr *TyUInt128() const;
  ast::Expr *TyFloat32() const;
  ast::Expr *TyFloat64() const;

  ast::BlockStmt *EmptyBlock() const;

  ast::Identifier NewIdentifier();

  ast::Stmt *Call(ast::FunctionDecl *fn, util::RegionVector<ast::Expr *> &&args);

  // TODO(WAN): how to handle builtins?
  ast::IdentifierExpr *BptrCast();
  ast::IdentifierExpr *BoutputAlloc();
  ast::IdentifierExpr *BoutputAdvance();
  ast::IdentifierExpr *BoutputFinalize();
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
