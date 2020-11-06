#include "execution/parsing/parser.h"

#include <memory>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "execution/sema/error_reporter.h"

namespace noisepage::execution::parsing {

static std::unordered_set<Token::Type> k_top_level_decls = {Token::Type::STRUCT, Token::Type::FUN};

Parser::Parser(Scanner *scanner, ast::Context *context)
    : scanner_(scanner),
      context_(context),
      node_factory_(context->GetNodeFactory()),
      error_reporter_(context->GetErrorReporter()) {}

ast::AstNode *Parser::Parse() {
  util::RegionVector<ast::Decl *> decls(Region());

  const SourcePosition &start_pos = scanner_->CurrentPosition();

  while (Peek() != Token::Type::EOS) {
    if (ast::Decl *decl = ParseDecl()) {
      decls.push_back(decl);
    }
  }

  return node_factory_->NewFile(start_pos, std::move(decls));
}

void Parser::Sync(const std::unordered_set<Token::Type> &s) {
  Next();
  while (Peek() != Token::Type::EOS) {
    if (s.count(Peek()) > 0) {
      return;
    }
    Next();
  }
}

ast::Expr *Parser::MakeExpr(ast::AstNode *node) {
  if (node == nullptr) {
    return nullptr;
  }

  if (auto *expr_stmt = node->SafeAs<ast::ExpressionStmt>()) {
    return expr_stmt->Expression();
  }

  const auto err_msg = sema::ErrorMessages::kExpectingExpression;
  error_reporter_->Report(node->Position(), err_msg);
  return nullptr;
}

ast::Decl *Parser::ParseDecl() {
  // At the top-level, we only allow structs and functions
  switch (Peek()) {
    case Token::Type::STRUCT: {
      return ParseStructDecl();
    }
    case Token::Type::FUN: {
      return ParseFunctionDecl();
    }
    default: {
      break;
    }
  }

  // Report error, sync up and try again
  error_reporter_->Report(scanner_->CurrentPosition(), sema::ErrorMessages::kInvalidDeclaration);
  Sync(k_top_level_decls);
  return nullptr;
}

ast::Decl *Parser::ParseFunctionDecl() {
  Expect(Token::Type::FUN);

  const SourcePosition &position = scanner_->CurrentPosition();

  // The function name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = GetSymbol();

  // The function literal
  auto *fun = ParseFunctionLitExpr()->As<ast::FunctionLitExpr>();

  // Create declaration
  ast::FunctionDecl *decl = node_factory_->NewFunctionDecl(position, name, fun);

  // Done
  return decl;
}

ast::Decl *Parser::ParseStructDecl() {
  Expect(Token::Type::STRUCT);

  const SourcePosition &position = scanner_->CurrentPosition();

  // The struct name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = GetSymbol();

  // The type
  auto *struct_type = ParseStructType()->As<ast::StructTypeRepr>();

  // The declaration object
  ast::StructDecl *decl = node_factory_->NewStructDecl(position, name, struct_type);

  // Done
  return decl;
}

ast::Decl *Parser::ParseVariableDecl() {
  // VariableDecl = 'var' Ident ':' Type [ '=' Expr ] ;

  Expect(Token::Type::VAR);

  const SourcePosition &position = scanner_->CurrentPosition();

  // The name
  Expect(Token::Type::IDENTIFIER);
  ast::Identifier name = GetSymbol();

  // The type (if exists)
  ast::Expr *type = nullptr;

  if (Matches(Token::Type::COLON)) {
    type = ParseType();
  }

  // The initializer (if exists)
  ast::Expr *init = nullptr;

  if (Matches(Token::Type::EQUAL)) {
    init = ParseExpr();
  }

  if (type == nullptr && init == nullptr) {
    error_reporter_->Report(scanner_->CurrentPosition(), sema::ErrorMessages::kMissingTypeAndInitialValue, name);
  }

  // Create declaration object
  ast::VariableDecl *decl = node_factory_->NewVariableDecl(position, name, type, init);

  // Done
  return decl;
}

ast::Stmt *Parser::ParseStmt() {
  // Statement = Block | ExpressionStmt | ForStmt | IfStmt | ReturnStmt |
  //             SimpleStmt | VariableDecl ;

  switch (Peek()) {
    case Token::Type::LEFT_BRACE: {
      return ParseBlockStmt();
    }
    case Token::Type::FOR: {
      return ParseForStmt();
    }
    case Token::Type::IF: {
      return ParseIfStmt();
    }
    case Token::Type::RETURN: {
      return ParseReturnStmt();
    }
    case Token::Type::VAR: {
      ast::Decl *var_decl = ParseVariableDecl();
      return node_factory_->NewDeclStmt(var_decl);
    }
    default: {
      return ParseSimpleStmt();
    }
  }
}

ast::Stmt *Parser::ParseSimpleStmt() {
  // SimpleStmt = AssignmentStmt | ExpressionStmt ;
  ast::Expr *left = ParseExpr();

  if (Matches(Token::Type::EQUAL)) {
    const SourcePosition &pos = scanner_->CurrentPosition();
    ast::Expr *right = ParseExpr();
    return node_factory_->NewAssignmentStmt(pos, left, right);
  }

  return node_factory_->NewExpressionStmt(left);
}

ast::Stmt *Parser::ParseBlockStmt() {
  // BlockStmt = '{' { Stmt } '}' ;

  // Eat the left brace
  Expect(Token::Type::LEFT_BRACE);
  const SourcePosition &start_position = scanner_->CurrentPosition();

  // Where we store all the statements in the block
  util::RegionVector<ast::Stmt *> statements(Region());
  statements.reserve(16);

  // Loop while we don't see the right brace
  while (Peek() != Token::Type::RIGHT_BRACE && Peek() != Token::Type::EOS) {
    ast::Stmt *stmt = ParseStmt();
    statements.emplace_back(stmt);
  }

  // Eat the right brace
  Expect(Token::Type::RIGHT_BRACE);
  const SourcePosition &end_position = scanner_->CurrentPosition();

  return node_factory_->NewBlockStmt(start_position, end_position, std::move(statements));
}

class Parser::ForHeader {
 public:
  // Header for infinite loops
  static ForHeader Infinite() { return ForHeader(); }

  // Header for standard for-loops
  static ForHeader Standard(ast::Stmt *init, ast::Expr *cond, ast::Stmt *next) {
    return ForHeader(init, cond, next, nullptr, nullptr);
  }

  // Header for for-in loops
  static ForHeader ForIn(ast::Expr *target, ast::Expr *iter) {
    return ForHeader(nullptr, nullptr, nullptr, target, iter);
  }

  bool IsForIn() const { return target_ != nullptr && iter_ != nullptr; }

  bool IsStandard() const { return !IsForIn(); }

  std::tuple<ast::Stmt *, ast::Expr *, ast::Stmt *> GetForElements() const {
    NOISEPAGE_ASSERT(IsStandard(), "Loop isn't a standard for-loop");
    return {init_, cond_, next_};
  }

  std::tuple<ast::Expr *, ast::Expr *> GetForInElements() const {
    NOISEPAGE_ASSERT(IsForIn(), "Loop isn't a for-in");
    return {target_, iter_};
  }

 private:
  ForHeader(ast::Stmt *init, ast::Expr *cond, ast::Stmt *next, ast::Expr *target, ast::Expr *iter)
      : init_(init), cond_(cond), next_(next), target_(target), iter_(iter) {}

  ForHeader() : ForHeader(nullptr, nullptr, nullptr, nullptr, nullptr) {}

 private:
  ast::Stmt *init_;
  ast::Expr *cond_;
  ast::Stmt *next_;

  ast::Expr *target_;
  ast::Expr *iter_;
};

Parser::ForHeader Parser::ParseForHeader() {
  // ForHeader = [ '(' ForWhile ')' | '(' ForReg ')' | '(' ForIn ')' ] .
  // ForWhile = Expr .
  // ForReg = [ Stmt ] ';' [ Expr ] ';' [ Stmt ] .
  // ForIn = Expr 'in' Expr [ '[' Attributes ']' ] .
  // Attributes = { Ident '=' Expr } .

  // Infinite loop?
  if (Peek() == Token::Type::LEFT_BRACE) {
    return ForHeader::Infinite();
  }

  Expect(Token::Type::LEFT_PAREN);

  ast::Stmt *init = nullptr, *cond = nullptr, *next = nullptr;

  if (Peek() != Token::Type::SEMI) {
    cond = ParseStmt();
  }

  // If we see an 'in', it's a for-in loop
  if (Matches(Token::Type::IN)) {
    NOISEPAGE_ASSERT(cond != nullptr, "Must have parsed can't be null");
    ast::Expr *target = MakeExpr(cond);
    ast::Expr *iter = MakeExpr(ParseStmt());
    Expect(Token::Type::RIGHT_PAREN);
    return ForHeader::ForIn(target, iter);
  }

  // Parse either regular for or for-while
  if (Matches(Token::Type::SEMI)) {
    init = cond;
    cond = nullptr;
    if (Peek() != Token::Type::SEMI) {
      cond = ParseStmt();
    }
    Expect(Token::Type::SEMI);
    if (Peek() != Token::Type::RIGHT_PAREN) {
      next = ParseStmt();
    }
  }

  Expect(Token::Type::RIGHT_PAREN);

  return ForHeader::Standard(init, MakeExpr(cond), next);
}

ast::Stmt *Parser::ParseForStmt() {
  // ForStmt = 'for' ForHeader Block ;
  Expect(Token::Type::FOR);

  const SourcePosition &position = scanner_->CurrentPosition();

  // Parse the header to get the initialization statement, loop condition and
  // next-value statement
  const ForHeader header = ParseForHeader();

  // Now the loop body
  auto *body = ParseBlockStmt()->As<ast::BlockStmt>();

  if (header.IsStandard()) {
    const auto &[init, cond, next] = header.GetForElements();
    return node_factory_->NewForStmt(position, init, cond, next, body);
  }

  const auto &[target, iter] = header.GetForInElements();
  return node_factory_->NewForInStmt(position, target, iter, body);
}

ast::Stmt *Parser::ParseIfStmt() {
  // IfStmt = 'if' '(' Expr ')' Block [ 'else' ( IfStmt | Block ) ] ;

  Expect(Token::Type::IF);

  const SourcePosition &position = scanner_->CurrentPosition();

  // Handle condition
  Expect(Token::Type::LEFT_PAREN);
  ast::Expr *cond = ParseExpr();
  Expect(Token::Type::RIGHT_PAREN);

  // Handle 'then' statement
  auto *then_stmt = ParseBlockStmt()->As<ast::BlockStmt>();

  // Handle 'else' statement, if one exists
  ast::Stmt *else_stmt = nullptr;
  if (Matches(Token::Type::ELSE)) {
    if (Peek() == Token::Type::IF) {
      else_stmt = ParseIfStmt();
    } else {
      else_stmt = ParseBlockStmt();
    }
  }

  return node_factory_->NewIfStmt(position, cond, then_stmt, else_stmt);
}

ast::Stmt *Parser::ParseReturnStmt() {
  Expect(Token::Type::RETURN);

  const SourcePosition &position = scanner_->CurrentPosition();

  ast::Expr *ret = nullptr;
  if (Peek() != Token::Type::RIGHT_BRACE) {
    ret = ParseExpr();
  }

  return node_factory_->NewReturnStmt(position, ret);
}

ast::Expr *Parser::ParseExpr() { return ParseBinaryOpExpr(Token::LowestPrecedence() + 1); }

ast::Expr *Parser::ParseBinaryOpExpr(uint32_t min_prec) {
  NOISEPAGE_ASSERT(min_prec > 0, "The minimum precedence cannot be 0");

  ast::Expr *left = ParseUnaryOpExpr();

  for (uint32_t prec = Token::GetPrecedence(Peek()); prec > min_prec; prec--) {
    // It's possible that we reach a token that has lower precedence than the
    // minimum (e.g., EOS) so we check and early exit
    if (Token::GetPrecedence(Peek()) < min_prec) {
      break;
    }

    // Make sure to consume **all** tokens with the same precedence as the
    // current value before moving on to a lower precedence expression. This is
    // to handle cases like 1+2+3+4.
    while (Token::GetPrecedence(Peek()) == prec) {
      Token::Type op = Next();
      const SourcePosition &position = scanner_->CurrentPosition();
      ast::Expr *right = ParseBinaryOpExpr(prec);

      if (Token::IsCompareOp(op)) {
        left = node_factory_->NewComparisonOpExpr(position, op, left, right);
      } else {
        left = node_factory_->NewBinaryOpExpr(position, op, left, right);
      }
    }
  }

  return left;
}

ast::Expr *Parser::ParseUnaryOpExpr() {
  // UnaryOpExpr = PrimaryExpr | unary_op UnaryOpExpr ;
  // unary_op = '&' | '!' | '~' | '^' | '-' | '*'

  switch (Peek()) {
    case Token::Type::AMPERSAND:
    case Token::Type::BANG:
    case Token::Type::BIT_NOT:
    case Token::Type::BIT_XOR:
    case Token::Type::MINUS:
    case Token::Type::STAR: {
      Token::Type op = Next();
      const SourcePosition &position = scanner_->CurrentPosition();
      ast::Expr *expr = ParseUnaryOpExpr();
      return node_factory_->NewUnaryOpExpr(position, op, expr);
    }
    default: {
      break;
    }
  }

  return ParsePrimaryExpr();
}

ast::Expr *Parser::ParsePrimaryExpr() {
  // PrimaryExpr = Operand | CallExpr | MemberExpr | IndexExpr ;
  // CallExpr = PrimaryExpr '(' (Expr)* ') ;
  // MemberExpr = PrimaryExpr '.' Expr
  // IndexExpr = PrimaryExpr '[' Expr ']'

  ast::Expr *result = ParseOperand();

  do {
    switch (Peek()) {
      case Token::Type::LEFT_PAREN: {
        // Call expression
        Consume(Token::Type::LEFT_PAREN);
        util::RegionVector<ast::Expr *> args(Region());
        while (Peek() != Token::Type::RIGHT_PAREN) {
          ast::Expr *arg = ParseExpr();
          args.push_back(arg);
          if (Peek() == Token::Type::COMMA) {
            Next();
          }
        }
        Expect(Token::Type::RIGHT_PAREN);
        result = node_factory_->NewCallExpr(result, std::move(args));
        break;
      }
      case Token::Type::DOT: {
        // Member expression
        Consume(Token::Type::DOT);
        ast::Expr *member = ParseOperand();
        result = node_factory_->NewMemberExpr(result->Position(), result, member);
        break;
        // @ptrCast(*Row, expr)
      }
      case Token::Type::LEFT_BRACKET: {
        // Index expression (i.e., array or map access)
        Consume(Token::Type::LEFT_BRACKET);
        ast::Expr *index = ParseExpr();
        Expect(Token::Type::RIGHT_BRACKET);
        result = node_factory_->NewIndexExpr(result->Position(), result, index);
        break;
      }
      default: {
        break;
      }
    }
  } while (Token::IsCallOrMemberOrIndex(Peek()));

  return result;
}

ast::Expr *Parser::ParseOperand() {
  // Operand = Literal | OperandName | '(' Expr ')'
  // Literal = int_lit | float_lit | 'nil' | 'true' | 'false' | FunctionLiteral
  // OperandName = identifier

  switch (Peek()) {
    case Token::Type::NIL: {
      Consume(Token::Type::NIL);
      return node_factory_->NewNilLiteral(scanner_->CurrentPosition());
    }
    case Token::Type::FALSE:
    case Token::Type::TRUE: {
      const bool bool_val = (Next() == Token::Type::TRUE);
      return node_factory_->NewBoolLiteral(scanner_->CurrentPosition(), bool_val);
    }
    case Token::Type::BUILTIN_IDENTIFIER: {
      // Builtin call expression
      Next();
      ast::Expr *func_name = node_factory_->NewIdentifierExpr(scanner_->CurrentPosition(), GetSymbol());
      Consume(Token::Type::LEFT_PAREN);
      util::RegionVector<ast::Expr *> args(Region());
      while (Peek() != Token::Type::RIGHT_PAREN) {
        ast::Expr *arg = ParseExpr();
        args.push_back(arg);
        if (Peek() == Token::Type::COMMA) {
          Next();
        }
      }
      Expect(Token::Type::RIGHT_PAREN);
      return node_factory_->NewBuiltinCallExpr(func_name, std::move(args));
    }
    case Token::Type::IDENTIFIER: {
      Next();
      return node_factory_->NewIdentifierExpr(scanner_->CurrentPosition(), GetSymbol());
    }
    case Token::Type::INTEGER: {
      Next();
      // Convert the number
      char *end = nullptr;
      int64_t num = std::strtoll(GetSymbol().GetData(), &end, 10);
      return node_factory_->NewIntLiteral(scanner_->CurrentPosition(), num);
    }
    case Token::Type::FLOAT: {
      Next();
      // Convert the number
      char *end = nullptr;
      double num = std::strtod(GetSymbol().GetData(), &end);
      return node_factory_->NewFloatLiteral(scanner_->CurrentPosition(), num);
    }
    case Token::Type::STRING: {
      Next();
      return node_factory_->NewStringLiteral(scanner_->CurrentPosition(), GetSymbol());
    }
    case Token::Type::FUN: {
      Next();
      return ParseFunctionLitExpr();
    }
    case Token::Type::LEFT_PAREN: {
      Consume(Token::Type::LEFT_PAREN);
      ast::Expr *expr = ParseExpr();
      Expect(Token::Type::RIGHT_PAREN);
      return expr;
    }
    default: {
      break;
    }
  }

  // Error
  error_reporter_->Report(scanner_->CurrentPosition(), sema::ErrorMessages::kExpectingExpression);
  Next();
  return node_factory_->NewBadExpr(scanner_->CurrentPosition());
}

ast::Expr *Parser::ParseFunctionLitExpr() {
  // FunctionLiteral = Signature FunctionBody ;
  //
  // FunctionBody = Block ;
  // Parse the type
  auto *func_type = ParseFunctionType()->As<ast::FunctionTypeRepr>();
  // Parse the body
  auto *body = ParseBlockStmt()->As<ast::BlockStmt>();
  // Done
  return node_factory_->NewFunctionLitExpr(func_type, body);
}

ast::Expr *Parser::ParseType() {
  switch (Peek()) {
    case Token::Type::NIL:
    case Token::Type::IDENTIFIER: {
      Next();
      const SourcePosition &position = scanner_->CurrentPosition();
      return node_factory_->NewIdentifierExpr(position, GetSymbol());
    }
    case Token::Type::MAP: {
      return ParseMapType();
    }
    case Token::Type::LEFT_PAREN: {
      return ParseFunctionType();
    }
    case Token::Type::STAR: {
      return ParsePointerType();
    }
    case Token::Type::LEFT_BRACKET: {
      return ParseArrayType();
    }
    case Token::Type::STRUCT: {
      return ParseStructType();
    }
    default: {
      break;
    }
  }

  // Error
  error_reporter_->Report(scanner_->CurrentPosition(), sema::ErrorMessages::kExpectingType);

  return nullptr;
}

ast::Expr *Parser::ParseFunctionType() {
  // FuncType = '(' { ParameterList } ')' '->' Type ;
  // ParameterList = { Ident ':' } Type ;

  const SourcePosition &position = scanner_->CurrentPosition();

  Consume(Token::Type::LEFT_PAREN);

  util::RegionVector<ast::FieldDecl *> params(Region());
  params.reserve(4);

  while (Peek() != Token::Type::RIGHT_PAREN) {
    const SourcePosition &field_position = scanner_->CurrentPosition();

    ast::Identifier ident{};

    ast::Expr *type = nullptr;

    if (Matches(Token::Type::IDENTIFIER)) {
      ident = GetSymbol();
    }

    if (Matches(Token::Type::COLON) || ident.GetData() == nullptr) {
      type = ParseType();
    } else {
      type = node_factory_->NewIdentifierExpr(field_position, ident);
      ident = ast::Identifier();
    }

    // That's it
    params.push_back(node_factory_->NewFieldDecl(field_position, ident, type));

    if (!Matches(Token::Type::COMMA)) {
      break;
    }
  }

  Expect(Token::Type::RIGHT_PAREN);
  Expect(Token::Type::ARROW);

  ast::Expr *ret = ParseType();

  return node_factory_->NewFunctionType(position, std::move(params), ret);
}

ast::Expr *Parser::ParsePointerType() {
  // PointerTypeRepr = '*' Type ;

  const SourcePosition &position = scanner_->CurrentPosition();

  Expect(Token::Type::STAR);

  ast::Expr *base = ParseType();

  return node_factory_->NewPointerType(position, base);
}

ast::Expr *Parser::ParseArrayType() {
  // ArrayTypeRepr = '[' Length ']' Type ;
  // Length = [ '*' | Expr ] ;

  const SourcePosition &position = scanner_->CurrentPosition();

  Consume(Token::Type::LEFT_BRACKET);

  // If the next token doesn't match a right bracket, it means we have a length
  ast::Expr *len = nullptr;
  if (!Matches(Token::Type::RIGHT_BRACKET)) {
    if (!Matches(Token::Type::STAR)) {
      len = ParseExpr();
    }
    Expect(Token::Type::RIGHT_BRACKET);
  } else {
    error_reporter_->Report(position, sema::ErrorMessages::kMissingArrayLength);
  }

  // Now the type
  ast::Expr *elem_type = ParseType();

  // Done
  return node_factory_->NewArrayType(position, len, elem_type);
}

ast::Expr *Parser::ParseStructType() {
  // StructType = '{' { Ident ':' Type } '}' ;

  const SourcePosition &position = scanner_->CurrentPosition();

  Consume(Token::Type::LEFT_BRACE);

  util::RegionVector<ast::FieldDecl *> fields(Region());

  while (Peek() != Token::Type::RIGHT_BRACE) {
    Expect(Token::Type::IDENTIFIER);

    const SourcePosition &field_position = scanner_->CurrentPosition();

    // The parameter name
    ast::Identifier name = GetSymbol();

    // Prepare for parameter type by eating the colon (ew ...)
    Expect(Token::Type::COLON);

    // Parse the type
    ast::Expr *type = ParseType();

    // That's it
    fields.push_back(node_factory_->NewFieldDecl(field_position, name, type));
  }

  Consume(Token::Type::RIGHT_BRACE);

  return node_factory_->NewStructType(position, std::move(fields));
}

ast::Expr *Parser::ParseMapType() {
  // MapType = 'map' '[' Expr ']' Expr ;

  const SourcePosition &position = scanner_->CurrentPosition();

  Consume(Token::Type::MAP);

  Expect(Token::Type::LEFT_BRACKET);

  ast::Expr *key_type = ParseType();

  Expect(Token::Type::RIGHT_BRACKET);

  ast::Expr *value_type = ParseType();

  return node_factory_->NewMapType(position, key_type, value_type);
}

}  // namespace noisepage::execution::parsing
