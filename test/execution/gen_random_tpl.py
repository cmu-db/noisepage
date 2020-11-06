# http://pastie.org/pastes/10943132/text
# Copyright (c) 2016 1wd
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import random
import sys

class Expr(object):
    def __init__(self):
        pass

class ConstExpr(Expr):
    def __init__(self, val):
        self.val = val

class VarExpr(Expr):
    def __init__(self, name):
        self.name = name

class BinOp(Expr):
    def __init__(self, op, left, right):
        self.op = op
        self.left = left
        self.right = right

class FunCallExpr(Expr):
    def __init__(self, name):
        self.name = name

class Statement(object):
    def __init__(self):
        pass

class Assignment(Statement):
    def __init__(self, lval, expr):
        self.lval = lval
        self.expr = expr

class VarDecl(Statement):
    def __init__(self, name, expr):
        self.name = name
        self.expr = expr
        self.mut = False
        self.used = False

class Return(Statement):
    def __init__(self, expr, context):
        self.expr = expr
        self.context = context

class Print(Statement):
    def __init__(self, expr):
        self.expr = expr

class FunDecl(object):
    def __init__(self, name, statements, return_type):
        self.name = name
        self.statements = statements
        self.return_type = return_type
        self.used = False

class Program(object):
    def __init__(self, main, functions):
        self.main = main
        self.functions = functions

#----------------------------------------------------------

class Context(object):
    def __init__(self, parent=None, decl_name=None):
        self.env = {}
        self.id = 0
        self.parent = parent
        self.decl_name = decl_name

    def name(self, base, i):
        return "%s%s" % (base, i)

    def new_name(self, base):
        self.id += 1
        return self.name(base, self.id)

    def random_name(self, base):
        biased_min = random.randint(1, self.id)
        i = random.randint(biased_min, self.id)
        return self.name(base, i)

    def random_expr(self):
        return random.choice([
            self.random_const_expr,
            self.random_var_expr,
            self.random_binary_op,
            self.random_fun_call,
        ])()

    def find_unused(self):
        for decl in self.env.values():
            if not decl.used:
                return decl
        return None

    def force_use_expr(self):
        expr = self.random_const_expr()
        decl = self.find_unused()
        while decl is not None:
            left = self.forced_var_expr(decl.name)
            expr = self.forced_random_binary_op(left, expr)
            decl = self.find_unused()

        decl = self.parent.find_unused()
        while decl is not None:
            left = self.forced_fun_call(decl.name)
            expr = self.forced_random_binary_op(left, expr)
            decl = self.parent.find_unused()
        return expr

    def random_const_expr(self):
        return ConstExpr(str(random.randint(1, 1000)))

    def forced_var_expr(self, name):
        decl = self.env[name]
        decl.used = True
        return VarExpr(name)

    def random_var_expr(self):
        if self.id == 0:
            return self.random_const_expr()
        name = self.random_name('x')
        return self.forced_var_expr(name)

    def forced_random_binary_op(self, left, right):
        #op = random.choice(["+", "-", "*", "|", "&", "^"])
        op = random.choice(["|", "&", "^"])
        return BinOp(op, left, right)

    def random_binary_op(self):
        left = self.random_expr()
        right = self.random_expr()
        return self.forced_random_binary_op(left, right)

    def forced_fun_call(self, name):
        decl = self.parent.env[name]
        decl.used = True
        return FunCallExpr(name)

    def random_fun_call(self):
        if self.parent.id == 0:
            return self.random_const_expr()
        name = self.parent.random_name('f')
        return self.forced_fun_call(name)

    def random_statement(self):
        return random.choice([
            self.random_assignment,
            self.random_var_decl,
        ])()

    def random_assignment(self):
        name = self.random_name('x')
        decl = self.env[name]
        expr = self.random_expr()
        if not decl.used:
            left = self.forced_var_expr(name)
            expr = self.forced_random_binary_op(left, expr)
        decl.used = False
        decl.mut = True
        return Assignment(name, expr)

    def random_return_statement(self):
        return Return(self.force_use_expr(), self)

    def random_print_statement(self):
        return Print(self.force_use_expr())

    def random_var_decl(self):
        expr = self.random_expr()
        name = self.new_name('x')
        decl = VarDecl(name, expr)
        self.env[name] = decl
        return decl

    def random_fun_decl(self, num_statements, return_type):
        local = Context(self)
        statements = []
        statements.append(local.random_var_decl())
        for i in range(num_statements):
            statements.append(local.random_statement())
        if return_type is not None:
            statements.append(local.random_return_statement())
        else:
            statements.append(local.random_print_statement())
        name = self.new_name('f')
        decl = FunDecl(name, statements, return_type)
        local.decl = decl
        self.env[name] = decl
        return decl

    def random_program(self, num_funs, max_statements_per_fun):
        functions = []
        for i in range(num_funs):
            num_statements = random.randint(1, max_statements_per_fun)
            fun_decl = self.random_fun_decl(num_statements, 'int')
            functions.append(fun_decl)
        num_statements = random.randint(1, max_statements_per_fun)
        main = self.random_fun_decl(num_statements, None)
        return Program(main, functions)

#----------------------------------------------------------

class Lang(object):

    operators = {
        '&': '&',
        '|': '|',
        '^': '^',
    }

    def __init__(self):
        self.indent = 0

    def write_indent(self, f):
        f.write(' ' * 4 * self.indent)

    def write_statement(self, f, statement):
        handlers = {
            VarDecl: self.write_var_decl,
            Assignment: self.write_assignment,
            Return: self.write_return,
            Print: self.write_print,
        }
        handler = handlers.get(type(statement))
        if handler is not None:
            handler(f, statement)
        else:
            raise Exception("Unknown kind of statement")

    def write_lval(self, f, lval):
        f.write(lval)

    def write_expr(self, f, expr, needs_parens=False):
        handlers = {
            ConstExpr: self.write_const_expr,
            VarExpr: self.write_var_expr,
            BinOp: self.write_bin_op,
            FunCallExpr: self.write_fun_call,
        }
        handler = handlers.get(type(expr))
        if handler is not None:
            handler(f, expr, needs_parens)
        else:
            raise Exception("Unknown kind of expr")

    def write_const_expr(self, f, expr, needs_parens):
        f.write(expr.val)

    def write_var_expr(self, f, expr, needs_parens):
        f.write(expr.name)

    def write_bin_op(self, f, expr, needs_parens):
        if needs_parens:
            f.write("(")
        self.write_expr(f, expr.left, needs_parens=True)
        f.write(" %s " % self.operators[expr.op])
        self.write_expr(f, expr.right, needs_parens=True)
        if needs_parens:
            f.write(")")

    def write_fun_call(self, f, expr, needs_parens):
        f.write("%s()" % expr.name)

class CppLang(Lang):

    ext = 'cpp'

    type_names = {
        'int': 'int',
    }

    def write_program(self, f, program):
        f.write('#include <cstdio>\n\n')
        for fun_decl in program.functions:
            self.write_fun_decl(f, fun_decl)
            f.write('\n')
        self.write_fun_decl(f, program.main, main=True)

    def write_fun_decl(self, f, fun_decl, main=False):
        if fun_decl.return_type is None:
            optional_result = 'int '
        else:
            type_name = self.type_names[fun_decl.return_type]
            optional_result = '%s ' % type_name
        fun_name = 'main' if main else fun_decl.name
        f.write('%s %s() {\n' % (optional_result, fun_name))
        self.indent += 1
        for statement in fun_decl.statements:
            self.write_statement(f, statement)
        self.indent -= 1
        f.write('}\n')

    def write_var_decl(self, f, var_decl):
        self.write_indent(f)
        f.write('int ')
        self.write_lval(f, var_decl.name)
        f.write(' = ')
        self.write_expr(f, var_decl.expr)
        f.write(';\n')

    def write_assignment(self, f, assignment):
        self.write_indent(f)
        self.write_lval(f, assignment.lval)
        f.write(' = ')
        self.write_expr(f, assignment.expr)
        f.write(';\n')

    def write_return(self, f, statement):
        self.write_indent(f)
        f.write('return ')
        self.write_expr(f, statement.expr)
        f.write(';\n')

    def write_print(self, f, statement):
        self.write_indent(f)
        f.write('printf("%i\\n", ')
        self.write_expr(f, statement.expr)
        f.write(');\n')

class CLang(CppLang):

    ext = 'c'

    def write_program(self, f, program):
        f.write('#include <stdio.h>\n\n')
        for fun_decl in program.functions:
            self.write_fun_decl(f, fun_decl)
            f.write('\n')
        self.write_fun_decl(f, program.main, main=True)

class DLang(Lang):

    ext = 'd'

    type_names = {
        'int': 'int',
    }

    def write_program(self, f, program):
        f.write('import std.stdio;\n\n')
        for fun_decl in program.functions:
            self.write_fun_decl(f, fun_decl)
            f.write('\n')
        self.write_fun_decl(f, program.main, main=True)

    def write_fun_decl(self, f, fun_decl, main=False):
        if fun_decl.return_type is None:
            optional_result = 'void '
        else:
            type_name = self.type_names[fun_decl.return_type]
            optional_result = '%s ' % type_name
        fun_name = 'main' if main else fun_decl.name
        f.write('%s %s() {\n' % (optional_result, fun_name))
        self.indent += 1
        for statement in fun_decl.statements:
            self.write_statement(f, statement)
        self.indent -= 1
        f.write('}\n')

    def write_var_decl(self, f, var_decl):
        self.write_indent(f)
        f.write('int ')
        self.write_lval(f, var_decl.name)
        f.write(' = ')
        self.write_expr(f, var_decl.expr)
        f.write(';\n')

    def write_assignment(self, f, assignment):
        self.write_indent(f)
        self.write_lval(f, assignment.lval)
        f.write(' = ')
        self.write_expr(f, assignment.expr)
        f.write(';\n')

    def write_return(self, f, statement):
        self.write_indent(f)
        f.write('return ')
        self.write_expr(f, statement.expr)
        f.write(';\n')

    def write_print(self, f, statement):
        self.write_indent(f)
        f.write('writefln("%d", ')
        self.write_expr(f, statement.expr)
        f.write(');\n')

class GoLang(Lang):

    ext = 'go'

    type_names = {
        'int': 'int',
    }

    def write_program(self, f, program):
        f.write('package main\n\n')
        f.write('import "fmt"\n\n')
        for fun_decl in program.functions:
            self.write_fun_decl(f, fun_decl)
            f.write('\n')
        self.write_fun_decl(f, program.main, main=True)

    def write_fun_decl(self, f, fun_decl, main=False):
        if fun_decl.return_type is None:
            optional_result = ''
        else:
            type_name = self.type_names[fun_decl.return_type]
            optional_result = ' %s' % type_name
        fun_name = 'main' if main else fun_decl.name
        f.write('func %s()%s {\n' % (fun_name, optional_result))
        self.indent += 1
        for statement in fun_decl.statements:
            self.write_statement(f, statement)
        self.indent -= 1
        f.write('}\n')

    def write_var_decl(self, f, var_decl):
        self.write_indent(f)
        self.write_lval(f, var_decl.name)
        f.write(' := ')
        self.write_expr(f, var_decl.expr)
        f.write('\n')

    def write_assignment(self, f, assignment):
        self.write_indent(f)
        self.write_lval(f, assignment.lval)
        f.write(' = ')
        self.write_expr(f, assignment.expr)
        f.write('\n')

    def write_return(self, f, statement):
        self.write_indent(f)
        f.write('return ')
        self.write_expr(f, statement.expr)
        f.write('\n')

    def write_print(self, f, statement):
        self.write_indent(f)
        f.write('fmt.Printf("%d\\n", ')
        self.write_expr(f, statement.expr)
        f.write(')\n')

class PascalLang(Lang):

    ext = 'pas'

    type_names = {
        'int': 'integer',
    }

    operators = {
        '&': 'and',
        '|': 'or',
        '^': 'xor',
    }

    def write_program(self, f, program):
        f.write('program main;\n\n')
        for fun_decl in program.functions:
            self.write_fun_decl(f, fun_decl)
            f.write('\n')
        self.write_fun_decl(f, program.main, main=True)

    def write_fun_decl(self, f, fun_decl, main=False):
        if not main:
            fun_name = fun_decl.name
            type_name = self.type_names[fun_decl.return_type]
            f.write('function %s() : %s;\n' % (fun_name, type_name))
        vars = [s for s in fun_decl.statements if isinstance(s, VarDecl)]
        if vars:
            f.write('var\n')
            for v in vars:
                type_name = self.type_names['int']
                f.write('  %s : %s;\n' % (v.name, type_name))
        f.write('begin\n')
        self.indent += 1
        for statement in fun_decl.statements:
            self.write_statement(f, statement)
        self.indent -= 1
        f.write('end%s\n' % ('.' if main else ';'))

    def write_var_decl(self, f, var_decl):
        self.write_indent(f)
        self.write_lval(f, var_decl.name)
        f.write(' := ')
        self.write_expr(f, var_decl.expr)
        f.write(';\n')

    def write_assignment(self, f, assignment):
        self.write_indent(f)
        self.write_lval(f, assignment.lval)
        f.write(' := ')
        self.write_expr(f, assignment.expr)
        f.write(';\n')

    def write_return(self, f, statement):
        self.write_indent(f)
        self.write_lval(f, statement.context.decl.name)
        f.write(' := ')
        self.write_expr(f, statement.expr)
        f.write(';\n')

    def write_print(self, f, statement):
        self.write_indent(f)
        f.write('writeln(')
        self.write_expr(f, statement.expr)
        f.write(');\n')

class RustLang(Lang):

    ext = 'rs'

    type_names = {
        'int': 'i32',
    }

    def write_program(self, f, program):
        for fun_decl in program.functions:
            self.write_fun_decl(f, fun_decl)
            f.write('\n')
        self.write_fun_decl(f, program.main, main=True)

    def write_fun_decl(self, f, fun_decl, main=False):
        if fun_decl.return_type is None:
            optional_result = ''
        else:
            type_name = self.type_names[fun_decl.return_type]
            optional_result = ' -> %s' % type_name
        fun_name = 'main' if main else fun_decl.name
        f.write('fn %s()%s {\n' % (fun_name, optional_result))
        self.indent += 1
        for statement in fun_decl.statements:
            self.write_statement(f, statement)
        self.indent -= 1
        f.write('}\n')

    def write_var_decl(self, f, var_decl):
        self.write_indent(f)
        f.write('let ')
        if var_decl.mut:
            f.write('mut ')
        self.write_lval(f, var_decl.name)
        f.write(': i32')
        f.write(' = ')
        self.write_expr(f, var_decl.expr)
        f.write(';\n')

    def write_const_expr(self, f, expr, needs_parens):
        f.write(expr.val + 'i32')

    def write_assignment(self, f, assignment):
        self.write_indent(f)
        self.write_lval(f, assignment.lval)
        f.write(' = ')
        self.write_expr(f, assignment.expr)
        f.write(';\n')

    def write_return(self, f, statement):
        self.write_indent(f)
        self.write_expr(f, statement.expr)
        f.write('\n')

    def write_print(self, f, statement):
        self.write_indent(f)
        f.write('println!("{}", ')
        self.write_expr(f, statement.expr)
        f.write(')\n')

class TplLang(Lang):

    ext = 'tpl'

    type_names = {
        'int': 'int32',
    }

    def write_program(self, f, program):
        for fun_decl in program.functions:
            self.write_fun_decl(f, fun_decl)
            f.write('\n')
        self.write_fun_decl(f, program.main, main=True)

    def write_fun_decl(self, f, fun_decl, main=False):
        if fun_decl.return_type is None:
            optional_result = ' -> int'
        else:
            type_name = self.type_names[fun_decl.return_type]
            optional_result = ' -> %s' % type_name
        fun_name = 'main' if main else fun_decl.name
        f.write('fun %s()%s {\n' % (fun_name, optional_result))
        self.indent += 1
        for statement in fun_decl.statements:
            self.write_statement(f, statement)
        self.indent -= 1
        f.write('}\n')

    def write_var_decl(self, f, var_decl):
        self.write_indent(f)
        f.write('var ')
        self.write_lval(f, var_decl.name)
        f.write(': int32')
        f.write(' = ')
        self.write_expr(f, var_decl.expr)
        f.write('\n')

    def write_assignment(self, f, assignment):
        self.write_indent(f)
        self.write_lval(f, assignment.lval)
        f.write(' = ')
        self.write_expr(f, assignment.expr)
        f.write('\n')

    def write_return(self, f, statement):
        self.write_indent(f)
        f.write('return ')
        self.write_expr(f, statement.expr)
        f.write('\n')

    def write_print(self, f, statement):
        self.write_indent(f)
        f.write('println("{}", ')
        self.write_expr(f, statement.expr)
        f.write(')\n')

#----------------------------------------------------------

seed = sys.argv[1]
num_funs = sys.argv[2]

random.seed(seed)
c = Context()
p = c.random_program(
    num_funs=int(num_funs),
    max_statements_per_fun=20)

langs = [
    #CppLang(),
    #CLang(),
    #DLang(),
    #GoLang(),
    #PascalLang(),
    #RustLang(),
    TplLang(),
]

for lang in langs:
    filename = 'test_%s_s%s_n%s.%s' % (lang.ext, seed, num_funs, lang.ext)
    with open(filename, 'w') as f:
        lang.write_program(f, p)
