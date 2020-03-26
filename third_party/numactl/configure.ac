AC_PREREQ([2.64])
AC_INIT([numactl], [2.0.13])

AC_CONFIG_SRCDIR([numactl.c])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_HEADERS([config.h])

AM_INIT_AUTOMAKE([foreign 1.11 silent-rules subdir-objects parallel-tests])
AM_SILENT_RULES([yes])

LT_PREREQ([2.2])
LT_INIT

AC_PROG_CC

# Override CFLAGS so that we can specify custom CFLAGS for numademo.
AX_AM_OVERRIDE_VAR([CFLAGS])

AX_TLS

AX_CHECK_COMPILE_FLAG([-ftree-vectorize], [tree_vectorize="true"])
AM_CONDITIONAL([HAVE_TREE_VECTORIZE], [test x"${tree_vectorize}" = x"true"])

AC_CONFIG_FILES([Makefile])

AC_OUTPUT
