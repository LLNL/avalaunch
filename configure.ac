#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.68])
AC_INIT([launcher], [0.1], [perkinjo@cse.ohio-state.edu])
AC_CONFIG_SRCDIR([config.h.in])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE([-Wall -Werror foreign])

# Checks for programs.
AC_PROG_CXX
AC_PROG_AWK
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_YACC

AM_PROG_CC_C_O
AM_PROG_AR
AM_SILENT_RULES
AM_PROG_LEX

LT_INIT([dlopen])

# Checks for libraries.
AC_SEARCH_LIBS([ibv_create_qp], [ibverbs], [],
               [AC_MSG_ERROR([missing required library])])

# Checks for header files.
AC_CHECK_HEADERS([arpa/inet.h limits.h netdb.h stdlib.h string.h sys/socket.h unistd.h])
AC_CHECK_HEADER([infiniband/verbs.h], [],
                [AC_MSG_ERROR([cannot find required header file])])
AC_CHECK_HEADER([infiniband/umad.h], [],
                [AC_MSG_ERROR([cannot find required header file])])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_PID_T

# Checks for library functions.
AC_FUNC_FORK
AC_CHECK_FUNCS([dup2 memset strdup strerror])

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 src/new/Makefile
                 src/new/hostfile/Makefile])
AC_OUTPUT
