AC_DEFUN([X_AC_SPAWNNET], [
  AC_MSG_CHECKING([for libspawnnet])
  AC_ARG_WITH([spawnnet], [AC_HELP_STRING([--with-spawnnet=PATH], 
    [path to installed libspawnnet [default=/usr/local]])], [
    LIBSPAWNNET_INCLUDE="${withval}/include"
    LIBSPAWNNET_LIB="${withval}/lib"
    AC_MSG_RESULT("${withval}")
  ], [
    LIBSPAWNNET_INCLUDE="/usr/local/include"
    LIBSPAWNNET_LIB="/usr/local/lib"
    AC_MSG_RESULT(/usr/local)
  ])
  AC_SUBST(LIBSPAWNNET_INCLUDE)
  AC_SUBST(LIBSPAWNNET_LIB)

  CFLAGS="$CFLAGS -I${LIBSPAWNNET_INCLUDE}"
  CXXFLAGS="$CXXFLAGS -I${LIBSPAWNNET_INCLUDE}"
  LDFLAGS="$LDFLAGS -L${LIBSPAWNNET_LIB}"

  AC_SEARCH_LIBS([spawn_net_open], [spawn], [], [
    AC_MSG_ERROR([couldn't find a suitable libspawnnet, use --with-spawnnet=PATH])], [])
])
