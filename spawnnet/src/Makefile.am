ACLOCAL_AMFLAGS = -I m4

SUBDIRS = .
noinst_HEADERS = spawn_clock.h spawn_internal.h spawn_net_fifo.h spawn_net_ib.h spawn_net_ib_internal.h spawn_net_tcp.h
include_HEADERS = spawn.h spawn_util.h strmap.h spawn_net.h spawn_net_util.h lwgrp.h
lib_LTLIBRARIES = libspawn.la

libspawn_la_SOURCES = \
  spawn.h \
  spawn_util.c spawn_util.h \
  strmap.c strmap.h \
  spawn_net.c spawn_net.h \
  spawn_net_tcp.c spawn_net_tcp.h \
  spawn_net_fifo.c spawn_net_fifo.h \
  spawn_net_ib.c spawn_net_ib.h \
  spawn_net_util.c spawn_net_util.h \
  spawn_clock.c spawn_clock.h \
  lwgrp.c lwgrp.h
libspawn_la_CFLAGS  = -DHAVE_SPAWN_NET_IBUD
libspawn_la_LDFLAGS = -lpthread -lrt
