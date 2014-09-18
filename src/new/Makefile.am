ACLOCAL_AMFLAGS = -I m4

SUBDIRS = hostfile .
include_HEADERS = pmi.h ring.h spawn_net.h lwgrp.h strmap.h spawn_net_util.h
bin_PROGRAMS = launcher
lib_LTLIBRARIES = libspawn.la libpmi.la

libspawn_la_SOURCES = \
  spawn_net.c spawn_net.h \
  spawn_net_tcp.c spawn_net_tcp.h \
  spawn_net_fifo.c spawn_net_fifo.h \
  spawn_net_ib.c spawn_net_ib.h \
  spawn_net_util.c spawn_net_util.h \
  spawn_util.c spawn_util.h \
  spawn_clock.c spawn_clock.h \
  strmap.c strmap.h \
  lwgrp.c lwgrp.h
libspawn_la_CFLAGS  = -DHAVE_SPAWN_NET_IBUD
libspawn_la_LDFLAGS = -lpthread -lrt

libpmi_la_SOURCES = \
  mpir.c \
  ring.c ring.h \
  pmi.c pmi.h
libpmi_la_LIBADD  = libspawn.la
libpmi_la_LDFLAGS = -lpthread -lrt

launcher_SOURCES = \
  main.c \
  node.c \
  print_errmsg.c print_errmsg.h \
  event_handler.c event_handler.h \
  pollfds.c pollfds.h \
  session.c session.h \
  timer_util.c timer_util.h
launcher_CFLAGS  = -pthread -Wall -g
launcher_LDADD   = hostfile/libhostfile.a libspawn.la
launcher_LDFLAGS = -lpthread -lrt
