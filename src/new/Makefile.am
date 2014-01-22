ACLOCAL_AMFLAGS = -I m4

SUBDIRS = hostfile .
include_HEADERS = pmi.h ring.h
bin_PROGRAMS = launcher
lib_LTLIBRARIES = libpmi.la

launcher_SOURCES = \
  main.c \
  node.c \
  print_errmsg.c print_errmsg.h \
  event_handler.c event_handler.h \
  pollfds.c pollfds.h \
  session.c session.h \
  spawn_net.c spawn_net.h \
  spawn_net_tcp.c spawn_net_tcp.h \
  spawn_net_fifo.c spawn_net_fifo.h \
  spawn_net_util.c spawn_net_util.h \
  spawn_util.c spawn_util.h \
  spawn_clock.c spawn_clock.h \
  timer_util.c timer_util.h\
  strmap.c strmap.h \
  spawn_net_ib.c spawn_net_ib.h \
  spawn_net_ib_internal.h
launcher_CFLAGS = -pthread -Wall -g
launcher_LDFLAGS = -lpthread -lrt
launcher_LDADD = hostfile/libhostfile.a

libpmi_la_SOURCES = \
  spawn_net.c spawn_net.h \
  spawn_net_tcp.c spawn_net_tcp.h \
  spawn_net_fifo.c spawn_net_fifo.h \
  spawn_net_ib.c spawn_net_ib.h \
  spawn_net_util.c spawn_net_util.h \
  spawn_util.c spawn_util.h \
  strmap.c strmap.h \
  mpir.c \
  ring.c ring.h \
  pmi.c pmi.h
