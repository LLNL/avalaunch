OBJS= \
  lwgrp.o \
  strmap.o \
  spawn_util.o \
  spawn_clock.o \
  spawn_net_util.o \
  spawn_net_tcp.o \
  spawn_net_fifo.o \
  spawn_net_ib.o \
  spawn_net.o

LIBS= \
  -libverbs \
  -lpthread \
  -lrt

all: clean
	gcc -g -O0 -o spawn_util.o -c ../spawn_util.c
	gcc -g -O0 -o spawn_clock.o -c ../spawn_clock.c -I ../.
	gcc -g -O0 -o spawn_net.o -c ../spawn_net.c
	gcc -g -O0 -o spawn_net_tcp.o -c ../spawn_net_tcp.c
	gcc -g -O0 -o spawn_net_fifo.o -c ../spawn_net_fifo.c
	gcc -g -O0 -o spawn_net_ib.o -c ../spawn_net_ib.c -I../.
	gcc -g -O0 -o spawn_net_util.o -c ../spawn_net_util.c -I../.
	gcc -g -O0 -o strmap.o -c ../strmap.c
	gcc -g -O0 -o lwgrp.o -c ../lwgrp.c
	gcc -g -O0 -o strmap_test strmap_test.c $(OBJS) -I../. $(LIBS)
	gcc -g -O0 -o pmi_test pmi_test.c $(OBJS) -I../../../install/include ../../../install/lib/libpmi.a $(LIBS)
	gcc -g -O0 -o ring_test ring_test.c $(OBJS) -I../. -I../../../install/include ../../../install/lib/libpmi.a $(LIBS)
	mpicc -g -O0 -o connect_test connect_test.c $(OBJS) -I../. $(LIBS)
	mpicc -g -O0 -o connect_test2 connect_test2.c $(OBJS) -I../. $(LIBS)
	mpicc -g -O0 -o fifo_test fifo_test.c $(OBJS) -I../. $(LIBS)
	mpicc -g -O0 -o ibud_test ibud_test.c $(OBJS) -I../. $(LIBS)
#	gcc -g -O0 -o binary_size-1g binary_size-1g.c
#	gcc -g -O0 -o binary_size-512m binary_size-512m.c
#	gcc -g -O0 -o binary_size-256m binary_size-256m.c
#	gcc -g -O0 -o binary_size-128m binary_size-128m.c
#	gcc -g -O0 -o binary_size-64m binary_size-64m.c
#	gcc -g -O0 -o binary_size-32m binary_size-32m.c
#	gcc -g -O0 -o binary_size-16m binary_size-16m.c
#	gcc -g -O0 -o binary_size-8m binary_size-8m.c 
#	gcc -g -O0 -o binary_size-4m binary_size-4m.c
#	gcc -g -O0 -o binary_size-2m binary_size-2m.c

clean:
	rm -rf *.o strmap_test connect_test connect_test2 fifo_test ibud_test pmi_test ring_test binary_size-1g binary_size-512m binary_size-256m binary_size-128m binary_size-64m binary_size-32m binary_size-16m ring binary_size-8m binary_size-4m binary_size-2m
