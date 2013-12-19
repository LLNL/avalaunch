OBJS= \
  strmap.o \
  spawn_util.o \
  spawn_net_tcp.o \
  spawn_net_fifo.o \
  spawn_net_ib.o \
  spawn_net.o \
  ib_internal.o \
  mv2_spawn_net_debug_utils.o \
  mv2_spawn_net_ibv_ud.o \
  mv2_spawn_net_clock.o \
  mv2_spawn_net_vbuf.o

LIBS= \
  -libverbs \
  -lpthread \
  -lrt

all: clean
	gcc -g -O0 -o spawn_util.o -c ../spawn_util.c
	gcc -g -O0 -o spawn_net.o -c ../spawn_net.c
	gcc -g -O0 -o spawn_net_tcp.o -c ../spawn_net_tcp.c
	gcc -g -O0 -o spawn_net_fifo.o -c ../spawn_net_fifo.c
	gcc -g -O0 -o spawn_net_ib.o -c ../spawn_net_ib.c -I../.
	gcc -g -O0 -o ib_internal.o -c ../ib_internal.c -I ../.
	gcc -g -O0 -o mv2_spawn_net_debug_utils.o -c ../mv2_spawn_net_debug_utils.c -I ../.
	gcc -g -O0 -o mv2_spawn_net_ibv_ud.o -c ../mv2_spawn_net_ibv_ud.c -I ../.
	gcc -g -O0 -o mv2_spawn_net_clock.o -c ../mv2_spawn_net_clock.c -I ../.
	gcc -g -O0 -o mv2_spawn_net_vbuf.o -c ../mv2_spawn_net_vbuf.c -I ../.
	gcc -g -O0 -o strmap.o -c ../strmap.c
	gcc -g -O0 -o strmap_test strmap_test.c $(OBJS) -I../. $(LIBS)
	gcc -g -O0 -o pmi_test pmi_test.c $(OBJS) -I../../../install/include ../../../install/lib/libpmi.a $(LIBS)
	gcc -g -O0 -o ring_test ring_test.c $(OBJS) -I../. -I../../../install/include ../../../install/lib/libpmi.a $(LIBS)
	mpicc -g -O0 -o connect_test connect_test.c $(OBJS) -I../. $(LIBS)
	mpicc -g -O0 -o connect_test2 connect_test2.c $(OBJS) -I../. $(LIBS)
	mpicc -g -O0 -o fifo_test fifo_test.c $(OBJS) -I../. $(LIBS)
	mpicc -g -O0 -o ibud_test ibud_test.c $(OBJS) -I../. $(LIBS)

clean:
	rm -rf *.o strmap_test connect_test connect_test2 fifo_test ibud_test pmi_test ring_test
