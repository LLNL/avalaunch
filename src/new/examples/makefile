all: clean
	gcc -g -O0 -o spawn_util.o -c ../spawn_util.c
	gcc -g -O0 -o spawn_net.o -c ../spawn_net.c
	gcc -g -O0 -o spawn_net_tcp.o -c ../spawn_net_tcp.c
	gcc -g -O0 -o spawn_net_fifo.o -c ../spawn_net_fifo.c
	gcc -g -O0 -o strmap.o -c ../strmap.c
	gcc -g -O0 -o strmap_test strmap_test.c strmap.o spawn_util.o spawn_net_tcp.o spawn_net_fifo.o spawn_net.o -I../.
	gcc -g -O0 -o pmi_test pmi_test.c -I../../../install/include ../../../install/lib/libpmi.a
	gcc -g -O0 -o ring_test ring_test.c -I../../../install/include ../../../install/lib/libpmi.a
	mpicc -g -O0 -o connect_test connect_test.c strmap.o spawn_util.o spawn_net_tcp.o spawn_net_fifo.o spawn_net.o -I../.
	mpicc -g -O0 -o connect_test2 connect_test2.c strmap.o spawn_util.o spawn_net_tcp.o spawn_net_fifo.o spawn_net.o -I../.
	mpicc -g -O0 -o fifo_test fifo_test.c strmap.o spawn_util.o spawn_net_tcp.o spawn_net_fifo.o spawn_net.o -I../.

clean:
	rm -rf *.o strmap_test connect_test connect_test2 fifo_test pmi_test ring_test
