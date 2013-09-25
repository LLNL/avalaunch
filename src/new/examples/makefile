all: clean
	gcc -g -O0 -o spawn_util.o -c ../spawn_util.c
	gcc -g -O0 -o spawn_net.o -c ../spawn_net.c
	gcc -g -O0 -o spawn_net_tcp.o -c ../spawn_net_tcp.c
	gcc -g -O0 -o strmap.o -c ../strmap.c
	gcc -g -O0 -o strmap_test strmap_test.c strmap.o spawn_util.o spawn_net_tcp.o spawn_net.o -I../.
	mpicc -g -O0 -o connect_test connect_test.c strmap.o spawn_util.o spawn_net_tcp.o spawn_net.o -I../.

clean:
	rm -rf *.o strmap_test connect_test
