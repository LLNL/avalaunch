PMI=../../../install
SPAWN=../../../spawnnet/install

LIBS= \
  -libverbs \
  -lpthread \
  -lrt \
  -L../../../spawnnet/install/lib \
  -lspawn

all: clean
	gcc -g -O0 -o pmi_test  pmi_test.c  -I$(PMI)/include -I$(SPAWN)/include $(FLAGS) $(PMI)/lib/libpmi.a $(LIBS)
	gcc -g -O0 -o ring_test ring_test.c -I$(PMI)/include -I$(SPAWN)/include $(FLAGS) $(PMI)/lib/libpmi.a $(LIBS)
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
	rm -rf *.o pmi_test ring_test binary_size-1g binary_size-512m binary_size-256m binary_size-128m binary_size-64m binary_size-32m binary_size-16m ring binary_size-8m binary_size-4m binary_size-2m
