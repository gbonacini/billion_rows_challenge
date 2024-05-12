OPTS = -O3 -I. --std c++20  -arch=sm_86

all: process

process: process.cu
	nvcc $(OPTS)   -o $@  $<

install:
	install --target-directory /usr/local/bin process 

uninstall:
	rm -f /usr/local/bin/process

genfile: original_challenge_input_generator/gen.c
	gcc -o $@  $<

testfile: genfile
	./genfile 1000000000

clean:
	rm -f process genfile  measurements.txt
