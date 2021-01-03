LIBC = $(wildcard libc/*.cpp)
CC = g++
CFLAGS = -std=c++11

simedc: simedc.cpp $(LIBC)
	$(CC) $(CFLAGS) -o $@ $^ -lpthread

clean:
	rm simedc libc/*.o
