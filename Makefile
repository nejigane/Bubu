# Makefile
.PHONY: all
all: test

test: TestMain.o DBMTest.o BubuTest.o Bubu.o
	g++ -L/usr/local/lib -lgtest -o bubutest TestMain.o DBMTest.o BubuTest.o Bubu.o

TestMain.o: test/TestMain.cpp
	g++ -c test/TestMain.cpp

DBMTest.o: test/DBMTest.cpp
	g++ -I./include -c test/DBMTest.cpp
DBMTest.o: include/bb/DBM.hpp

BubuTest.o: test/BubuTest.cpp
	g++ -I./include -c test/BubuTest.cpp
BubuTest.o: include/bb/Bubu.hpp

Bubu.o: src/Bubu.cpp
	g++ -I./include -c src/Bubu.cpp
Bubu.o: include/bb/Bubu.hpp include/bb/DBM.hpp

.PHONY: clean
clean:
	rm -rf *.o bubutest