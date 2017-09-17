DACAPO=../dacapo/dacapo-9.12-bach.jar
CAPNPLIB=/usr/local/Cellar/capnp/0.5.1/lib/
#lib/java_util_hashtable.jar
BCP=-Xbootclasspath/p:$(PWD)/lib/asm-debug-all-5.0.3.jar
JLIB=-cp lib/asm-debug-all-5.0.3.jar

UNAME=$(shell uname)
PWD=$(shell pwd)
CCFILES=$(AliasingLogger.cpp)
OFILES=$(CCFILES:.cpp=.o)

JAVAFILES=$(shell ls *.java)
CLASSFILES=$(JAVAFILES:.java=.class)

CC=g++ -std=c++14 -Wall -O2
###IFLAGS=-I /System/Library/Frameworks/JavaVM.framework/Headers -I "$(JAVA_HOME)/include/" -I /opt/local/lib/gcc48/gcc/x86_64-apple-darwin12/4.8.1/include/ -I $(JAVA_HOME)/include/darwin/
IFLAGS=-I /usr/lib/jvm/java-8-openjdk-amd64/include/linux -I /usr/lib/jvm/java-8-openjdk-amd64/include/

all: lib/NativeInterface.so NativeInterface.class
	#

NativeInterface.class: NativeInterface.java
	javac NativeInterface.java

analyse: analysers/countEvents analysers/traceEvents analysers/heapMoves analysers/lifeTimes analysers/heapMovesObj analysers/moves analysers/movesObj analysers/checkTrace analysers/stationaryObjects analysers/stationaryObjectsObj  analysers/immutable analysers/immutableObj analysers/logFieldRefs analysers/logVarRefs analysers/logTypes analysers/heapMovesObj2 analysers/classNames
	@echo done

NativeInterface.h: NativeInterface.class
	javah NativeInterface

lib/NativeInterface.so: NativeInterface.cpp NativeInterface.h events.o tagging.hh callstack.hh MonitorGuard.hh
	$(CC) events.o -c -fPIC $(LIBS) $(IFLAGS) NativeInterface.cpp -o lib/NativeInterface.so

%.o: %.cpp %.hh
	$(CC) -Wno-unused-variable -c $*.cpp

events.o: events.h
	$(CC) -c $*.cpp

%.class: %.java
	javac  -Xlint:deprecation $(JLIB) $<

events.h: ../capnp/com/github/kaeluka/spencer/events.capnp
	capnp -I../capnp/ compile ../capnp/com/github/kaeluka/spencer/events.capnp -oc++
	mv ../capnp/com/github/kaeluka/spencer/events.capnp.h events.h
	mv ../capnp/com/github/kaeluka/spencer/events.capnp.c++ events.cpp
	sed -i.bak "s/events.capnp.h/events.h/" events.cpp

clean:
	rm -f NativeInterface.class
	rm -f test/*.class
	rm -f *.o
	rm -f *~
	rm -f output
	rm -rf scratch
	rm -rf lib/scratch
	rm -f lib/NativeInterface.dylib
	rm -f lib/NativeInterface.so
	rm -f lib/NativeInterface.o
	rm -f events.h events.cpp
	rm -f NativeInterface.h
	rm -rf pmd/
	find analysers -type f -perm +111 -delete
	rm -rf analysers/*.dSYM

.PHONY: all clean test bench
