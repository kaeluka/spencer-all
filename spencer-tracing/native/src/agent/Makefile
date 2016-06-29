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

ifeq ($(UNAME),Darwin)
	CC=clang++ -std=c++14 -Wall -ggdb -O2
	IFLAGS=-I /System/Library/Frameworks/JavaVM.framework/Headers -I "$(JAVA_HOME)/include/" -I /opt/local/lib/gcc48/gcc/x86_64-apple-darwin12/4.8.1/include/ -I $(JAVA_HOME)/include/darwin/
	LIBS=-stdlib=libc++ -lcapnp -lkj -lc
endif

all: lib/NativeInterface.so NativeInterface.class
	#javac -d . test/*.java

NativeInterface.class: NativeInterface.java
	javac NativeInterface.java

analyse: analysers/countEvents analysers/traceEvents analysers/heapMoves analysers/lifeTimes analysers/heapMovesObj analysers/moves analysers/movesObj analysers/checkTrace analysers/stationaryObjects analysers/stationaryObjectsObj  analysers/immutable analysers/immutableObj analysers/logFieldRefs analysers/logVarRefs analysers/logTypes analysers/heapMovesObj2 analysers/classNames
	@echo done

analysers/countEvents: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DCOUNT protocols.o Debug.o events.o -o $@ $<

analysers/traceEvents: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DTRACE protocols.o Debug.o events.o -o $@ $<

analysers/heapMoves: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DHEAPMOVES protocols.o Debug.o events.o -o $@ $<

## BEG Tobias' additions
analysers/lifeTimes: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DLIFETIMES protocols.o Debug.o events.o -o $@ $<

analysers/useTimes: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DUSETIMES protocols.o Debug.o events.o -o $@ $<

analysers/eventTimes: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DEVENTTIMES protocols.o Debug.o events.o -o $@ $<

analysers/classNames: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DCLASSNAMES protocols.o Debug.o events.o -o $@ $<

analysers/heapMovesObj2: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DHEAPMOVES_OBJ_2 protocols.o Debug.o events.o -o $@ $<
## END Tobias' additions

analysers/heapMovesObj: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DHEAPMOVES_OBJ protocols.o Debug.o events.o -o $@ $<

analysers/moves: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DMOVES protocols.o Debug.o events.o -o $@ $<

analysers/movesObj: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DMOVES_OBJ protocols.o Debug.o events.o -o $@ $<

analysers/immutable: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DIMMUTABLE protocols.o Debug.o events.o -o $@ $<

analysers/immutableObj: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DIMMUTABLE_OBJ protocols.o Debug.o events.o -o $@ $<

analysers/checkTrace: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DCHECKTRACE protocols.o Debug.o events.o -o $@ $<

analysers/stationaryObjects: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DSTATIONARY_OBJECTS protocols.o Debug.o events.o -o $@ $<

analysers/stationaryObjectsObj: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DSTATIONARY_OBJECTS_OBJ protocols.o Debug.o events.o -o $@ $<

analysers/logFieldRefs: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DLOG_FIELDREFS protocols.o Debug.o events.o -o $@ $<

analysers/logVarRefs: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DLOG_VARREFS protocols.o Debug.o events.o -o $@ $<

analysers/logTypes: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
	$(CC) $(IFLAGS) $(LIBS) -DLOG_TYPES protocols.o Debug.o events.o -o $@ $<

#analysers/logDominators: analyse.cpp bar.hh Queries.hh bar.hh callstack.hh protocols.o events.o NativeInterface.h Debug.o
#	$(CC) $(IFLAGS) $(LIBS) -DLOG_DOMINATORS protocols.o Debug.o events.o -o $@ $<

lib/NativeInterface.so: lib/NativeInterface.o NativeInterface.h Debug.o
	$(CC) Debug.o -shared $(LIBS) -o lib/NativeInterface.so lib/NativeInterface.o -L.
	$(CC) Debug.o -dynamiclib $(LIBS) -o lib/NativeInterface.dylib lib/NativeInterface.o -L.

NativeInterface.h: NativeInterface.java
	javah NativeInterface

lib/NativeInterface.o: NativeInterface.cpp NativeInterface.h events.o tagging.hh callstack.hh Debug.o MonitorGuard.hh
	$(CC) events.o Debug.o -c -fPIC $(LIBS) $(IFLAGS) NativeInterface.cpp -o lib/NativeInterface.o

//protocols.o: protocols.cpp protocols.hh refgraph.hh Debug.o
protocols.o: protocols.cpp protocols.hh Debug.o

events.o: events.h
		$(CC) -c $*.cpp

Debug.o: Debug.cpp Debug.h
	$(CC) $(IFLAGS) -c Debug.cpp

%.o: %.cpp %.hh
	$(CC) -Wno-unused-variable -c $*.cpp

%.class: %.java
	javac  -Xlint:deprecation $(JLIB) $<

events.h: events.capnp
	capnp compile events.capnp -oc++
	mv events.capnp.h events.h
	mv events.capnp.c++ events.cpp
	sed -i.bak "s/events.capnp.h/events.h/" events.cpp

$(DACAPO):
	@test ! -e $(DACAPO) && (echo "can't find $(DACAPO). please download from http://www.dacapobench.org/"; false)

bench: all $(DACAPO)
	./runAllBenchmarks.sh

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
