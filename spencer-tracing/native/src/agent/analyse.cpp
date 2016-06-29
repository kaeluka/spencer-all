#include "debug.h"
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <capnp/serialize.h>
#include <kj/io.h>
#include <kj/exception.h>
#include "events.h"
#include "Queries.hh"
#include "timer.h"
#include "bar.hh"
#include <climits>

using namespace Queries;

using std::cout;
using std::string;
auto query
#ifdef COUNT
     = _countkinds(); // traced(two(_implement<proto_check>(), _countkinds()));
     #define MAX_SPEED 1e6
     #define INTERMEDIATE_OUTPUT
#endif
#ifdef TRACE
     = traced(_implement<proto_dbg<37,proto_any> >());
     #define MAX_SPEED 1e6
#endif
#ifdef HEAPMOVES
     = _perClassStats<proto_heapMoves>();
     #define MAX_SPEED 1e6
//     #define INTERMEDIATE_OUTPUT
#endif
#ifdef HEAPMOVES_OBJ
     = _perObjectStats<proto_heapMoves>();
     #define MAX_SPEED 1e6
//     #define INTERMEDIATE_OUTPUT
#endif
#ifdef MOVES
     = _perClassStats<proto_moves>();
     #define MAX_SPEED 1e6
//     #define INTERMEDIATE_OUTPUT
#endif
#ifdef MOVES_OBJ
     = _perObjectStats<proto_moves>();
     #define MAX_SPEED 1e6
//     #define INTERMEDIATE_OUTPUT
#endif
#ifdef CHECKTRACE
     = _ignoreBackgroundThreads<_checkStack>();
     //= two(traced(succeed()), two(_implement<proto_any>(),_ignoreBackgroundThreads<_checkStack>()));
     #define MAX_SPEED 50000
//     #define INTERMEDIATE_OUTPUT
#endif
#ifdef STATIONARY_OBJECTS
     = _ignoreBackgroundThreads<_perClassStats<proto_stationaryObjects> >();
     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
#endif
#ifdef STATIONARY_OBJECTS_OBJ
     = _ignoreBackgroundThreads<_perObjectStats<proto_stationaryObjects> >();
     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
#endif
//#ifdef THREADLOCAL
//     = _perClassStats<proto_dbg<200, proto_thread_local> >();
//     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
//#endif
#ifdef IMMUTABLE
     = _ignoreBackgroundThreads<_perClassStats<proto_immutable> >();
     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
#endif
#ifdef IMMUTABLE_OBJ
     = _ignoreBackgroundThreads<_perObjectStats<proto_immutable> >();
     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
#endif
//#ifdef IMMUTABLE_VS_HEAPMOVES
//     = _perCommonSubsetsStats<proto_immutable, proto_heapMoves>();
//     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
//#endif
//#ifdef IMMUTABLE_VS_THREADLOCAL
//     = _perCommonSubsetsStats<proto_immutable, proto_thread_local>();
//     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
//#endif
//#ifdef IMMUTABLE_VS_STATIONARY
//     = _perCommonSubsetsStats<proto_immutable, proto_stationaryObjects>();
//     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
//#endif
#ifdef LOG_FIELDREFS
     = _ignoreBackgroundThreads<_logFieldRefs>();
     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
#endif
#ifdef LOG_VARREFS
     = _ignoreBackgroundThreads<_logVarRefs>();
     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
#endif
#ifdef LOG_TYPES
     = _ignoreBackgroundThreads<_logTypes>();
     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
#endif
#ifdef LOG_DOMINATORS
     //= _ignoreBackgroundThreads<_logDominators>();
     = two(traced(succeed()), _ignoreBackgroundThreads<_logDominators>());
     #define MAX_SPEED 500000
//     #define INTERMEDIATE_OUTPUT
#endif
#ifdef LIFETIMES
     = _perObjectQuery<proto_global_lifetime>();
     #define MAX_SPEED 1e6
#endif
#ifdef USETIMES
     = _perObjectQuery<proto_use_counter>();
     #define MAX_SPEED 1e6
#endif
#ifdef EVENTTIMES
     = _perObjectQuery<proto_event_counter>();
     #define MAX_SPEED 1e6
#endif
#ifdef CLASSNAMES
     = _perObjectQuery<proto_class_name_extractor>();
     #define MAX_SPEED 1e6
#endif
#ifdef HEAPMOVES_OBJ_2
     = _perObjectQuery<proto_heapMoves>();
     #define MAX_SPEED 1e6
#endif


// traced(two(_implement<proto_objectGraph>(),
// _countkinds()));

// traced(two(_implement<proto_objectGraph>(),
//            two(_countkinds(), _implement<proto_heapMoves>())));
// traced(_implement<proto_classAliasingGraph>());
// traced(_implement<proto_objectGraph>());
//_implement<proto_heapMoves>();
//    _implement<proto_unique>();
//_implement<proto_heapunique<1> >();
// traced(succeed());
// succeed();
int main(int argc, char *argv[]) {
  int fd;
  std::string file;
  if (argc < 2) {
    fd = 0; // STDIN
    file = "stdin";
  } else {
    fd = open(argv[1], O_RDONLY);
    file = argv[1];
  }

  // std::cout << "Running query '" << query.description() << "' on <" << file
  //          << ">\n";

  capnp::ReaderOptions options;

  options.traversalLimitInWords = ULLONG_MAX;
  long cnt = 0;
  const long SLICE=1e7;

  FieldLoadEvt::Reader fdldR;
  try {
    while (true) {
#ifdef INTERMEDIATE_OUTPUT
      for (int _i = 0; _i<5; ++_i) {
#endif
        tik();
        for (int _j = 0; _j<SLICE; ++_j) {
          capnp::StreamFdMessageReader reader(fd, options);
          AnyEvt::Reader next = reader.getRoot<AnyEvt>();
          query.process(next);
        }
        cnt += SLICE;
        double tok_s = ((double)tok_us())/1e6;
        std::cerr << argv[0] << ": EVT "<<(cnt/1E6)<<"E6 ";
        bar("evts/sec", SLICE/tok_s, MAX_SPEED, std::cerr);
#ifdef INTERMEDIATE_OUTPUT
      }
      cerr << "===========\n";
      query.description(cerr);
      cerr << "\n===========\n";
#endif
    }
  } catch (const kj::Exception &ignore) {
    //cout << "done\n";
    query.description(cout);
    cout << "\n";
  }

  if (fd != 0) {
    close(fd);
  }
  return 0;
}
