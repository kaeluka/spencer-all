#include "protocols.hh"

std::unordered_map<string, long> proto_heapMoves::perClassFailures;
std::unordered_map<string, long> proto_heapMoves::perClassQueries;

/// Gets incremented each time there is an event in the system
long global_event_counter = 0;

//refgraph<string> proto_classAliasingGraph::classAliasing;
//refgraph<long> proto_objectGraph::objectGraph;
