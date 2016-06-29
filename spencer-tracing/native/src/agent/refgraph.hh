#ifndef REFGRAPH_HH
#define REFGRAPH_HH
#include <boost/graph/graph_traits.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/labeled_graph.hpp>
#include <boost/graph/graphviz.hpp>
#include "Debug.h"

using namespace boost;
using std::string;

using Label = const string;

template <class Key> struct Vtx {
  int numInstances = 0;
  Key key;
  //string label = "???";

  string toString() const {
    std::stringstream ss;
    ss << this->key;// << ":" << this->label;
    return ss.str();
  }

  friend std::ostream &operator<<(std::ostream &os, const Vtx<Key> &vtx) {
    os << vtx.toString();
    return os;
  }
};

struct Edg {
  int numEdges = 1;
  float fanOut = 0;
  string edgeLabel = "...";
  std::map<Label, int> counts;

  void addLabel(const Label &lbl) { counts[lbl] = counts[lbl] + 1; }

  void dropLabel(const Label &lbl) {
    counts[lbl] = counts[lbl] - 1;
    // assert(counts[lbl] >= 0);
  }

  string toString() const {
    std::stringstream ss;
    assert(counts.size() > 0);
    if (counts.size() > 0) {

      for (const auto &kv : this->counts) {
        ss << kv.first << ":" << kv.second << " ";
      }

      return ss.str();
    } else {
      return "this->counts.size() must not be zero";
    }
  }

  friend std::ostream &operator<<(std::ostream &os, const Edg &edg) {
    os << edg.toString();
    return os;
  }
};

template <class Key> struct refgraph {
  using Graph =
      labeled_graph<adjacency_list<vecS, vecS, bidirectionalS, Vtx<Key>, Edg>, Key>;
  using Vertex = typename graph_traits<Graph>::vertex_descriptor;
  using Edge = typename graph_traits<Graph>::edge_descriptor;
  Graph g;

  inline void deltaRef(const Key &from, const string &elbl, const Key &to,
                       bool isDropped) {
                         return;
    auto vfrom = g.add_vertex(from);
    auto vto = g.add_vertex(to);
    /////////// assert(g[from].label != string("???"));
    /////////// assert(g[to].label != string("???"));

    // g[from].label = l;
    // g[to].label = to;
    bool b;
    Edge e;
    tie(e, b) = edge(vfrom, vto, g);
    if (isDropped) {
      // assert(b);
      if (b) {
        g[e].dropLabel(elbl);
      } else {
      //  std::cerr << "WARNING: tried dropping ref that I didn't know about\n";
      }
    } else {
      if (!b) {
        add_edge(vfrom, vto, Edg(), g);
      }
      tie(e, b) = edge(vfrom, vto, g);
      g[e].addLabel(elbl);
    }
  }

  template <class G> struct myLabelWriter {
    myLabelWriter(G _g) : g(_g) {}
    template <class VertexOrEdge>
    void operator()(std::ostream &out, const VertexOrEdge &v) const {
      out << "[label=\"" << g[v] << "\"]";
    }

    G g;
  };

  template <class G> struct myEdgeWriter {
      myEdgeWriter(G _g) : g(_g) {}
    template <class VertexOrEdge>
    void operator()(std::ostream &out, const VertexOrEdge &v) const {
      out << "[label=\"" << g[v] << "\"]";
    }

    G g;
  };

  void addNode(const Key &key, const string &label) {
    this->g.add_vertex(key);
    DODBG("adding node " << key << ":" << label);
    this->g[key].key = key;
    //this->g[key].label = label;
  }

  void addOneEdge(const Key &from, const string &elbl, const Key &to) {
    DBG("adding edge [" << from << "] --" << elbl << "--> [" << to << "]");
    this->deltaRef(from, elbl, to, false);
  }

  void dropOneEdge(const Key &from, const string &elbl, const Key &to) {
    this->deltaRef(from, elbl, to, true);
  }

  void dumpDot(const string &filepath) {
    std::ofstream dot(filepath);

    myEdgeWriter<Graph &> ew(this->g);
    myLabelWriter<typename Graph::graph_type &> lw(this->g.graph());
    write_graphviz(dot, g, lw, ew, boost::default_writer());
    dot.close();
  }
};

#endif // REFGRAPH_HH
