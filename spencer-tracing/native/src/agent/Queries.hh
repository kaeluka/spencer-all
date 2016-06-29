#ifndef QUERIES_H
#define QUERIES_H

#include <queue>
#include <vector>
#include <stack>
#include <functional>
#include <map>
#include <set>
#include <iterator>
#include <sstream>
#include <algorithm>
#include "events.h"
#include <capnp/pretty-print.h>
#include <assert.h>
#include "protocols.hh"
#include <kj/string-tree.h>
#include <cmath>
#include "Debug.h"
#include "callstack.hh"
#include "NativeInterface.h"
// #include <boost/graph/dominator_tree.hpp>
// #include <boost/graph/adjacency_list.hpp>
//#include <capnp/dynamic.h>

namespace Queries {
using namespace std;

typedef int NONE;

struct _succeed {
  _succeed() {}

  template <class T> inline void configure(const T &) {}

  inline void process(AnyEvt::Reader &) {}

  inline bool isAccepting() const { return true; }

  inline bool isFrozen() const { return true; }

  const void description(std::ostream &out) const { out << ""; }
};

template <class T> struct _debug {
  T inner;

  _debug(T &&_inner) : inner(_inner) {}

  template <class T2> inline void configure(const T2 &) {}

  inline void process(AnyEvt::Reader &evt) {
    cout << "================================\n";
    cout << "Incoming " << evt.toString().flatten().cStr()
         << "\n Press any key to process\n";
    cin.ignore();
    // cin.get();

    // char c;
    // std::cin >> c;

    bool was_accepting = inner.isAccepting();
    bool was_frozen = inner.isFrozen();
    this->inner.process(evt);
    bool is_accepting = inner.isAccepting();
    bool is_frozen = inner.isFrozen();
    if (was_accepting != is_accepting) {
      cout << "CHANGE: isAccepting() now " << is_accepting << "\n";
    }
    if (was_frozen != is_frozen) {
      cout << "CHANGE: isFrozen() now " << is_frozen << "\n";
    }
    inner.description(cout);
    cout << "\n";
  }

  inline bool isAccepting() const { return true; }

  inline bool isFrozen() const { return true; }
  const void description(std::ostream &out) const { inner.description(out); }
};

/*
template <class T> struct _describeWhenDone {
  T inner;

  _describeWhenDone(T &&_inner) : inner(std::move(_inner)) {}

  template <class T2> inline void configure(const T2 &) {}

  inline void process(AnyEvt::Reader &evt) { this->inner.process(evt); }

  inline bool isAccepting() const { return true; }

  inline bool isFrozen() const { return true; }

  const std::string description() const { return this->inner.description(); }

  ~_describeWhenDone() { std::cout << this->description() << std::endl; }
};
*/

struct _logVarRefs {
  using VarMap = std::map<std::string, std::set<long> >;
  std::map<long, VarMap> log;

  template <class T> inline void configure(const T &) {}

  inline void process(AnyEvt::Reader &evt) {
    if (evt.which() == AnyEvt::VARSTORE) {
      const auto &vs = evt.getVarstore();
      stringstream var;
      var << vs.getCallermethod().cStr()<<"::"<<(int)vs.getVar();
      log[vs.getCallertag()][var.str()].insert(vs.getNewval());
    }
  }

  inline bool isAccepting() const { return true; }

  inline bool isFrozen() const { return true; }

  const void description(std::ostream &out) const {
    const string className = "N/A";
    out<<"HOLDER\tVAR\tVALUES\n";
    for (auto oit = this->log.begin(); oit != this->log.end(); ++oit) {
      const long holder = oit->first;
      const VarMap &vm = oit->second;
      for (auto vit = vm.begin(); vit != vm.end(); ++vit) {
        const std::string &fname = vit->first;
        const std::set<long> &values = vit->second;
        out << holder<<"\t"<<fname<<"\t[ ";
        for (auto val=values.begin(); val!=values.end(); ++val) {
          out << *val<<" ";
        }
        out << "]\n";
      }
    }
    out << "\n";
  }
};

template <class T> struct _ignoreBackgroundThreads {
  T inner;

  template <class T2> inline void configure(const T2 &t) {
    inner.configure(t);
  }

  inline void process(AnyEvt::Reader &evt) {
    if (!isBackgroundThreadEvent(evt)) {
      this->inner.process(evt);
    }
  }

  inline bool isAccepting() const { return inner.isAccepting(); }

  inline bool isFrozen() const { return inner.isFrozen(); }

  const void description(std::ostream &out) const {
    this->inner.description(out);
  }

  static bool isBackgroundThreadEvent(AnyEvt::Reader &evt) {
    return isBackgroundThread(getThreadName(evt));
  }

private:
  static std::string getThreadName(AnyEvt::Reader &evt) {
    switch (evt.which()) {
      case AnyEvt::METHODENTER: {
        return evt.getMethodenter().getThreadName();
        case AnyEvt::OBJFREE: {
          return "JVM_Thread<GC>";
        }
        case AnyEvt::OBJALLOC: {
          return evt.getObjalloc().getThreadName();
        }
        case AnyEvt::FIELDSTORE: {
          return evt.getFieldstore().getThreadName();
        }
        case AnyEvt::FIELDLOAD: {
          return evt.getFieldload().getThreadName();
        }
        case AnyEvt::VARSTORE: {
          return evt.getVarstore().getThreadName();
        }
        case AnyEvt::VARLOAD: {
          return evt.getVarload().getThreadName();
        }
        case AnyEvt::READMODIFY: {
          return evt.getReadmodify().getThreadName();
        }
        case AnyEvt::METHODEXIT: {
          return evt.getMethodexit().getThreadName();
        }
      }
    }
  }
  static bool isBackgroundThread(const std::string& name) {
    return
      (strstr(name.c_str(), "JVM_Thread<") != NULL)
      || (std::string("DestroyJavaVM") == name);
  }
};

template <class A, class B> struct _two {
  A a;
  B b;
  _two(A &&a, B &&b) : a(std::move(a)), b(std::move(b)) {}

  template <class T> inline void configure(const T &) {}

  inline void process(AnyEvt::Reader &evt) {
    this->a.process(evt);
    this->b.process(evt);
  }

  inline bool isAccepting() const { return true; }

  inline bool isFrozen() const { return true; }

  const void description(std::ostream &out) const {
    this->a.description(out);
    out << "\n";
    this->b.description(out);
  }
};

void bar(const string &name, long num, long maximum) {
  if (maximum <= 0) {
    return;
  }

  std::cout << name << " ";
  printf(" (%15ld) ", num);

  int next_power_of_ten = (int)pow(10.0, ceil(log10((double)maximum)));
  const long BARLENGTH = (num * 70 / next_power_of_ten);
  for (int i = 0; i < BARLENGTH; ++i) {
    cout << "#";
  }
  cout << "\n";
}

struct _countkinds {

  long methodenter_cnt = 0;
  long objalloc_cnt = 0;
  long objfree_cnt = 0;
  long fieldstore_cnt = 0;
  long fieldload_cnt = 0;
  long varstore_cnt = 0;
  long varload_cnt = 0;
  long modify_cnt = 0;
  long read_cnt = 0;
  long methodexit_cnt = 0;

  long totalCnt() const {
    return this->methodenter_cnt + this->objalloc_cnt + this->objfree_cnt +
           this->fieldstore_cnt + this->fieldload_cnt + this->varstore_cnt +
           this->varload_cnt + this->modify_cnt + this->read_cnt +
           this->methodexit_cnt;
  }

  long maxCnt() const {
    return max(
        methodenter_cnt,
        max(objalloc_cnt,
            max(objfree_cnt,
                max(fieldstore_cnt,
                    max(fieldload_cnt,
                        max(varstore_cnt,
                            max(varload_cnt,
                                max(modify_cnt,
                                    max(read_cnt, methodexit_cnt)))))))));
  }
  //    // additional data is ignored:
  //    template <class T> _countkinds(_countkinds, T config) {
  //    }

  template <class T> inline void configure(const T &) {}

  inline void process(AnyEvt::Reader &evt) {
    // std::cout << "counting event!\n";
    switch (evt.which()) {
    case AnyEvt::METHODENTER:
      methodenter_cnt++;
      // cout << "adding enter" << endl;
      // std::cout << "METHODENTER\n";
      break;
    case AnyEvt::OBJALLOC:
      // std::cout << "OBJALLOC:\n";
      objalloc_cnt++;
      break;
    case AnyEvt::OBJFREE:
      objfree_cnt++;
      // std::cout << "OBJFREE:\n";
      break;
    case AnyEvt::FIELDSTORE:
      fieldstore_cnt++;
      // std::cout << "FIELDSTORE:\n";
      break;
    case AnyEvt::FIELDLOAD:
      fieldload_cnt++;
      //        std::cout << "FIELDLOAD:\n";
      //        std::cout << "  holderclass = '" <<
      // std::string(evt.getFieldload().getHolderclass()) << "'\n";
      //        std::cout << "  holdertag = " <<
      // evt.getFieldload().getHoldertag() << "\n";
      //        std::cout << "  val = " << evt.getFieldload().getVal() << "\n";
      //        std::cout << "  fname = '" <<
      // std::string(evt.getFieldload().getFname()) << "'\n";
      //        std::cout << "  type = '" <<
      // std::string(evt.getFieldload().getType()) << "'\n";
      //        std::cout << "  callermethod = '" <<
      // std::string(evt.getFieldload().getCallermethod()) << "'\n";
      //        std::cout << "  callerclass = '" <<
      // std::string(evt.getFieldload().getCallerclass()) << "'\n";
      //        std::cout << "  callertag = " <<
      // evt.getFieldload().getCallertag() << "\n";
      //        std::cout << "  threadName = " <<
      // evt.getFieldload().getThreadName() << "\n";
      break;
    case AnyEvt::VARSTORE:
      varstore_cnt++;
      // std::cout << "VARSTORE:\n";
      break;
    case AnyEvt::VARLOAD:
      varload_cnt++;
      // std::cout << "VARLOAD:\n";
      break;
    case AnyEvt::READMODIFY:
      if (evt.getReadmodify().getIsModify()) {
        modify_cnt++;
      } else {
        read_cnt++;
      }
      // std::cout << "READMODIFY:\n";
      break;
    case AnyEvt::METHODEXIT:
      methodexit_cnt++;
      // std::cout << "METHODENTER\n";
      break;
    }
  }

  inline bool isAccepting() const { return true; }

  inline bool isFrozen() const { return true; }

  inline const void description(std::ostream &out) const {
    // FIXME: adapt bar to take stream
    bar("methodenter = ", methodenter_cnt, this->maxCnt());
    bar("objalloc    = ", objalloc_cnt, this->maxCnt());
    bar("objfree     = ", objfree_cnt, this->maxCnt());
    bar("fieldstore  = ", fieldstore_cnt, this->maxCnt());
    bar("fieldload   = ", fieldload_cnt, this->maxCnt());
    bar("varstore    = ", varstore_cnt, this->maxCnt());
    bar("varload     = ", varload_cnt, this->maxCnt());
    bar("read        = ", read_cnt, this->maxCnt());
    bar("modify      = ", modify_cnt, this->maxCnt());
    bar("methodexit  = ", methodexit_cnt, this->maxCnt());
    out <<
        "TOTAL: " << this->totalCnt() << " events" << std::endl;
  }
};

template <class Inner> class _filter {
  const std::function<bool(AnyEvt::Reader &)> tst;
  const std::string desc;
  Inner inner;

public:
  _filter(std::function<bool(AnyEvt::Reader &)> tst, const std::string desc,
          Inner inner)
      : tst(tst), desc(desc), inner(inner) {}

  template <class T>
  _filter(_filter<Inner> other, T config)
      : tst(other.tst), desc(other.desc), inner(other.inner, config) {}

  template <class T> inline void configure(const T &config) {
    inner.configure(config);
  }

  inline void process(AnyEvt::Reader &evt) {
    if (this->tst(evt)) {
      this->inner.process(evt);
    }
  }

  inline bool isAccepting() const { return this->inner.isAccepting(); }

  inline bool isFrozen() const { return this->inner.isFrozen(); }

  inline const void description(std::ostream &out) const {
    out <<"Filter("<<this->desc<<",";
    inner.description(out);
    out << ")";
  }
};

//  template<class Inner>
//  class _filterCallee {
//    long objid;
//    Inner inner;
//  public:
//    _filterCallee(Inner inner) : objid(-1), inner(inner) { }
//
//    /**
//     * Config constructor will copy the inner query, but use the
//     * passed in objid to filter. Useful to _onePerObject
//     */
//    _filterCallee(const _filterCallee<Inner> &other,
//                 long objid) : objid(objid), inner(other.inner, objid) { }
//
//    inline void configure(const long &objid) {
//      this->objid = objid;
//      inner.configure(objid);
//    }
//
//    template <class T>
//    inline void configure(const T& config) {
//      inner.configure(config);
//    }
//
//    inline void process(AnyEvt::Reader &evt) {
//      bool interesting = false;
//      switch (evt.which()) {
//      case AnyEvt::METHODENTER:
//        interesting = (evt.getMethodenter().getCalleetag() == this->objid);
//        break;
//      case AnyEvt::OBJFREE:
//        interesting = (evt.getObjfree().getTag() == this->objid);
//        break;
//      case AnyEvt::FIELDSTORE:
//        interesting = (evt.getFieldstore().getHoldertag() == this->objid);
//        break;
//      case AnyEvt::FIELDLOAD:
//        interesting = (evt.getFieldload().getHoldertag() == this->objid);
//        break;
//      case AnyEvt::VARSTORE:
//        interesting = (evt.getVarstore().getCallertag() == this->objid);
//        break;
//      case AnyEvt::VARLOAD:
//        interesting = (evt.getVarload().getCallertag() == this->objid);
//        break;
//      case AnyEvt::READMODIFY:
//        interesting = (evt.getReadmodify().getCallertag() == this->objid);
//        break;
//      case AnyEvt::METHODEXIT:
//        interesting = (evt.getMethodexit().getThreadName() == this->objid);
//        break;
//      }
//      if (interesting) {
//        inner.process(evt);
//      }
//    }
//
//    inline bool isAccepting() const {
//      return this->inner.isAccepting();
//    }
//
//    inline bool isFrozen() const {
//      return this->inner.isFrozen();
//    }
//
//    inline const std::string description() const {
//      stringstream concat;
//      concat << "FilterCallee(" << this->objid << "," << inner.description()
// << ")";
//      return concat.str();
//    }
//  };

template <class Inner> class _forever {
  Inner inner;

public:
  _forever(Inner inner) : inner(inner) {}

  template <class T>
  _forever(_forever<Inner> other, T config)
      : inner(other.inner, config) {}

  template <class T> inline void configure(const T &config) {
    inner.configure(config);
  }

  inline void process(AnyEvt::Reader &evt) {
    if (this->isAccepting()) {
      inner.process(evt);
    }
  }

  inline bool isAccepting() const { return this->inner.isAccepting(); }

  inline bool isFrozen() const {
    // once it's failed, it's always failed:
    return !this->isAccepting();
  }

  inline const void description(std::ostream &out) const {
    out << "Forever(";
    inner.description(out);
    out << ")";
  }
};

template <class State, class Config, class Inner> class _observe {
  State state;
  Config config;
  const std::function<bool(State &, const Config &, AnyEvt::Reader &)> tst;
  const std::string desc;
  Inner inner;
  bool accepting = true;

public:
  // _observe(std::function<bool(State&, AnyEvt::Reader&)> &tst,
  //         std::string desc) : tst(tst), desc(desc), inner(Queries::yes()) { }

  _observe(std::function<bool(State &, const Config &, AnyEvt::Reader &)> &tst,
           const std::string desc, const Config &config)
      : config(config), tst(tst), desc(desc), inner(), accepting(true) {}

  _observe(std::function<bool(State &, const Config &, AnyEvt::Reader &)> &tst,
           const std::string desc, Inner inner, State state, Config config)
      : state(state), config(config), tst(tst), desc(desc), inner(inner),
        accepting(true) {}

  _observe(const _observe<State, Config, Inner> &other, const Config &config)
      : state(other.state), config(config), tst(other.tst), desc(other.desc),
        inner(other.inner, config) {}

  inline void configure(const Config &config) {
    this->config = config;
    inner.configure(config);
  }

  template <class T> inline void configure(const T &config) {
    inner.configure(config);
  }

  inline void process(AnyEvt::Reader &evt) {
    this->accepting = tst(this->state, this->config, evt);
    // TODO: this shouldn't have an inner.. the inner can be in the State if
    // need be!
    if (this->isAccepting()) {
      this->inner.process(evt);
    }
  }

  inline bool isAccepting() const { return this->accepting; }

  inline bool isFrozen() const { return false; }

  inline void description(std::ostream &out) const {
    // std::string txt = "Observe(";
    // txt.append(this->desc);
    // txt.append(")");
    out << this->desc;
    this->inner.description(out);
  }
};

/**
 * Calls the appropriate handles of the protocol. If a protocol's
 * handle returns false, will fail forever.
 */
template <class Protocol> struct _implement {

  // using callstack = std::stack<frame>;
  // const frame topframe;
  std::map<long, Protocol> actives;
  std::set<long> ignored;
  std::set<long> dead;
  std::map<std::string, callstack> stacks;

  _implement() {
    const char *syntheticClass = "JVM_Internals";
    this->newProtocolFor(NativeInterface_SPECIAL_VAL_JVM, syntheticClass);
    this->getIfActive(NativeInterface_SPECIAL_VAL_JVM)
        .allocate(NativeInterface_SPECIAL_VAL_JVM, syntheticClass, "main");
  }

  template <class T> void configure(const T &) {}

  inline void process(AnyEvt::Reader &evt) {
    bool haveSeen = handleUnseenParticipants(evt);
    switch (evt.which()) {
    case AnyEvt::METHODENTER: {
      MethodEnterEvt::Reader me = evt.getMethodenter();

      long callee = me.getCalleetag();
      const frame &oldstackframe = this->getLastFrame(me.getThreadName());

      {
        stacks[me.getThreadName()]
            .pushFrame(me.getName(), me.getCalleeclass(), me.getCalleetag());
      }

      // can't be dead AND active
      ASSERT(!(this->isDead(callee) && this->isActive(callee)));

      if (this->isDead(callee)) {
        return;
      }

      if (NULL != strstr(me.getName().cStr(),"<init>")) {
        if (!this->isActive(callee)) {
          // this is NOT an <init> call to the super class!

          this->newProtocolFor(callee, me.getCalleeclass().cStr());
          if(
            !(
              this->getIfActive(callee).allocate(callee,
                me.getCalleeclass().cStr(),
                me.getThreadName().cStr())
               &&
              this->getIfActive(callee).newRefFromVar(
                false,
                oldstackframe.getThisId(),
                -1,
                oldstackframe.getClassName().c_str(),
                oldstackframe.getMethodName().c_str(),
                me.getThreadName().cStr())
            )
           ) {
            this->killProtocol(callee);
          }
          return;
        } else {
          // this is an <init> call to the super class. We forward only one.
          return;
        }
      }

      if (this->isActive(callee)) {
        ASSERT(me.getName() != string("<init>"));
        if (!this->getIfActive(callee)
                 .useFrom(false, // could be write
                          oldstackframe.getThisId(),
                          oldstackframe.getClassName().c_str(),
                          oldstackframe.getMethodName().c_str(),
                          NULL, /*no field use!*/
                          me.getThreadName().cStr())) {
          this->killProtocol(callee);
        }
        return;
      }

      return;
    }
    case AnyEvt::OBJALLOC: {
      ERR("not handling objalloc");
      // don't need to handle this, we will call dealloc on all
      // protocls later!
      return;
    }
    case AnyEvt::OBJFREE: {
      // don't need to handle this, we will call dealloc on all
      // protocls later!
      return;
    }
    case AnyEvt::FIELDSTORE: {
      const FieldStoreEvt::Reader fs = evt.getFieldstore();

      if (this->isActive(fs.getNewval())) {
        if (!this->getIfActive(fs.getNewval())
                 .newRefFromField(false, // is not dropped
                                  fs.getHoldertag(), fs.getHolderclass().cStr(),
                                  fs.getFname().cStr(),
                                  fs.getThreadName().cStr())) {
          this->killProtocol(fs.getNewval());
        }
      }
      if (this->isActive(fs.getOldval())) {
        if (!this->getIfActive(fs.getOldval())
                 .newRefFromField(true, // is dropped
                                  fs.getHoldertag(), fs.getHolderclass().cStr(),
                                  fs.getFname().cStr(),
                                  fs.getThreadName().cStr())) {
          this->killProtocol(fs.getOldval());
        }
      }
      if (this->isActive(fs.getHoldertag())) {
        if (!this->getIfActive(fs.getHoldertag())
                 .useFrom(true /* isModify */,
                          fs.getCallertag(),
                          fs.getCallerclass().cStr(),
                          NULL /* no method call */,
                          fs.getFname().cStr(),
                          fs.getThreadName().cStr())) {
          this->killProtocol(fs.getHoldertag());
        }
      }
      return;
    }
    case AnyEvt::FIELDLOAD: {
      FieldLoadEvt::Reader fl = evt.getFieldload();
      if (this->isActive(fl.getVal())) {
        if (!this->getIfActive(fl.getVal())
                 .loadFromField(fl.getHoldertag(), fl.getHolderclass().cStr(),
                                fl.getFname().cStr(),
                                fl.getThreadName().cStr())) {
          this->killProtocol(fl.getVal());
        }
      }
      if (this->isActive(fl.getHoldertag())) {
        if (!this->getIfActive(fl.getHoldertag())
                 .useFrom(false /* is no modify */,
                          fl.getCallertag(),
                          fl.getCallerclass().cStr(),
                          NULL /* no method call */,
                          fl.getFname().cStr(),
                          fl.getThreadName().cStr())) {
          this->killProtocol(fl.getHoldertag());
        }
      }
      return;
    }
    case AnyEvt::VARSTORE: {
      VarStoreEvt::Reader vs = evt.getVarstore();
      // BUG : not tracking oldval!
      // DBG("warning: not tracking old val in varstore event");
      int oldval = vs.getOldval();
      if (this->isActive(oldval)) {
        // if (vs.getCallertag() == 0) {
        //   DODBG(evt.toString().flatten().cStr());
        // }
        // ASSERT(vs.getCallertag() != 0);
        if (!this->getIfActive(oldval).newRefFromVar(
                 true, // is dropped
                 vs.getCallertag(), vs.getVar(), vs.getCallerclass().cStr(),
                 vs.getCallermethod().cStr(),
                 vs.getThreadName().cStr())) {
          this->killProtocol(oldval);
        }
      }

      if (this->isActive(vs.getNewval())) {
        // TODO: crash here (not allocd)
        if (!this->getIfActive(vs.getNewval()).newRefFromVar(
                 false, // is not dropped
                 vs.getCallertag(), vs.getVar(), vs.getCallerclass().cStr(),
                 vs.getCallermethod().cStr(),
                 vs.getThreadName().cStr())) {
          this->killProtocol(vs.getNewval());
        }
      }
      return;
    }
    case AnyEvt::VARLOAD: {
      VarLoadEvt::Reader vl = evt.getVarload();
      if (this->isActive(vl.getVal())) {
        if (!this->getIfActive(vl.getVal())
                 .loadFromVar(vl.getCallertag(), vl.getCallerclass().cStr(),
                              vl.getCallermethod().cStr(), vl.getVar(),
                              vl.getThreadName().cStr())) {
          this->killProtocol(vl.getVal());
        }
      }
      return;
    }

    case AnyEvt::READMODIFY: {
      ReadModifyEvt::Reader rm = evt.getReadmodify();
      //if (this->isActive(rm.getThreadName())) {
      //  this->getIfActive(rm.getThreadName()).useFrom(
      //      rm.getIsModify(),
      //      rm.getCallertag(),
      //      rm.getCallerclass().cStr(),
      //      NULL /* no method used! */,
      //      rm.getFname().cStr());
      //}
      if (this->isActive(rm.getCalleetag())) {
        if (!this->getIfActive(rm.getCalleetag())
                 .useFrom(rm.getIsModify(),
                          rm.getCallertag(),
                          rm.getCallerclass().cStr(),
                          NULL /* no method call */,
                          rm.getFname().cStr(),
                          rm.getThreadName().cStr())) {
          this->killProtocol(rm.getCalleetag());
        }
      }
      return;
    }
    case AnyEvt::METHODEXIT:
      MethodExitEvt::Reader me = evt.getMethodexit();
      stacks[me.getThreadName()].popFrame();
      return;
    }
  }

  inline bool isAccepting() const { return true; }

  inline bool isFrozen() const { return false; }

  inline const void description(std::ostream &out) const {
    const int SUCC = this->actives.size();
    const int TOTL = SUCC + this->dead.size();
    const float PERC = SUCC * 100.0 / TOTL;
    out << SUCC << " survivors (" << PERC << "%), " << this->dead.size()
       << " failed, " << TOTL << " total\n";

    out << "FAILED\n[ ";
    copy(this->dead.begin(), this->dead.end(), ostream_iterator<long>(out, " "));
    out << "]";

    out << Protocol::classSummary();
  }

  ~_implement() {
    for (auto &kv : this->actives) {
      kv.second.deallocate();
    }
  }

private:
  bool handleUnseenParticipant(long part, std::string subject,
                               AnyEvt::Reader &evt) {
    if (part >= 0 && !this->isActive(part)) {
      //if (this->ignored.find(part) == this->ignored.end()) {
      //  DODBG("have not seen "<<subject<<" "<<part<<" before. Event was "
      //        << evt.toString().flatten().cStr());
      //  this->ignoreProtocol(part);
      //}
      return false;
    } else {
      return true;
    }
  }

  bool handleUnseenParticipants(AnyEvt::Reader &evt) {
    switch (evt.which()) {
    case AnyEvt::METHODENTER: {
      MethodEnterEvt::Reader me = evt.getMethodenter();
      long callee = me.getCalleetag();
      if (me.getName() != string("<init>")) {
        return this->handleUnseenParticipant(callee, "callee", evt);
        // this->newProtocolFor(callee, me.getCalleeclass().cStr());
        // this->getIfActive(callee).allocate(callee,
        // me.getCalleeclass().cStr());
      }
      return true;
    }
    case AnyEvt::OBJFREE: {
      return this->handleUnseenParticipant(evt.getObjfree().getTag(),
                                           "freed object tag", evt);
    }
    case AnyEvt::OBJALLOC: {
      ASSERT(false && "not handling OBJALLOC");
      break;
    }
    case AnyEvt::FIELDSTORE: {
      return (this->handleUnseenParticipant(evt.getFieldstore().getHoldertag(),
                                            "holdertag", evt) &&
              this->handleUnseenParticipant(evt.getFieldstore().getNewval(),
                                            "newval", evt) &&
              this->handleUnseenParticipant(evt.getFieldstore().getOldval(),
                                            "oldval", evt) &&
              this->handleUnseenParticipant(evt.getFieldstore().getCallertag(),
                                            "caller", evt));
      // We don't require the thread to be allocated!
      // this->handleUnseenParticipant(fs.getThreadName(), "thread", evt);
    }
    case AnyEvt::FIELDLOAD: {
      return (this->handleUnseenParticipant(evt.getFieldload().getHoldertag(),
                                            "holdertag", evt) &&
              this->handleUnseenParticipant(evt.getFieldload().getVal(),
                                                    "val", evt) &&
              this->handleUnseenParticipant(evt.getFieldload().getCallertag(),
                                                    "callertag", evt));
      break;
    }
    case AnyEvt::VARSTORE: {
      return (this->handleUnseenParticipant(evt.getVarstore().getNewval(),
                                            "newval", evt) &&
              this->handleUnseenParticipant(evt.getVarstore().getOldval(),
                                            "oldval", evt) &&
              this->handleUnseenParticipant(evt.getVarstore().getCallertag(),
                                            "callertag", evt));
    }
    case AnyEvt::VARLOAD: {
      return (this->handleUnseenParticipant(evt.getVarload().getVal(), "val",
                                            evt) &&
              this->handleUnseenParticipant(evt.getVarload().getCallertag(),
                                            "callertag", evt));
    }
    case AnyEvt::READMODIFY: {
      return (this->handleUnseenParticipant(evt.getReadmodify().getCallertag(),
                                            "caller", evt) &&
              this->handleUnseenParticipant(evt.getReadmodify().getCalleetag(),
                                            "callee", evt));
    }
    case AnyEvt::METHODEXIT: {
      // return
      // this->handleUnseenParticipant(evt.getMethodexit().getThreadName(),
      //                                       "thread", evt);
      return true;
    }
    }
    ERR("switch above should be total");
  }

  const frame getLastFrame(const std::string &thread) {
    return this->stacks[thread].peekFrame();
  }

  inline Protocol &getIfActive(long obj) {
    ASSERT(this->isActive(obj));
    return this->actives.at(obj);
  }

  inline bool isActive(const long obj) const {
    return (this->actives.find(obj) != this->actives.end());
  }

  inline bool isDead(const long obj) const {
    return this->dead.find(obj) != this->dead.end();
  }

  inline void ignoreProtocol(const long obj) {
    // this might, or might not be, in the actives. We'll remove it to be sure!
    this->actives.erase(obj);
    this->ignored.insert(obj);
  }

  inline void killProtocol(const long obj) {
    // this might, or might not be, in the actives. We'll remove it to be sure!
    this->actives.erase(obj);
    this->dead.insert(obj);
  }

  inline void newProtocolFor(const long obj, const char *klass) {
    Protocol newProtocol;
    this->actives.insert(std::make_pair(obj, std::move(newProtocol)));
  }
};

struct _logFieldRefs {
  using FieldsMap = std::map<std::string, std::set<long> >;
  std::map<long, FieldsMap> log;
  _implement<proto_any> stack;

  template <class T> inline void configure(const T &) {}

  inline void process(AnyEvt::Reader &evt) {
    if (evt.which() == AnyEvt::FIELDSTORE) {
      DBG("got FIELDSTORE");
      const auto &fs = evt.getFieldstore();
      log[fs.getHoldertag()][fs.getFname()].insert(fs.getNewval());
    } else if (evt.which() == AnyEvt::METHODENTER) {
      DBG("got VARSTORE");
      const auto &me = evt.getMethodenter();
      if (strcmp(me.getName().cStr(), "<init>") == 0) {
        long caller =
          stack.stacks[me.getThreadName().cStr()].peekFrame().getThisId();
        log[caller]["<ALLOCATED_OBJS>"].insert(me.getCalleetag());
      }
    }
    stack.process(evt);
  }

  inline bool isAccepting() const { return true; }

  inline bool isFrozen() const { return true; }

  const void description(std::ostream &out) const {
    const string className = "N/A";
    out<<"HOLDER\tFIELD\tVALUES\n";
    for (auto oit = this->log.begin(); oit != this->log.end(); ++oit) {
      const long holder = oit->first;
      const FieldsMap &fm = oit->second;
      for (auto fit = fm.begin(); fit != fm.end(); ++fit) {
        const std::string &fname = fit->first;
        const std::set<long> &values = fit->second;
        out << holder<<"\t"<<fname<<"\t[ ";
        for (auto val=values.begin(); val!=values.end(); ++val) {
          out << *val<<" ";
        }
        out << "]\n";
      }
    }
    out <<"\n";
  }
};


template <class T> struct _perClassStats {
  _implement<T> inner;
  std::map<long, std::string> type;

  template <class T2> inline void configure(const T2 &) {}

  inline void process(AnyEvt::Reader &evt) {
    if (evt.which() == AnyEvt::METHODENTER) {
      auto me = evt.getMethodenter();
      if (std::string("<init>") == me.getName().cStr()) {
        //DODBG(me.toString().flatten().cStr());
        if (type.find(me.getCalleetag()) == type.end()) {
          this->type[me.getCalleetag()] = me.getCalleeclass();
          //DODBG("adding type: "<<me.getCalleeclass().cStr());
        }
      }
    }
    this->inner.process(evt);
  }

  inline bool isAccepting() const { return this->inner.isAccepting(); }

  inline bool isFrozen() const { return this->inner.isFrozen(); }

  const void description(std::ostream &out) const {
    //this->inner
    std::map<std::string, int> perClassSucc;
    std::map<std::string, int> perClassFail;
    std::set<std::string> classNames;
    for (auto it=this->type.begin(); it != this->type.end(); it++) {
      const auto &tag = it->first;
      const auto &className = it->second;
      classNames.insert(className);
      if (this->inner.actives.find(tag) != this->inner.actives.end()) {
        perClassSucc[className]++;
        //DODBG("alive tag "<<tag<<" has type "<<className<<perClassSucc[className]);
      } else {
        if (this->inner.dead.find(tag) != this->inner.dead.end()) {
          //DODBG("dead tag "<<tag<<" has type "<<className);
          perClassFail[className]++;
        }
      }
    }
    out<<"CLASS\tPROP\tSUCC\tFAIL\n";
    for (auto it=classNames.begin(); it != classNames.end(); it++) {
      const auto &className = *it;
      //DODBG("classname "<<className);
      const long   SUCC = perClassSucc[className];
      const long   FAIL = perClassFail[className];
      const double PROP = ((double)SUCC)/(SUCC+FAIL);
      out<<className<<"\t"<<PROP<<"\t"<<SUCC<<"\t"<<FAIL<<"\n";
    }
  }
};

struct _logTypes {
  std::map<long, std::string> types;
  template <class T2> inline void configure(const T2 &) {}

  inline void process(AnyEvt::Reader &evt) {
    if (evt.which() == AnyEvt::METHODENTER) {
      auto me = evt.getMethodenter();
      //if (std::string("<init>") == me.getName().cStr()) {
        const long calleetag = me.getCalleetag();
        if (types.find(calleetag) == types.end()) {
          this->types[calleetag] = string(me.getCalleeclass().cStr());
        }
      //}
    }
  }

  inline bool isAccepting() const { return true; }

  inline bool isFrozen() const { return false; }

  const void description(std::ostream &out) const {
    out << "OBJ\tCLASS\n";
    long cnt=0;
    for (auto it=this->types.begin(); it != this->types.end(); ++it) {
      out << it->first<<"\t"<<it->second<<"\n";
    }
  }
};

//struct _logLifetime {
//  std::map<long, std::pair<long, long> > lifetime;
//  long cur = 0;
//  template <class T2> inline void configure(const T2 &) {}
//
//  inline void process(AnyEvt::Reader &evt) {
//    // FIXME: impl
//    this->cur++;
//  }
//
//  inline bool isAccepting() const { return true; }
//
//  inline bool isFrozen() const { return false; }
//
//  const std::string description() const {
//    stringstream str;
//    str << "OBJ\tFROM\tTO\n";
//    for (auto it=this->lifetimes.begin(); it != this->lifetimes.end(); ++it) {
//      long obj = it->first;
//      long from = it->second->first;
//      long to = it->second->first;
//      str<<obj<<"\t"<<from<<"\t"<<to<<"\n";
//    }
//    return str.str();
//  }
//};

template <class T> struct _perObjectStats {
  _implement<T> inner;

  template <class T2> inline void configure(const T2 &) {}

  inline void process(AnyEvt::Reader &evt) {
    this->inner.process(evt);
  }

  inline bool isAccepting() const { return this->inner.isAccepting(); }

  inline bool isFrozen() const { return this->inner.isFrozen(); }

  std::set<long> getSuccesses() const {
    std::set<long> successes;
    for (auto it=this->inner.actives.begin(); it != this->inner.actives.end(); it++) {
      const auto &tag = it->first;
      successes.insert(tag);
    }
    return successes;
  }

  std::set<long> getFailures() const {
    std::set<long> failures;
    for (auto it=this->inner.dead.begin(); it != this->inner.dead.end(); it++) {
      failures.insert(*it);
    }
    return failures;
  }

  const void description(std::ostream &out) const {
    std::set<long> successes = this->getSuccesses();
    std::set<long> failures  = this->getFailures();
    out<<"SUCC\tFAIL\n";
    out<<"[ ";
    for (auto it=successes.begin(); it != successes.end(); it++) {
      out<<*it<<" ";
    }
    out << "]\t";
    out<<"[ ";
    for (auto it=failures.begin(); it != failures.end(); it++) {
      out<<*it<<" ";
    }
    out << "]\n";

  }
};

template <class T> struct _perObjectQuery {
  _implement<T> inner;
  // std::map<long, _implement<T>&> id_types;

  std::set<long> object_ids;

  template <class T2> inline void configure(const T2 &) {}

  inline void process(AnyEvt::Reader &evt) {
    this->inner.process(evt);

    if (evt.which() == AnyEvt::METHODENTER) {
      // this->id_types.insert(std::make_pair(evt.getMethodenter().getCalleetag(), this->inner));
      object_ids.insert(evt.getMethodenter().getCalleetag());
    }
  }

  inline bool isAccepting() const { return true; }

  inline bool isFrozen() const { return false; }

  const void description(std::ostream &out) const {
    for (auto it=this->inner.actives.begin(); it != this->inner.actives.end(); ++it) {
      out << it->first << "\t" << it->second.resultAsString() <<"\n";
    }
    out << "Ignored" << std::endl;
    for (auto it=this->inner.ignored.begin(); it != this->inner.ignored.end(); ++it) {
      out << *it << std::endl;
    }
    out << "Dead" << std::endl;
    for (auto it=this->inner.dead.begin(); it != this->inner.dead.end(); ++it) {
      out << *it << std::endl;
    }
    out << "Number of unique objects in method enter:" << this->object_ids.size() << std::endl; 
    out << "Number of active protocols:" << this->inner.actives.size() << std::endl; 
  }
};

struct _checkStack {
  _implement<proto_dbg<112, proto_check> > inner;
  inline void process(AnyEvt::Reader &evt) {
    if (evt.which() == AnyEvt::METHODEXIT) {
      auto mexit = evt.getMethodexit();
      ASSERT_EQ(inner.stacks[mexit.getThreadName()].peekFrame().getMethodName(),
                mexit.getName().cStr());
    }
    inner.process(evt);
  }

  inline void description(std::ostream &out) const {
    out << "checkStack\n";

    {
      for (auto it = this->inner.stacks.begin(); it != this->inner.stacks.end(); ++it) {
        out << "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n";
        out << "thread: " << it->first << "\n";
        const auto &stack = it->second;
        for (int cnt=0; cnt<stack.hasFrames(); ++cnt) {
          auto fr = stack.peekFrame(cnt);
          out <<(cnt+1)<<" left: "<<fr.getClassName()<<"@"<<fr.getThisId()<<"::"<<fr.getMethodName()<<"\n";
        }
        out << "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n";
      }
    }
    out << "\n";
    inner.description(out);
  }
};

template <class Inner, class Label> class _labeled {
  Label label;
  std::function<std::string(const Label &)> toString;
  Inner inner;

public:
  _labeled()
      : toString([](const Label &) { return "not configured"; }) {}

  _labeled(Inner &&inner, std::function<std::string(Label)> toString)
      : label(std::move(label)), toString(std::move(toString)),
        inner(std::move(inner)) {}

  //    _labeled(_labeled<Inner, Label> &&other, Label label)
  //      : label(label),
  //        inner(std::move(other.inner), label),
  //        toString(other.toString)
  //    { }

  inline void configure(const Label &label) {
    // std::cout << "labelling: " << this->toString(label) << std::endl;
    this->label = label;
    inner.configure(label);
  }

  template <class T> inline void configure(const T &config) {
    inner.configure(config);
  }

  inline void process(AnyEvt::Reader &evt) { inner.process(evt); }

  inline bool isAccepting() const { return inner.isAccepting(); }

  inline bool isFrozen() const { return inner.isFrozen(); }

  inline const void description(std::ostream &out) const {
    out << this->toString(this->label) << ": ";
    inner.description(out);
  }

  //~_labeled() { std::cout << this->description() << "\n"; }
};

// typedef std::function<UQuery(long)> QueryFactory;

template <class Inner> class _counted {
  unsigned long ctr = 0;
  Inner inner;

public:
  _counted() : inner() {}
  _counted(Inner inner) : inner(std::move(inner)) {}

  //    template<class T>
  //    _counted(const _counted &other, T config) : inner(std::move(inner),
  // config) { }

  template <class T> inline void configure(const T &config) {
    inner.configure(config);
  }

  inline void process(AnyEvt::Reader &evt) {
    this->ctr++;
    inner.process(evt);
    if (this->ctr % 100000 == 0) {
      std::cout << "processed event " << (this->ctr / 1000000.0) << "E+6"
                << std::endl;
    }
  }

  inline bool isAccepting() const { return inner.isAccepting(); }

  bool isFrozen() const { return inner.isFrozen(); }

  inline const void description(std::ostream &out) const {
    out << "Counted(" << this->ctr << " events, ";
    this->inner.description(out);
    out << ")";
  }
};

template <class Inner> class _onePerObject {
private:
  std::map<long, Inner> queries;
  std::map<long, Inner> frozen_queries;
  const std::string desc;
  const Inner prototype;
  unsigned long acceptingWhenFreed = 0;
  unsigned long failingWhenFreed = 0;

public:
  // the factory needs to be side effect free at least when the input
  // is the value -1
  _onePerObject(Inner prototype) {
    stringstream ss;
    prototype.description(ss);
    this->desc = ss.str();
    this->prototype = prototype;
  }

  // if the factory is not side effect free, you can supply a name
  // explicitly
  _onePerObject(Inner prototype, const std::string desc)
      : prototype(prototype), desc(desc) {}

  template <class T> inline void configure(const T &config) {
    for (const auto &kv : queries) {
      kv->second.configure(config);
    }
  }

  inline void process(AnyEvt::Reader &evt) {
    if (evt.which() == AnyEvt::METHODENTER &&
        std::string(evt.getMethodenter().getName()) == "<init>") {
      const long id = evt.getMethodenter().getCalleetag();
      if (this->queries.find(id) == this->queries.end()) {
        Inner newInner(this->prototype);
        newInner.configure(id);
        this->queries.insert({ { id, std::move(newInner) } });
      }
    }

    if (evt.which() == AnyEvt::OBJFREE) {
      auto it = this->queries.find(evt.getObjfree().getTag());
      if (it != this->queries.end()) {
        const auto &objquerykv = *it;
        if (objquerykv.second.isAccepting()) {
          this->acceptingWhenFreed++;
        } else {
          this->failingWhenFreed++;
        }

        DBG("maintaining now: " << this->queries.size() << " active qs, "
                                << this->frozen_queries.size() << " frozen qs ("
                                << (this->acceptingWhenFreed +
                                    this->failingWhenFreed) << " freed qs");

        const size_t num_erased =
            this->queries.erase(evt.getObjfree().getTag());
        if (num_erased > 1) {
          ERR("BUG: object had " << num_erased << " queries");
        }
      }
    }

    auto valIsFrozen = [](auto p) { return std::get<1>(p).isFrozen(); };
    std::copy_if(
        this->queries.begin(), this->queries.end(),
        std::inserter(this->frozen_queries, this->frozen_queries.begin()),
        valIsFrozen);

    for (auto it = this->queries.begin(); it != this->queries.end();) {
      if (it->second.isFrozen()) {
        it = this->queries.erase(it);
      } else {
        ++it;
      }
    }

    for (auto i = queries.begin(); i != queries.end(); ++i) {
      std::get<1>(*i).process(evt);
      if (std::get<1>(*i).isFrozen()) {
        this->frozen_queries.insert({ { std::get<0>(*i), std::get<1>(*i) } });
        this->queries.erase(std::get<0>(*i));
      }
    }
  }

  inline bool isAccepting() const { return true; }

  inline bool isFrozen() const { return this->prototype.isFrozen(); }

  inline const void description(std::ostream &out) const {
    const auto totalQueries = this->queries.size() +
                              this->frozen_queries.size() +
                              this->acceptingWhenFreed + this->failingWhenFreed;
    auto isAccepting = [](const auto &q) { return q.second.isAccepting(); };
    const auto acceptingQueries =
        std::count_if(this->queries.begin(), this->queries.end(), isAccepting) +
        std::count_if(this->frozen_queries.begin(), this->frozen_queries.end(),
                      isAccepting) +
        this->acceptingWhenFreed;
    if (totalQueries > 0) {
      out << "destroying OnePerObject" << std::endl;
      out << "SUMMARY\n"
         << " ran " << this->desc << " for " << totalQueries << " objects \n"
         << " query succeeded for " << (acceptingQueries * 100.0 / totalQueries)
         << "% of objects (" << acceptingQueries << ")\n\n";
      //        for (const auto& kv : this->queries) {
      //          std:ss << kv.second.description() << "\n";
      //        }
      out << std::endl;
    }
  }
};

using Trace = std::queue<const std::string>;

_succeed succeed() { return _succeed(); }

template <class Inner> auto onePerObject(Inner proto) {
  return _onePerObject<Inner>(std::move(_onePerObject<Inner>(proto)));
}

// the int state is used for the current count, the long config is the
// object
// id
// we may focus on:
template <class Inner> auto maxInDegree(int max, Inner &&inner) {
  std::function<bool(int &, const long &, AnyEvt::Reader &)> fun = [max](
      int &acc, const long &objid, AnyEvt::Reader &evt) {
    switch (evt.which()) {
    case AnyEvt::FIELDSTORE:
      if (evt.getFieldstore().getNewval() == objid) {
        acc++;
      }
      if (evt.getFieldstore().getOldval() == objid) {
        acc--;
      }
    case AnyEvt::METHODENTER:
    case AnyEvt::OBJALLOC:
    case AnyEvt::OBJFREE:
    case AnyEvt::FIELDLOAD:
    case AnyEvt::VARSTORE:
    case AnyEvt::VARLOAD:
    case AnyEvt::READMODIFY:
    case AnyEvt::METHODEXIT:
      break;
    }
    return (acc <= max);
  };
  stringstream concat;
  concat << "limitInDegree to " << max;
  return _observe<int, long, Inner>(fun, concat.str(), inner, 0, -1);
}

template <class Inner>
auto traced(Inner &&inner, std::ostream &out = std::cout) {
  std::function<bool(Queries::Trace &, const long &, AnyEvt::Reader &)> func =
      [&out](Trace &, const long &objid, AnyEvt::Reader &evt) {
    //        capnp::prettyPrint(evt);
    if (!_ignoreBackgroundThreads<proto_any>::isBackgroundThreadEvent(evt)) {
      out << evt.toString().flatten().cStr() << "\n";
    }

    return true;
  };
  return _observe<Trace, long, Inner>(func, "", inner, Trace(), (long)-1);
}

//  template<class Inner>
//  auto filter(std::function<bool(const AnyEvt::Reader&)> fun,
//                       const std::string desc,
//         Inner &&inner) {
//    return _filter<Inner>(fun, desc, std::move(inner));
//  }

//  template<class Inner>
//  auto filterCallee(Inner &&inner) {
//    return _filterCallee<Inner>(inner);
//  }

//template <class T> auto describeWhenDone(T &&inner) {
//  return _describeWhenDone<T>(std::move(inner));
//}

template <class T> auto debug(T &&inner) { return _debug<T>(std::move(inner)); }

template <class Inner> auto forever(Inner &&inner) {
  return _forever<Inner>(inner);
}

template <class Inner> auto counted(Inner &&inner) {
  return _counted<Inner>(inner);
}

template <class A, class B> _two<A, B> two(A &&a, B &&b) {
  return _two<A, B>(std::move(a), std::move(b));
}

template <class Inner, class T = AnyEvt>
auto labeled(std::function<std::string(const T &)> toString, Inner &&inner) {
  return _labeled<Inner, T>(std::move(inner), toString);
}

template <class Inner, class T>
auto partition(std::function<T(AnyEvt::Reader &)> func, Inner &&prototype) {
  using State = std::map<T, Inner>;
  using Config = const Inner;
  std::function<bool(State &, const Config &, AnyEvt::Reader &)> obsfunc =
      [func](State &inners, const Config &prototype, AnyEvt::Reader &evt) {
    const auto &key = func(evt);
    auto it = inners.find(key);
    if (it != inners.end()) {
      it->second.process(evt);
    } else {
      auto newInner = Inner(prototype);
      newInner.configure(key);
      newInner.process(evt);
      inners.insert({ { key, std::move(newInner) } });
    }
    return false;
  };
  return _observe<State, Config, Inner>(obsfunc, std::string("partition"),
                                        std::move(prototype), State(),
                                        std::move(prototype));
}

template <class Inner> auto replayTrace(Inner &&inner) {}

/*
struct _logDominators {
  long cnt = 0;
  _implement<proto_objectGraph> objectGraph;

  template <class T> inline void configure(const T &) {
  }

  inline void process(AnyEvt::Reader &evt) {
    objectGraph.process(evt);
    //stringstream filename;
    //filename << "./file_"<<((cnt<10) ? "0" : "") <<this->cnt<<".dot";
    //std::cout << "cnt="<<cnt<<"\n";
    //this->cnt++;
    //proto_objectGraph::objectGraph.dumpDot(filename.str());
  }

  const void description(std::ostream &out) const {
    typedef long Vertex;
    typedef refgraph<long>::Graph G;
    typedef property_map<G, vertex_index_t>::type IndexMap;
    typedef
      iterator_property_map<vector<Vertex>::iterator, IndexMap>
      PredMap;

    out << "OBJ\tDOMINATORS\n";
    auto &g = proto_objectGraph::objectGraph.g.graph();
    std::vector<long> domTreePredVector =
      vector<long>(num_vertices(g), graph_traits<G>::null_vertex());
    IndexMap indexMap(get(vertex_index, g));
    //vertex(0, g).foo();
    PredMap domTreePredMap =
      make_iterator_property_map(domTreePredVector.begin(), indexMap);
    const auto &startingNode = proto_objectGraph::objectGraph.g[NativeInterface_SPECIAL_VAL_JVM];
    DODBG("starting node is "<<startingNode<<" (#"<<startingNode.key<<")");
    proto_objectGraph::objectGraph.dumpDot("before.dot");
    lengauer_tarjan_dominator_tree(g, startingNode.key, domTreePredMap);

    vector<int> idom(num_vertices(g));
    vector<long> kdom(num_vertices(g));

    graph_traits<G>::vertex_iterator uItr, uEnd;
    for (tie(uItr, uEnd) = vertices(g); uItr != uEnd; ++uItr) {
      if (get(domTreePredMap, *uItr) != graph_traits<G>::null_vertex()) {
        int mapIdx = get(indexMap, *uItr);
        DODBG(*uItr<<"-->"<<mapIdx);
        auto dominated = g[get(indexMap,*uItr)];
        auto dominator = g[get(domTreePredMap, *uItr)];
        DODBG("node "<<dominated<<" dominated by "<<dominator);
        idom[get(indexMap, *uItr)] =
          get(indexMap, get(domTreePredMap, *uItr));
//        idom[get(indexMap, *uItr)] =
//          get(indexMap, get(domTreePredMap, *uItr));

      } else {
        idom[get(indexMap, *uItr)] = -1;
      }
    }

    copy(idom.begin(), idom.end(), ostream_iterator<int>(out, " "));
    out << endl;

    //for (auto it = this->dominators.begin(); it != this->dominators.end(); ++it) {
    //  sstr <<it->first<<"\t";
    //  //const auto &doms = it->second;
    //  //if (doms.size() > 0) {
    //  //  sstr<<doms();
    //  //} else {
    //  //  sstr<<"WORLD";
    //  //}
    //  sstr << it->second;
    //  sstr<<"\n";
    //}
  }
};
*/


} // NAMESPACE QUERIES


#endif // ifndef QUERIES_H
