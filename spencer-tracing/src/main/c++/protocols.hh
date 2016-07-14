#ifndef PROTOCOLS_HH
#define PROTOCOLS_HH

#include <sstream>
#include <unordered_map>
#include <iostream>
//#include "refgraph.hh"
#include <algorithm>
#include <vector>
#include "Debug.h"

using std::cout;
using std::endl;
using std::string;
using std::stringstream;
using std::unordered_map;
using std::vector;

/*
* Protocols are classes that define handler methods:
*
*  static string description();
*  inline bool allocate(long, const char* cname);
*
*  inline void deallocate();
*
*  inline bool newRefFromField(bool isDropped,
*                              long holder,
*                              const char *cname,
*                              const char *fname,
*                              const char *thread);
*
*  inline bool loadFromField(long holder,
*                            const char *cname,
*                            const char *fname,
*                            const char *thread);
*
*  inline bool newRefFromVar(bool isDropped,
*                            long holder,
*                            int var,
*                            const char *cname,
*                            const char *mname,
§*                            const char *thread);
*
*  inline bool loadFromVar(long holder,
*                          const char *cname,
*                          const char *mname,
*                          int var,
*                          const char *thread);
*
*  inline bool useFrom(bool isModify,
*                      long obj,
*                      const char *cname,
*                      const char *mname,
*                      const char *fname,
*                      const char *thread)
*
*  static string classSummary();
*/

template <long TAG, typename Inner> struct proto_dbg {
  long tag;
  Inner inner;
  static string description() {
    stringstream ss;
    ss << TAG;
    return "dbg<"+ss.str()+","+Inner::description()+">";
  }
  inline bool allocate(long _tag, const char* cname, const char *thread) {
    this->tag = _tag;
    if (tag == TAG) {
      std::cout << "obj #"<<TAG<<": "<<"allocate("<<tag<<", "<<cname<<", "<<thread<<")\n";
    }
    return yellIfFalse(inner.allocate(_tag, cname, thread));
  }

  inline void deallocate() {
    if (this->tag == TAG) {
      std::cout << "obj #"<<TAG<<": deallocate()\n";
    }
    inner.deallocate();
  }

  inline bool newRefFromField(bool isDropped,
                              long holder,
                              const char *cname,
                              const char *fname,
                              const char *thread) {
    if (this->tag == TAG) {
      std::cout << "obj #"<<TAG
        <<": newRefFromField("<<isDropped
        <<", holder="<<std::string(cname)<<"@"<<holder<<"."<<std::string(fname)<<")\n";
    }
    return yellIfFalse(inner.newRefFromField(
      isDropped,
      holder,
      cname,
      fname,
      thread));
  }

  inline bool loadFromField(long holder,
                            const char *cname,
                            const char *fname,
                            const char *thread) {
    if (this->tag == TAG) {
      std::cout << "obj #"<<TAG<<": loadFromField("
      <<"holder="<<cname<<"@"<<holder<<"."<<fname<<", thd="<<thread<<")\n";
    }
    return yellIfFalse(inner.loadFromField(holder,cname,fname,thread));
  }

  inline bool newRefFromVar(bool isDropped,
                            long holder,
                            int var,
                            const char *cname,
                            const char *mname,
                            const char *thread) {
    if (this->tag == TAG) {
      std::cout << "obj #"<<TAG<<": newRefFromVar("
         <<"isDropped="<<(isDropped?"yes":"no")
         <<", holder="<<cname<<"@"<<holder
         <<", var="<<var
         <<", caller mname="<<mname
         <<", thd="<<thread<<")\n";
    }
    return yellIfFalse(inner.newRefFromVar(
      isDropped,
      holder,
      var,
      cname,
      mname,
      thread));
  }

  inline bool loadFromVar(long holder,
                          const char *cname,
                          const char *mname,
                          int var,
                          const char *thread) {
    if (this->tag == TAG) {
      std::cout << "obj #"<<TAG<<": loadFromVar("
        <<"holder="<<cname<<"@"<<holder
        <<", caller mname="<<mname
        <<", var="<<var<<")\n";
    }
    return yellIfFalse(inner.loadFromVar(
      holder,
      cname,
      mname,
      var,
      thread));
  }

  inline bool useFrom(bool isModify,
                      long obj,
                      const char *cname,
                      const char *mname,
                      const char *fname,
                      const char *thread)  {
    if (this->tag == TAG) {
      std::cout << "obj #"<<TAG<<": useFrom("
      <<(isModify?"modify":"read")<<", "
      <<"caller="<<cname<<"@"<<obj<<","
      <<"called method="<<(mname?mname:"NULL")<<", "
      <<"used field="<<(fname?fname:"NULL")<<")\n";
    }
    return yellIfFalse(inner.useFrom(
      isModify,
      obj,
      cname,
      mname,
      fname,
      thread));
  }

  static string classSummary() {
    return "";
  }

private:
  bool yellIfFalse(bool in) {
    if (this->tag == TAG) {
      if (!in) {
        std::cout << "obj #"<<TAG<<": FAILURE" << std::endl;
      }
    }
    return in;
  }
};

struct proto_any {
  static string description() {
    return "any";
  }

  inline bool allocate(long id, const char* cname, const char *thread) {
    return true;
  }

  inline void deallocate() {}

  inline bool newRefFromField(bool isDropped,
                              long holder,
                              const char *cname,
                              const char *fname,
                              const char *thread) {
    return true;
  }

  inline bool loadFromField(long holder,
                            const char *cname,
                            const char *fname,
                            const char *thread) {
    return true;
  }

  inline bool newRefFromVar(bool isDropped,
                            long holder,
                            int var,
                            const char *cname,
                            const char *mname,
                            const char *thread) {
    return true;
  }

  inline bool loadFromVar(long holder,
                          const char *cname,
                          const char *mname,
                          int var,
                          const char *thread) {
    return true;
  }

  inline bool useFrom(bool isModify,
                      long obj,
                      const char *cname,
                      const char *mname,
                      const char *fname,
                      const char *thread) {
    return true;
  }

  static string classSummary() {
    return "";
  }
};

struct proto_immutable {
  bool constructed;
  long id;

  static string description() {
    return "immutable";
  }

  inline bool allocate(long id, const char* cname, const char *thread) {
    this->id = id;
    this->constructed = false;
    return true;
  }

  inline void deallocate() {
  }

  inline bool newRefFromField(bool isDropped,
                              long holder,
                              const char *cname,
                              const char *fname,
                              const char *thread) {
    this->constructed = true;
    return true;
  }

  inline bool loadFromField(long holder,
                            const char *cname,
                            const char *fname,
                            const char *thread) {
    this->constructed = true;
    return true;
  }

  inline bool newRefFromVar(bool isDropped,
                            long holder,
                            int var,
                            const char *cname,
                            const char *mname,
                            const char *thread) {
    if (var != -1) {
      this->constructed = true;
    }
    return true;
  }

  inline bool loadFromVar(long holder,
                          const char *cname,
                          const char *mname,
                          int var,
                          const char *thread) {
    this->constructed = true;
    return true;
  }

  inline bool useFrom(bool isModify,
                      long obj,
                      const char *cname,
                      const char *mname,
                      const char *fname,
                      const char *thread) {
    if (this->constructed) {
      return !isModify;
    } else {
      return true;
    }
  }

  static string classSummary() {
    return "";
  }
};

struct proto_thread_local {
  std::string thread;
  static string description() {
    return "thread_local";
  }
  inline bool allocate(long id, const char* cname, const char *thread) {
    this->thread = thread;
    return true;
  }

  inline void deallocate() {
  }

  inline bool newRefFromField(bool isDropped,
                              long holder,
                              const char *cname,
                              const char *fname,
                              const char *thread) {
    return this->thread == thread;
  }

  inline bool loadFromField(long holder,
                            const char *cname,
                            const char *fname,
                            const char *thread) {
    return this->thread == thread;
  }

  inline bool newRefFromVar(bool isDropped,
                            long holder,
                            int var,
                            const char *cname,
                            const char *mname,
                            const char *thread) {
    return this->thread == thread;
  }

  inline bool loadFromVar(long holder,
                          const char *cname,
                          const char *mname,
                          int var,
                          const char *thread) {
    return this->thread == thread;
  }

  inline bool useFrom(bool isModify,
                      long obj,
                      const char *cname,
                      const char *mname,
                      const char *fname,
                      const char *thread) {
    return this->thread == thread;
  }

  static string classSummary() {
    return "";
  }
};

struct proto_stationaryObjects {
  long id;
  static string description() {
    return "stationaryObjects";
  }

  inline bool allocate(long id, const char* cname, const char *threadName) {
    this->id= id;
    return true;
  }

  inline void deallocate() { }

  inline bool newRefFromField(bool isDropped,
                              long holder,
                              const char *cname,
                              const char *fname,
                              const char *thread) const {
    return true;
  }

  inline bool loadFromField(long holder,
                            const char *cname,
                            const char *fname,
                            const char *thread) const {
    return true;
  }

  inline bool newRefFromVar(bool isDropped,
                            long holder,
                            int var,
                            const char *cname,
                            const char *mname,
                            const char *thread) const {
    return true;
  }

  inline bool loadFromVar(long holder,
                          const char *cname,
                          const char *mname,
                          int var,
                          const char *thread) const {
    return true;
  }

  inline bool useFrom(bool isModify,
                      long obj,
                      const char *cname,
                      const char *mname,
                      const char *fname,
                      const char *thread)  {
    if (fname == NULL) {
      // it's a method call!
      return true;
    } else {
      if (isModify) {
        return this->isWritable(fname);
      } else {
        this->makeReadOnly(fname);
        return true;
      }
    }
  }

  static string classSummary() {
    return "";
  }

private:
  //std::set<std::string> readonlyFields;
  bool isReadOnly = false;

  inline bool isWritable(const std::string &fname) const {
    // it's writable if it's not yet readonly!
    //auto it = std::find(this->readonlyFields.begin(), this->readonlyFields.end(), fname);
    //return (it == this->readonlyFields.end());
    return !this->isReadOnly;
  }

  inline void makeReadOnly(const std::string &fname) {
    //this->readonlyFields.insert(fname);
    this->isReadOnly = true;
    // std::cout<<"obj #"<<this->id<<": making readonly\n";
    assert(!this->isWritable(fname));
  }
};



struct proto_unique {
  struct ref {
    bool isVar;
    long holder = -1;
    string name; // field / method
    int var;     // only for var
    string cname;
  };

  ref newest;
  string cname = "???";
  //  int newestHolder = -1;
  //  string newestFname = "???";
  //  string newestCname = "???";
  //  static std::unordered_map<string, long> perClassFailures;
  //  static std::unordered_map<string, long> perClassQueries;

  static string description() { return "unique"; }

  inline bool allocate(long, const char *cname) {
    this->cname = string(cname);
    // proto_unique::perClassQueries[this->cname]++;
    return true;
  }

  inline void deallocate() {}

  inline bool newRefFromField(bool isDropped,
                              long holder,
                              const char *cname,
                              const char *fname,
                              const char *thread) {
    assert(holder >= 0);
    if (!isDropped) {
      this->newest.isVar = false;
      this->newest.holder = holder;
      this->newest.name = fname;
      this->newest.var = -1;
      this->newest.cname = cname;
    }
    return true;
  }

  inline bool loadFromField(long holder, const char *cname, const char *fname) {
    return (!this->newest.isVar && this->newest.holder == holder &&
            this->newest.name == fname && this->newest.cname == cname);
  }

  inline bool newRefFromVar(bool isDropped, long holder, int var,
                            const char *cname, const char *mname,
                            const char *thread) {
    assert(holder >= 0);
    if (!isDropped) {
      this->newest.isVar = true;
      this->newest.holder = holder;
      this->newest.name = mname;
      this->newest.var = var;
      this->newest.cname = cname;
    }
    return true;
  }

  inline bool loadFromVar(long holder, const char *cname, const char *mname,
                          int var,
                          const char *thread) {
    return (this->newest.isVar && this->newest.holder == holder &&
            this->newest.name == mname && this->newest.var == var);
  }

  inline bool useFrom(bool isModify, long obj, const char *cname,
                      const char *mname, const char *fname) {
    return true;

    //    if(obj < 0) {
    //      std::cerr << "warning: object id " << obj << " invalid\n";
    //    }
    //    ref newRef;
    //    newRef.isVar = ! isHeap;
    //    newRef.holder = obj;
    //    newRef.name = newRef.isVar ? mname : fname;
    //    newRef.var = var;
    //    newRef.cname = cname;
    //    if (this->newest.holder == -1 ||
    //        (! this->newest.isVar &&
    //         this->newest.holder == obj && fname && this->newest.name ==
    // fname) ||
    //        (this->newest.isVar   &&
    //         this->newest.holder == obj && mname && this->newest.name == mname
    // && this->newest.var == var)) {
    //      return true;
    //    }
    //    return false;
  }

  static string classSummary() {
    // stringstream sstream;
    // sstream << "SUCC; TOTL; CLASS; PERC\n";
    // for (const auto &kv : proto_unique::perClassQueries) {
    //  const string &CNAME = kv.first;
    //  const long TOTL = kv.second;
    //  const long SUCC = TOTL - proto_unique::perClassFailures[CNAME];
    //  const double PERC = SUCC * 100.0 / TOTL;
    //  sstream << SUCC << "; " << TOTL << "; " << CNAME << "; " << PERC <<
    // "\n";
    //}
    //
    // return sstream.str();
    return "";
  }
};

// requires that only the newest reference is used for accesses
struct proto_heapMoves {
  int newestHolder = -1;
  string newestFname = "???";
  string newestCname = "???";
  string cname = "???";
  static std::unordered_map<string, long> perClassFailures;
  static std::unordered_map<string, long> perClassQueries;

  void fail() const { proto_heapMoves::perClassFailures[this->cname]++; }

  static string description() { return "heapMoves"; }

  inline bool allocate(long, const char *cname,
  const char *thread) {
    this->cname = string(cname);
    proto_heapMoves::perClassQueries[this->cname]++;
    return true;
  }

  inline void deallocate() {}

  inline bool newRefFromField(bool isDropped, long holder, const char *cname,
                              const char *fname,
                              const char *thread) {
    if (!isDropped) {
      this->newestHolder = holder;
      this->newestFname = fname;
      this->newestCname = cname;
    }
    return true;
  }

  inline bool loadFromField(long holder, const char *cname, const char *fname,
  const char *thread) {
    if (newestHolder == holder && newestFname == fname) {
      return true;
    } else {
      if (newestHolder == -1) {
        WARN("warning: missed the original assignment (pbly due to "
                     "skipping ctors)");
        return true;
      }
      // std::cout << "this->cname = " << this->cname << "\n";
      // std::cout << holder << " vs " << newestHolder << "\n";
      // std::cout << fname << " vs " << newestFname << "\n";
      fail();
      return false;
    }
  }

  inline bool newRefFromVar(bool isDropped, long holder, int var,
                            const char *cname, const char *mname,
                            const char *thread) {
    return true;
  }

  inline bool loadFromVar(long holder, const char *cname, const char *mname,
                          int var,
                          const char *thread) {
    return true;
  }

  inline bool useFrom(bool isModify, long obj, const char *cname,
                      const char *mname, const char *fname,
                      const char *thread) {
    return true;
  }

  static string classSummary() {
    //stringstream sstream;
    //sstream << "SUCC\tTOTL\tCLASS\tPERC\n";
    //for (const auto &kv : proto_heapMoves::perClassQueries) {
    //  const string &CNAME = kv.first;
    //  const long TOTL = kv.second;
    //  const long SUCC = TOTL - proto_heapMoves::perClassFailures[CNAME];
    //  const double PERC = SUCC * 100.0 / TOTL;
    //  sstream << SUCC << "\t" << TOTL << "\t" << CNAME << "\t" << PERC << "\n";
    //}
    //return ss.str();

    return "";
  }

  string resultAsString() const {
    return "";
  }

};

struct proto_moves {
  int newestHolder = -1;
  bool newestIsVar = false;
  string newestName = "???";
  string newestCname = "???";
  string cname = "???";

  static string description() { return "moves"; }

  inline bool allocate(long, const char *cname,
  const char *thread) {
    this->cname = string(cname);
    return true;
  }

  inline void deallocate() {}

  inline bool newRefFromField(bool isDropped, long holder, const char *cname,
                              const char *fname,
                              const char *thread) {
    if (!isDropped) {
      this->newestHolder = holder;
      this->newestName = fname;
      this->newestCname = cname;
      this->newestIsVar = false;
    }
    return true;
  }

  inline bool loadFromField(long holder, const char *cname, const char *fname,
  const char *thread) {
    if (newestHolder == holder && newestName == fname && !newestIsVar) {
      return true;
    } else {
      if (newestHolder == -1) {
        return true;
      } else {
        return false;
      }
    }
  }

  inline bool newRefFromVar(bool isDropped, long holder, int var,
                            const char *cname, const char *mname,
                            const char *thread) {
  if (!isDropped) {
    stringstream varName;
    varName <<cname<<"::"<<var;
    this->newestHolder = holder;
    this->newestName = varName.str();
    this->newestCname = cname;
    this->newestIsVar = true;
  }
  return true;
  }

  inline bool loadFromVar(long holder, const char *cname, const char *mname,
                          int var,
                          const char *thread) {
  if (newestHolder == holder && !newestIsVar) {
    stringstream varName;
    varName <<cname<<"::"<<var;
    return newestName == varName.str();
  } else {
    if (newestHolder == -1) {
      return true;
    } else {
      return false;
    }
  }
}

  inline bool useFrom(bool isModify, long obj, const char *cname,
                      const char *mname, const char *fname,
                      const char *thread) {
    return true;
  }

  static string classSummary() {
    //stringstream sstream;
    //sstream << "SUCC\tTOTL\tCLASS\tPERC\n";
    //for (const auto &kv : proto_heapMoves::perClassQueries) {
    //  const string &CNAME = kv.first;
    //  const long TOTL = kv.second;
    //  const long SUCC = TOTL - proto_heapMoves::perClassFailures[CNAME];
    //  const double PERC = SUCC * 100.0 / TOTL;
    //  sstream << SUCC << "\t" << TOTL << "\t" << CNAME << "\t" << PERC << "\n";
    //}
    //return ss.str();

    return "";
  }
};

//#define BLA(msg) {if (this->obj == 17) { cout << "this = " << this << ", obj =
//" << this->obj << "\t" << msg << endl; }}

/**
 * A class that makes sure that the callbacks are called in the right
 * order. The right order is:

 * allocate (refFromField | refFromVar | useFrom)* deallocate destructor
 */
 struct proto_check {
   long obj;
   string cname;
   enum {
     DEFAULT,
     MOVE,
     EXPLICIT,
     MOVEEQ
   } constr;
   const string constrnames = "DME=";

   static string description() { return "check"; }

   enum {
     UNINIT,
     LIVE,
     DEAD
   } stage = UNINIT;
   const string names = "ULD";

   proto_check() { constr = DEFAULT; }

   proto_check(string cname, long obj) : obj(obj), cname(cname) {
     constr = EXPLICIT;
   }

   proto_check(proto_check &&other) {
     std::swap(obj, other.obj);
     std::swap(cname, other.cname);
     std::swap(stage, other.stage);
     constr = MOVE;

     other.stage = DEAD;
   }

   proto_check &operator=(proto_check &&other) {
     this->obj = other.obj;
     this->cname = std::move(other.cname);
     this->stage = other.stage;
     other.stage = DEAD;
     this->constr = MOVEEQ;
     return *this;
   }

   proto_check(const proto_check &other) {
     // no copy!
     //    throw "up";
   }

   inline bool allocate(long obj, const char *cname,
                        const char *thread) {
     this->obj = obj;
     // BLA("allocate");
     this->cname = cname;
     bool ret = (this->stage == UNINIT);
     this->stage = LIVE;
     return ret;
   }

   inline void deallocate() {
     if (!(stage == LIVE || this->obj == 0)) {
       cout << "stage(obj=" << this->obj << ") != LIVE, is "
            << (char)names[stage] << endl;
     }
     if(!(stage == LIVE || this->obj == 0)) {
       WARN("wat");
     }
     this->stage = DEAD;
   }

   inline bool newRefFromField(bool isDropped, long holder, const char *cname,
                               const char *fname,
                               const char *thread) {
     // BLA("refFromField");
     if (!isDropped) {
       this->addOwner(holder, cname);
     } else {
       this->removeOwner(holder);
     }
     return (holder >= 0) && (stage == LIVE);
   }

   inline bool loadFromField(long holder, const char *cname, const char *fname,
                             const char *thread) {
     if (! this->isOwner(holder)) {
       std::cout << "obj "<<this->cname<<"@"<<this->obj<<": dangling loadFromField from "<<cname<<"@"<<holder<<"."<<fname<<"\n";
     }
     return (holder >= 0) && (stage == LIVE) && this->isOwner(holder);
   }

   inline bool newRefFromVar(bool isDropped, long holder, int var,
                             const char *cname, const char *mname,
                             const char *thread) {
     if (!isDropped) {
       this->addOwner(holder, cname);
     } else {
       this->removeOwner(holder);
     }
     return (holder >= 0) && (stage == LIVE);
   }

   inline bool loadFromVar(long holder, const char *cname, const char *mname,
                           int var,
                           const char *thread) {
     if (! this->isOwner(holder)) {
       std::cerr<< "obj "<<this->cname<<"@"<<this->obj<<": dangling loadFromVar from "<<cname<<"@"<<holder<<"."<<mname<<"(..) :: var_"<<var<<"\n";
     }
     return (holder >= 0) && (stage == LIVE) && this->isOwner(holder);
   }

   inline bool useFrom(bool isModify, long holder, const char *cname,
                       const char *mname, const char *fname,
                       const char *thread) {
     //if (! this->isOwner(holder)) {
     //  std::cerr<< "obj "<<this->cname<<"@"<<this->obj<<": dangling useFrom from "<<cname<<"@"<<holder<<" ("<<(mname ? (string("method ")+mname) : (string("field ")+fname))<<")\n";
     //}
     return (stage == LIVE);// && this->isOwner(holder);
   }

   static string classSummary() { return ""; }

   ~proto_check() {
     // BLA("destructor");
     if (!(stage == DEAD)) {
       //cout << "class  = " << cname << endl;
       //cout << "objid  = " << obj << endl;
       //cout << "stage  = " << names[stage] << endl;
       //cout << "constr = " << constrnames[constr] << endl;
       //assert(stage == DEAD);
     }
   }

 private:
   std::vector<long> owners;
   bool reachableFromStatic = false;

   void addOwner(long tag, const char *cname) {
     if (std::string("ClassRep") == cname) {
       this->reachableFromStatic = true;
     } else {
       if (!this->isOwner(tag)) {
         this->owners.push_back(tag);
       }
     }
     assert(isOwner(tag));
   }

   void removeOwner(long tag) {
     auto it = std::find(this->owners.begin(), this->owners.end(), tag);
     if (it != this->owners.end()) {
       this->owners.erase(it);
     }
     //assert(! isOwner(tag) || this->reachableFromStatic);
   }

   bool isOwner(long tag) {
     return (this->reachableFromStatic) || (tag == this->obj) || (std::string("ClassRep") == this->cname) || (this->owners.end() != std::find(this->owners.begin(), this->owners.end(), tag));
   }
 };

/*
struct proto_classAliasingGraph {
  string cname;

  static refgraph<string> classAliasing;

  // static unordered_map<pair<string,string>, long> perClassFailures;
  // static unordered_map<string, long> perClassQueries;

  static string description() { return "classAliasing"; }

  inline bool allocate(long, const char *cname,
                       const char *thread) {
    this->cname = string(cname);
    return true;
  }

  inline void deallocate() {}

  inline bool newRefFromField(bool isDropped, long holder, const char *cname,
                              const char *fname,
                              const char *thread) {
    newRefFromX("field", isDropped, holder, cname, fname);
    return true;
  }

  inline bool loadFromField(long holder, const char *cname, const char *fname, const char *thread) {
    return true;
  }

  inline bool newRefFromVar(bool isDropped, long holder, int var,
                            const char *cname, const char *mname,
                            const char *thread) {
    newRefFromX("var", isDropped, holder, cname, mname);
    return true;
  }

  inline bool loadFromVar(long holder, const char *cname, const char *mname,
                          int var, const char *thread) {
    return true;
  }

  inline bool useFrom(bool isModify, long obj, const char *cname,
                      const char *mname, const char *fname,
                      const char *thread) {
    return true;
  }

  static string classSummary() {
    // stringstream sstream;
    classAliasing.dumpDot("classAliasing.dot");
    system("dot -Tpng classAliasing.dot > classAliasing.png");
    return "produced file 'classAliasing.dot' and 'classAliasing.png'";
  }

private:
  inline void newRefFromX(const string &X, bool isDropped, long holder,
                          const char *cname, const char *fname) {
    if (isDropped) {
      classAliasing.dropOneEdge(cname, X, this->cname);
    } else {
      classAliasing.addOneEdge(cname, X, this->cname);
    }
  }
};
*/

/*
struct proto_objectGraph {
  string cname;
  long oid;

  static refgraph<long> objectGraph;
  using Graph = refgraph<long>::Graph;

  static string description() { return "objectGraph"; }

  inline bool allocate(long _id, const char* _cname, const char *thread) {
    this->oid = _id;
    this->cname = _cname;
    this->objectGraph.addNode(this->oid, this->cname);
    return true;
  }

  inline void deallocate() {}

  inline bool newRefFromField(bool isDropped, long holder,
                              const char *holdercname, const char *fname,
                              const char *thread) {
    newRefFromX(isDropped, holder, holdercname, fname);
    return true;
  }

  inline bool loadFromField(long holder, const char *cname, const char *fname,
                            const char *thread) {
    return true;
  }

  inline bool newRefFromVar(bool isDropped, long holder, int var,
                            const char *holdercname, const char *mname,
                            const char *thread) {
    newRefFromX(isDropped, holder, holdercname, mname);
    return true;
  }

  inline bool loadFromVar(long holder, const char *cname, const char *mname,
                          int var,
                          const char *thread) {
    return true;
  }

  inline bool useFrom(bool isModify, long user, const char *cname,
                      const char *mname, const char *fname,
                      const char *thread) {
    //newRefFromX(false, user, cname, fname);
    // assert(!((string(cname) == "test/FieldStore") &&
    //         (this->cname == string("sun/misc/PerfCounter"))));
    return true;
  }

  static string classSummary() {
    // stringstream sstream;
    objectGraph.dumpDot("objectGraph.dot");
    system("dot -Tpng objectGraph.dot > objectGraph.png");
    return "produced file 'objectGraph.dot' and 'objectGraph.png'";
  }

private:
  inline void newRefFromX(bool isDropped, long holder,
                          const char *holdercname, const char *fname) {
    if (isDropped) {
      objectGraph.dropOneEdge(holder, "", this->oid);
    } else {
      objectGraph.addOneEdge(holder, "", this->oid);
    }
  }
};
*/

/*template <int MAX_DEG = 1> struct proto_heapunique {
  int in_degree = 0;
  string classname = "???";

  static string description() {
    stringstream sstream;
    sstream << "heapunique<" << MAX_DEG << ">";
    return sstream.str();
  }

  inline bool allocate(long, const char *cname) {
    this->classname = string(cname);
    return true;
  }

  inline bool refFromField(bool isDropped, long holder, const char *cname,
                           const char *fname,
                           const char *thread) {
    if (isDropped) {
      this->in_degree--;
    } else {
      this->in_degree++;
    }
    if (this->in_degree <= MAX_DEG) {
      return true;
    } else {
      // cout << "failed for " << this->classname << "\n";
      return false;
    }
  }

  inline bool refFromVar(bool isDropped, long holder, int var,
                         const char *cname, const char *mname,
                         const char *thread) {
    return true;
  }

  inline bool useFrom(bool isModify, long obj,
                      const char *cname, const char *mname, const char *fname,
                      const char *thread) {
    return true;
  }

  static string classSummary() { return ""; }
};*/

#endif // PROTOCOLS_HH
