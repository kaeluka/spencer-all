#ifndef CALLSTACK_HH
#define CALLSTACK_HH

#include <string>
#include <vector>
#include <assert.h>
#include <iostream>
#include "NativeInterface.h"

class frame {
  const std::string mname = "<no more frames>";
  const std::string cname = "<no more frames>";
  const long thisid = -1;

public:
  frame(std::string _mname, std::string _cname, long _thisid)
      : mname(_mname), cname(_cname), thisid(_thisid) {}

  long getThisId() const { return this->thisid; }

  const std::string &getMethodName() const { return this->mname; }

  const std::string &getClassName() const { return this->cname; }
};

class callstack {
  std::vector<frame> frames;

public:
  callstack() {
    this->pushFrame("jvmInternals", "JvmInternals",
                    NativeInterface_SPECIAL_VAL_JVM);
  }

  inline void pushFrame(std::string mname, std::string cname, long oid) {
    this->frames.push_back(frame(mname, cname, oid));
    // std::cout << "stack is now:\n";
    // for (const frame &f : this->frames) {
    //  std::cout << f.getClassName() << "@" << f.getThisId()
    //            << "::" << f.getMethodName() << "\n";
    //}

    // std::cout << std::endl;
  }

  inline void popFrame() {
    ASSERT(this->hasFrames() > 0 && "can't pop from empty stack");
        this->frames.pop_back();
  }

  //inline const &frame &peekFrame() const {
  //  assert(this->frames.size() > 0 && "can't peek on empty stack");
  //  return this->frames.back();
  //}

  inline const frame &peekFrame(int depth = 0) const {
    auto size = this->hasFrames();
    ASSERT(size >= depth && "can't peek that deep into stack");
    const auto &ret = this->frames.at(size-depth-1);
    return ret;
  }


  inline int hasFrames() const {
    return this->frames.size();
  }
};

#endif /* end of include guard: CALLSTACK_HH */
