#ifndef MONITOR_GUARD_HH
#define MONITOR_GUARD_HH
#include <jvmti.h>
#include <string>

#define NODEBUG_LOCKS

class MonitorGuard {
  const jrawMonitorID lck;
  jvmtiEnv *jvmti;
  std::string descr;

public:
  MonitorGuard(jrawMonitorID _lck, jvmtiEnv *_jvmti, std::string _descr)
      : lck(_lck), descr(_descr) {
    this->jvmti = _jvmti;
    if (this->descr != "") {
      printf("%s - trying to take lock %p\n", this->descr.c_str(), _lck);
    }
    jvmtiError err = this->jvmti->RawMonitorEnter(this->lck);
    if (this->descr != "") {
      printf("%s - took lock\n", this->descr.c_str());
    }
    ASSERT_NO_JVMTI_ERR(this->jvmti, err);
  }

  ~MonitorGuard() {
    jvmtiError err = this->jvmti->RawMonitorExit(this->lck);
    ASSERT_NO_JVMTI_ERR(this->jvmti, err);
    if (this->descr != "") {
      printf("%s - released lock\n", this->descr.c_str());
    }
  }
};

#ifndef NODEBUG_LOCKS

#define LOCK                                                                   \
  std::stringstream guardDescrStream;                                          \
  guardDescrStream << __FILE__ << ":" << __LINE__;                             \
  MonitorGuard guard(g_lock, g_jvmti, guardDescrStream.str());

#else

#define LOCK MonitorGuard guard(g_lock, g_jvmti, "");

#endif // ifndef NODEBUG_LOCKS

#endif /* end of include guard: MONITOR_GUARD_HH */
