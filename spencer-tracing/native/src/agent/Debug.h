#ifndef DEBUG_H
#define DEBUG_H

#include <iostream>
#include <assert.h>

#ifdef NODEBUG
#undef NODEBUG
#endif

struct DebugSummary {
  int totalWarnings;

  DebugSummary() : totalWarnings(0) {}

  ~DebugSummary() {
    //    std::cout << "instrumented with " << totalWarnings << "!" <<
    // std::endl;
  }
};

extern DebugSummary summary;

#define NODEBUG

#define DODBG(str)                                                             \
  { std::cout << __FILE__ << ":" << __LINE__ << ": DBG " << str << std::endl; }

#ifdef NODEBUG
#define DBG(str)                                                               \
  {}
#else
#define DBG(str)                                                               \
  { DODBG(str); }
#endif

#ifdef NODEBUG
#define WARN(str)                                                              \
  {}
#else
#define WARN(str)                                                              \
  {                                                                            \
    std::cout << __FILE__ << ":" << __LINE__ << ": WARN "                      \
              << ": " << str << std::endl;                                     \
  }
#endif

#define ASSERT(exp) {if (!(exp)) { ERR("assertion failed!"); }}
#define ASSERT_MSG(exp, msg) {if (!(exp)) { ERR("assertion failed: "<<msg); }}
#define ASSERT_EQ(exp, exp2) {if ((exp) != (exp2)) {  ERR("assertion failed:\n"<<exp<<"\n\t!=\n"<<exp2); }}
#define ASSERT_NEQ(exp, exp2) {if ((exp) == (exp2)) { ERR("assertion failed:\n"<<exp<<"\n\t==\n"<<exp2); }}

#define ERR(str)                                                               \
  {                                                                            \
    std::cout << __FILE__ << ":" << __LINE__ << ": ERROR " << str              \
              << std::endl;                                                    \
    exit(EXIT_FAILURE);                                                        \
  }
//#define ERRS(str, msg) { printf("ERR %s:%i %s: %s\n", __FILE__,__LINE__,str,
// msg); exit(EXIT_FAILURE); }
#define REQ(cond)                                                              \
  {                                                                            \
    if (!(cond))                                                               \
      ERR("REQUIRE");                                                          \
  }

#define ASSERT_NO_JVMTI_ERR(jvmti, err)                                        \
  {                                                                            \
    if (err != JVMTI_ERROR_NONE) {                                             \
      char *msg;                                                               \
      (jvmti)->GetErrorName(err, &msg);                                        \
      ERR(msg);                                                                \
    }                                                                          \
  }

#endif // ifndef DEBUG_H
