/**
  * Functions for tagging objects.
  *
  * All functions in this module require the global lock to be taken.
  */
#ifndef TAGGING_HH
#define TAGGING_HH

#include <jvmti.h>
#include <jni.h>
#include <map>
#include <stack>
#include <vector>
#include "NativeInterface.h"
#include <sstream>
#include <set>
#include "callstack.hh"
#include <sys/types.h>
#include <sys/dir.h>
#include <atomic>

std::atomic_long nextObjID(NativeInterface_SPECIAL_VAL_MAX+1);
std::map<std::string, callstack> stacks;
std::map<std::string, std::stack<long> > freshObjectIDs;
std::set<std::string> instrumentedClasses;
std::set<std::string> uninstrumentedClasses;

std::string toStdString(JNIEnv *env, jstring str);

/**
  Tag an object with a fresh ID.

  Requires that the object hasn't been tagged before (warns otherwise).

*/
long doTag(jvmtiEnv *g_jvmti, jobject jobj) {
  jlong tag;
  jvmtiError err;
  ASSERT(jobj != NULL);
  err = g_jvmti->GetTag(jobj, &tag);
  ASSERT_NO_JVMTI_ERR(g_jvmti, err);
  //  ASSERT(tag == 0);
  tag = nextObjID.fetch_add(1);
  DBG("tagging object in doTag with "<< tag);
  err = g_jvmti->SetTag(jobj, tag);
  ASSERT_NO_JVMTI_ERR(g_jvmti, err);
  ASSERT(tag != 0);
  // DBG("set tag " << tag << " on " << jobj);
  return tag;
}

#endif /* end of include guard: TAGGING_HH */
