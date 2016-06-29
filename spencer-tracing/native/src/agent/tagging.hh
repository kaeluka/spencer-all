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

std::atomic_long nextObjID(NativeInterface_SPECIAL_VAL_MAX+1);
//arst std::map<long, std::vector<std::pair<long, std::string> > > stacks;
std::map<std::string, callstack> stacks;
std::map<std::string, std::stack<long> > freshObjectIDs;
std::set<std::string> instrumentedClasses;
std::set<std::string> uninstrumentedClasses;

void ensureThreadKnown(const std::string &threadName) {
  if (stacks.find(threadName) == stacks.end()) {
    DBG("making thread #" << threadName << " known");
    // accessing, as a side effect, creates the object:
    stacks[threadName];
    freshObjectIDs[threadName];
  }
  ASSERT(freshObjectIDs.find(threadName) != freshObjectIDs.end());
}

callstack &getThreadStack(std::string threadName) {
  DBG("getting stack for thread '"<<threadName<<"'");
  if (stacks.find(threadName) != stacks.end()) {
    ensureThreadKnown(threadName);
  }
  return stacks[threadName];
}

void printCurrentStack(std::string threadName) {
  DBG("stack of thread " << threadName << " is now:");
  DBG("------------");
  const auto & threadStack = getThreadStack(threadName);
  for (int i=0; i<threadStack.hasFrames(); ++i) {
     DBG(" ******************  ["
     <<threadStack.peekFrame(i).getThisId()<<"@"
     <<threadStack.peekFrame(i).getClassName() << "::"
     <<threadStack.peekFrame(i).getMethodName() << "]");
  }
  //DBG("... omitted ..");
  DBG("------------");
}

bool fileExists(const std::string &path) {
  FILE *file;
  if ((file = fopen(path.c_str(), "r")) != NULL) {
    fclose(file);
    return true;
  } else {
    return false;
  }
}

bool isClassInstrumented(const std::string &className) {
  return (
    className.c_str()[0] == '['
      ||
    instrumentedClasses.find(className) != instrumentedClasses.end()
  );
}

void markClassAsInstrumented(const std::string &className) {
  DBG("marking "<<className<<" as instrumented");
  instrumentedClasses.insert(className);
  ASSERT(isClassInstrumented(className));
}

void markClassFilesAsInstrumented(const std::string &directory, const std::string &package = "") {
  DIR *dir;
  struct dirent *ent;
  if ((dir = opendir ((directory+"/"+package).c_str())) != NULL) {

    while ((ent = readdir (dir)) != NULL) {
      if (ent->d_name[0] != '.') {
        std::string fileName = package+"/"+ent->d_name;
        size_t appendixSize = std::string(".class").size();
        if (fileName.size() >= appendixSize) {
          auto stem = fileName.substr(1,fileName.size()-appendixSize-1);
          auto extension = fileName.substr(fileName.size()-appendixSize, appendixSize);
          if (extension == ".class") {
            DBG("adding "<<stem<<" to instrumented class files");
            markClassAsInstrumented(stem);
            continue;
          }
        }
        markClassFilesAsInstrumented(directory, package+"/"+ent->d_name);
      }
    }
    closedir (dir);
  }
}

void markClassAsUninstrumented(const std::string &className) {
  DBG("marking class "<<className<<" as uninstrumented");
  ASSERT(!isClassInstrumented(className));
  uninstrumentedClasses.insert(className);
  ASSERT(!isClassInstrumented(className));
}

//void handleValStatic(JNIEnv *env, jint *objectkind, jobject *jobj,
//                     jstring klassname);

std::string toStdString(JNIEnv *env, jstring str);

long doTag(jvmtiEnv *jvmti, jobject jobj);

long getOrDoTag(jint objkind, jobject jobj, std::string klass, const std::string &thread,
                jvmtiEnv *g_jvmti, jrawMonitorID g_lock);

std::string getThreadName(jvmtiEnv* g_jvmti, jrawMonitorID g_lock) {
  jthread thread;
  jvmtiError err = g_jvmti->GetCurrentThread(&thread);

  ASSERT_NO_JVMTI_ERR(g_jvmti, err);

  std::string threadName;

  jvmtiThreadInfo info;
  err = g_jvmti->GetThreadInfo(thread, &info);
  if (err == JVMTI_ERROR_WRONG_PHASE) {
    if (thread == NULL) {
      threadName = "JVM_Thread<0x0>";
    } else {
      DBG("tagging startup thread "<<thread);
      // GetThreadInfo works only during live phase
      std::stringstream ss;
      jlong tag;
      err = g_jvmti->GetTag(thread, &tag);
      ASSERT_NO_JVMTI_ERR(g_jvmti, err);
      if (tag == 0) {
        tag = nextObjID.fetch_add(20);
        err = g_jvmti->SetTag(thread, tag);
        ASSERT_NO_JVMTI_ERR(g_jvmti, err);
      }
        //getOrDoTag(NativeInterface_SPECIAL_VAL_NORMAL, thread, "java/lang/Thread", "wat", *g_jvmti, g_lock);
      //err = g_jvmti->SetTag(thread, tag);

      ss<<"JVM_Thread<"<<tag<<">";
      threadName = ss.str();
    }
  } else {
    threadName = info.name;
    ASSERT_NO_JVMTI_ERR(g_jvmti, err);
  }
  ensureThreadKnown(threadName);
  DBG("done getting thread name");
  return threadName;
}

void pushStackFrame(JNIEnv *env, const std::string &thread, jint callee_kind,
                    jobject callee, jstring mname, jstring cname,
                    jvmtiEnv *g_jvmti, jrawMonitorID g_lock) {
  //temporary  DBG("pushing stack frame");
  //temporary
  //temporary  std::string threadName = getThreadName(g_jvmti, g_lock);
  //temporary
  //temporary  jlong calleeTag;
  //temporary  if (callee_kind == NativeInterface_SPECIAL_VAL_THIS) {
  //temporary    // create a new object ID and push it to the stack.
  //temporary    // handleValStatic will then use this ID whenever it sees a
  //temporary    // SPECIAL_VAL_THIS!
  //temporary
  //temporary    ASSERT_MSG(!freshObjectIDs[threadName].empty(),
  //temporary               "have no object ID for SPECIAL_VAL_THIS (method "<<toStdString(env, mname)
  //temporary               <<", class "<<toStdString(env,cname)<<")");
  //temporary    calleeTag = freshObjectIDs[threadName].top();
  //temporary    DBG("thread " << threadName << ": generating tag " << calleeTag
  //temporary                  << " for SPECIAL_VAL_THIS::"<<toStdString(env,cname));
  //temporary    //freshObjectIDs[threadName].push(calleeTag);
  //temporary  } else {
  //temporary    //handleValStatic(env, &callee_kind, &callee, cname);
  //temporary    calleeTag = getOrDoTag(callee_kind, callee, toStdString(env, cname),
  //temporary                            threadName, g_jvmti, g_lock);
  //temporary  }
  //temporary  std::string mnameStr = toStdString(env, mname);
  //temporary  std::string cnameStr = toStdString(env, cname);
  //temporary  ASSERT(mnameStr != "java/security/BasicPermission");
  //temporary  getThreadStack(threadName)
  //temporary      .pushFrame(mnameStr, cnameStr, calleeTag);
  //temporary  ASSERT(getThreadStack(threadName).hasFrames() > 0);
  //temporary  ASSERT(getThreadStack(threadName).peekFrame().getMethodName() != "");
  //temporary  DBG("pushing stack frame done");
  //temporary  printCurrentStack(threadName);
}

long getTag(jint objkind, jobject jobj, std::string klass, const std::string &threadName,
            jvmtiEnv *g_jvmti);

bool tagFreshlyInitialisedObject(jvmtiEnv *g_jvmti, jobject callee,
                                 std::string threadName) {
  DBG("tagFreshlyInitialisedObject(..., threadName = " << threadName << ")");
  ASSERT(freshObjectIDs.find(threadName) != freshObjectIDs.end() &&
         "need to know the thread");
  if (getTag(NativeInterface_SPECIAL_VAL_NORMAL, callee, "class/unknown", threadName, g_jvmti) != 0) {
    return false;
  }
  ASSERT(g_jvmti);
  ASSERT(callee);
  //doTag(g_jvmti, callee);

  jlong tag;
  //  if (freshObjectIDs[threadName].empty()) {
  DBG("thread " << threadName << ": don't have an object ID");
  tag = nextObjID.fetch_add(1);
  freshObjectIDs[threadName].push(tag);
  //} else {
  //  tag = freshObjectIDs[threadName].top();
  //  freshObjectIDs[threadName].pop();
  //}
  DBG("tagging freshly initialised object with id " << tag);
  jvmtiError err = g_jvmti->SetTag(callee, tag);
  ASSERT_NO_JVMTI_ERR(g_jvmti, err);
  return true;
  // DBG("set tag " << tag << " on " << jobj);
}

//temporaryvoid popStackFrame(const std::string &threadName) {
  //temporary  //if (getThreadStack(threadName).hasFrames() == 0) {
  //temporary  //  WARN("trying to pop from empty stack model for thread" << threadName);
  //temporary  //} else {
  //temporary  if (getThreadStack(threadName).hasFrames() <= 0) {
  //temporary    ERR("popping frame from empty stack");
  //temporary    return;
  //temporary  }
  //temporary  const auto &oldTop = getThreadStack(threadName).peekFrame();
  //temporary  if (oldTop.getClassName() != "JvmInternals" && oldTop.getMethodName() != "jvmInternals") {
  //temporary    getThreadStack(threadName).popFrame();
  //temporary    DBG("popped frame ["<<oldTop.getThisId()<<"@"<<oldTop.getClassName()<<"::"<<oldTop.getMethodName()<<"]");
  //temporary  } else {
  //temporary    ERR("did not pop frame of magic method JvmInternals::jvmInternals");
  //temporary  }
  //temporary  //}
  //temporary  printCurrentStack(threadName);
//temporary}

//temporaryconst frame getRunningFrame(const std::string &threadName) {
//temporary  if (getThreadStack(threadName).hasFrames() <= 0) {
//temporary    ERR("getting from from empty stack");
//temporary    frame f("unregistered method", "unregistered class", NativeInterface_SPECIAL_VAL_NOT_IMPLEMENTED);
//temporary    return f;
//temporary  }
//temporary  return getThreadStack(threadName).peekFrame();
//temporary}

//temporarystd::string getRunningFunction(const std::string &threadName) {
//temporary  auto ret = getRunningFrame(threadName);
//temporary  ASSERT(ret.getMethodName() != "");
//temporary  ASSERT(ret.getClassName() != "");
//temporary  return ret.getMethodName();
//temporary  return "TODO:instrument exited method name";
//temporary}

//temporarylong getRunningObject(const std::string &threadName) { return getRunningFrame(threadName).getThisId(); }

//temporaryconst std::string getRunningClass(const std::string &threadName) { return getRunningFrame(threadName).getClassName(); }
const std::string getRunningClass(const std::string &threadName) { return "TODO:instrument running class"; }

/**
  Tag an object with a fresh ID.

  Requires that the object hasn't been tagged before (warns otherwise).

*/
long doTag(jvmtiEnv *g_jvmti, jobject jobj) {
  jlong tag;
  jvmtiError err;
  err = g_jvmti->GetTag(jobj, &tag);
  //ASSERT(tag == 0);
  tag = nextObjID.fetch_add(1);
  err = g_jvmti->SetTag(jobj, tag);
  //ASSERT_NO_JVMTI_ERR(g_jvmti, err);
  //ASSERT(tag != 0);
  // DBG("set tag " << tag << " on " << jobj);
  return tag;
}

#define NATIVE_CLASSREPS

#ifdef NATIVE_CLASSREPS
std::map<const std::string, const jlong> classReps;

jlong getClassRepTag(const std::string &className) {
  if (classReps.find(className) == classReps.end()) {
    jlong tag = -nextObjID.fetch_add(1);
    classReps.insert({ { className, tag } });
    return tag;
  } else {
    return classReps[className];
  }
}
#endif

long getTag(jint objkind, jobject jobj, std::string klass, const std::string &threadName,
            jvmtiEnv *g_jvmti) {
  jlong tag = 0;
    switch (objkind) {
  case NativeInterface_SPECIAL_VAL_NORMAL: {
    if (jobj) {
      jvmtiError err = g_jvmti->GetTag(jobj, &tag);
      // DBG("getting preliminary tag " << tag << " from " << jobj);
      DBG("getting tag (" << klass << " @ " << tag << ") from JVMTI");
      ASSERT_NO_JVMTI_ERR(g_jvmti, err);
      }
    }
    break;
   case NativeInterface_SPECIAL_VAL_THIS: {
     if (getThreadStack(threadName).hasFrames() > 0) {
       ASSERT(getThreadStack(threadName).hasFrames() > 0);
       tag = getThreadStack(threadName).peekFrame().getThisId();
       DBG("getting tag (" << klass << " @ " << tag << ") from top");
     } else {
       DBG("getting tag (" << klass << " @ " << NativeInterface_SPECIAL_VAL_JVM
                           << ") from SPECIAL_VAL_JVM");
       tag = NativeInterface_SPECIAL_VAL_JVM;
     }
     break;
  }
  case NativeInterface_SPECIAL_VAL_STATIC: {
    tag = getClassRepTag(klass);
    DBG("getting tag (" << klass << " @ " << tag << ") from classRep");
    break;
  }
  case NativeInterface_SPECIAL_VAL_NOT_IMPLEMENTED: {
    ERR("SPECIAL_VAL_NOT_IMPLEMENTED");
    break;
  }
  default:
    ERR("WAT? "<<objkind);
  }

  return tag;
}

long getOrDoTag(jint objkind, jobject jobj, std::string klass, const std::string &thread,
                jvmtiEnv *g_jvmti, jrawMonitorID g_lock) {
  // LOCK;

  jlong tag = getTag(objkind, jobj, klass, thread, g_jvmti);
  if (tag == 0) {
    tag = doTag(g_jvmti, jobj);
    DBG("setting tag (" << klass << " @ " << tag << ") using doTag");
    //ASSERT(getOrDoTag(objkind, jobj, klass, thread, g_jvmti, g_lock) ==
    //       tag);
    ASSERT(tag != 0);
  }
  return tag;
}

/*
  returns a c++ string with content copied from a java str
*/
std::string toStdString(JNIEnv *env, jstring str) {
  // BUG: if I comment stuff below back in: crashes!
  if (str == NULL) {
    return "NULL";
  }
  const char *c_str = env->GetStringUTFChars(str, NULL);
  const std::string result(c_str);
  env->ReleaseStringUTFChars(str, c_str);
  return result;
}

/**

If the object kind is SPECIAL_VAL_STATIC, replace the reference by a
reference to the class' representative.

*/

//void handleValStatic(JNIEnv *env, jint *objectkind, jobject *jobj,
//                     jlong *tag, jstring klassName) {
//   DBG("handleValStatic");
//   if (*objectkind == NativeInterface_SPECIAL_VAL_STATIC) {
//
//#ifdef NATIVE_CLASSREPS
//    *tag = getClassRepTag(klassName);
//#else
//    jclass cls = env->FindClass("NativeInterface");
//    jmethodID getClassRep =
//        env->GetStaticMethodID(cls, "getClassRepresentative",
//                               "(Ljava/lang/String;)Ljava/lang/Object;");
//    jclass nativeIF = (jclass)env->NewGlobalRef(cls);
//    *jobj = env->CallStaticObjectMethod(nativeIF, getClassRep, klassname);
//    *objectkind = NativeInterface_SPECIAL_VAL_NORMAL;
//#endif
//    // env->DeleteLocalRef(cls);
//  }
//}
//
#endif /* end of include guard: TAGGING_HH */
