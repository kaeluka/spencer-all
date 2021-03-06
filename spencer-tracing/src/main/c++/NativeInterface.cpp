#define ENABLED

#include "Debug.h"

#include "NativeInterface.h"
#include <iostream>
#include "events.h"
#include "MonitorGuard.hh"
#include "tagging.hh"

#include <fcntl.h>
#include <unistd.h>
#include <capnp/serialize.h>
#include <sstream>

#include <netdb.h>
#include <netinet/tcp.h>
#include <stdlib.h>

#include <algorithm>

#include <pwd.h>

using namespace std;

//extern std::atomic_long nextObjID;

/*******************************************************************/
/* Global Data                                                     */
/*******************************************************************/

int capnproto_fd;
#define SOCKET_PORT "1345"

std::string tracefilename = "none";
bool traceToDisk = false;

// global ref to jvmti enviroment
static jvmtiEnv *g_jvmti = NULL;
static bool flag_application_only = false;

// indicates JVM initialization
static bool g_init = false;
// indicates JVM death
static bool g_dead = false;

jvmtiError g_jvmtiError;

// prefixes of classes that will not be instrumented before the live phase:
std::vector<std::string> onlyDuringLivePhaseMatch;

struct SpencerClassRedefinition {
  std::string name;
  jvmtiClassDefinition klassDef;
};

std::vector<SpencerClassRedefinition> redefineDuringLivePhase;


string kindToStr(jint v) {
  ASSERT(NativeInterface_SPECIAL_VAL_NORMAL <= v &&
         v <= NativeInterface_SPECIAL_VAL_MAX);
  switch (v) {
  case NativeInterface_SPECIAL_VAL_NORMAL:
    return "SPECIAL_VAL_NORMAL";
  case NativeInterface_SPECIAL_VAL_THIS:
    return "SPECIAL_VAL_THIS";
  case NativeInterface_SPECIAL_VAL_STATIC:
    return "SPECIAL_VAL_STATIC";
  case NativeInterface_SPECIAL_VAL_NOT_IMPLEMENTED:
    return "SPECIAL_VAL_NOT_IMPLEMENTED";
  default:
    ERR("can't interpret val kind v");
    return "";
  }
}

static jrawMonitorID g_lock;

std::string getThreadName() {
  // return "a thread";
  jvmtiThreadInfo info;
  jthread thread;
  jvmtiError err = g_jvmti->GetCurrentThread(&thread);
  err = g_jvmti->GetThreadInfo(NULL, &info);
  if (err == JVMTI_ERROR_WRONG_PHASE) {
    //return "<unknown thread>";
    if (thread == NULL) {
      return "<JVM_Thread 0x0>";
    } else {
      // GetThreadInfo works only during live phase
      std::stringstream ss;
      jlong tag;
      err = g_jvmti->GetTag(thread, &tag);
      ASSERT_NO_JVMTI_ERR(g_jvmti, err);
      if (tag == 0) {
        tag = nextObjID.fetch_add(1);
        DBG("tagging thread "<<thread<<" with "<<tag);
        err = g_jvmti->SetTag(thread, tag);
        ASSERT_NO_JVMTI_ERR(g_jvmti, err);
      }

      ss<<"<JVM_Thread "<<tag<<">";
      return ss.str();
    }
  } else {
    ASSERT_NO_JVMTI_ERR(g_jvmti, err);
    std::string ret(info.name);
    g_jvmti->Deallocate((unsigned char*)info.name);
    if (ret == "") {
      return "<unnamed thread>";
    } else {
      return ret;
    }
  }
  /*
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

  */

}

std::string toCanonicalForm(std::string typ) {
  std::string ret = typ;
  std::replace(ret.begin(), ret.end(), '/', '.');
  return ret;
}

std::string toInternalForm(std::string typ) {
  std::string ret = typ;
  std::replace(ret.begin(), ret.end(), '.', '/');
  return ret;
}

void doFramePop(std::string mname, std::string cname) {
  std::string threadName = getThreadName();
  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  MethodExitEvt::Builder msgbuilder = innermessage.initRoot<MethodExitEvt>();
  //           "java/lang/Thread", thread, g_jvmti, g_lock);
  msgbuilder.setThreadName(threadName);
  msgbuilder.setName(mname);
  msgbuilder.setCname(cname);
  ASSERT(std::string("") != msgbuilder.getName().cStr());
  anybuilder.setMethodexit(msgbuilder.asReader());

  if (traceToDisk) {
    capnp::writeMessageToFd(capnproto_fd, outermessage);
  }
}

inline bool isInLivePhase() {
  jvmtiPhase phase;
  jvmtiError err = g_jvmti->GetPhase(&phase);
  ASSERT_NO_JVMTI_ERR(g_jvmti, err);
  return (phase == JVMTI_PHASE_LIVE);
}

#define REQUIRE_LIVE() {if (!isInLivePhase()) { return; } }

/*******************************************************************/
/* Socket management                                               */
/*******************************************************************/

int setupSocket() {
  int sock;
  int status;
  struct addrinfo hints;
  struct addrinfo *servinfo;       // address info for socket
  memset(&hints, 0, sizeof hints); // make sure the struct is empty
  hints.ai_family = AF_UNSPEC;     // don't care IPv4 or IPv6
  hints.ai_socktype = SOCK_STREAM; // TCP stream sockets
  hints.ai_flags = AI_PASSIVE;     // fill in my IP for me

  if ((status = getaddrinfo(NULL, SOCKET_PORT, &hints, &servinfo)) != 0) {
    ERR("getaddrinfo error: " << gai_strerror(status));
  }

  sock =
    socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);
  if (sock < 0) {
    ERR("could not open socket (" << sock << ")");
  }

  if (connect(sock, servinfo->ai_addr, servinfo->ai_addrlen) != 0) {
    ERR("Could not connect! Please make sure that the transformer "
        "program is running (path/to/java-alias-agent/transformer)");
  }
  freeaddrinfo(servinfo);

  int flag = 1;
  setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));

  return sock;
}

void closeSocket(int sock) { close(sock); }

size_t recvClass(int sock, unsigned char **data) {
  DBG("receiving class");
  union {
    long i;
    unsigned char bs[8];
  } x;
  x.i = 0;
  ssize_t rlen = recv(sock, x.bs, 8 * sizeof(unsigned char), 0);
  if (rlen == -1) {
    ERR("could not receive");
  }

  size_t len = 0;
  for (int i = 0; i <= 7; ++i) {
    len += (size_t)(x.bs[i] << (8 * (7 - i)));
  }

  DBG("received length " << len);

  *data = (unsigned char *)calloc(len, sizeof(unsigned char));
  rlen = recv(sock, *data, len * sizeof(unsigned char), MSG_WAITALL);
  DBG("rlen=" << rlen);
  ASSERT(rlen == len);
  closeSocket(sock);
  return (size_t)rlen;
}

void sendClass(int sock, const unsigned char *data, uint32_t len) {
  DBG("sending class, length = " << len);
  union {
    uint32_t i;
    unsigned char bs[4];
  } x;
  x.i = len;
  send(sock, x.bs, sizeof(unsigned char) * 4, 0);
  send(sock, data, sizeof(unsigned char) * len, 0);

  //if (len >= 1000) {
  //  return;
  //}
  //char empty[1000] = {0};
  //ASSERT(sizeof(empty)==1000);
  //send(sock, empty, sizeof(empty), 0);
  closeSocket(sock);
}

void transformClass(const unsigned char *class_data, uint32_t class_data_len, unsigned char **new_class_data, uint32_t *new_class_data_len) {
  int sock;
  sock = setupSocket();
  sendClass(sock, class_data, class_data_len);

  sock = setupSocket();
  *new_class_data_len = (uint32_t)recvClass(sock, new_class_data);
  ASSERT(*new_class_data != class_data);
}

void handleLoadFieldA(
                      JNIEnv *env, jclass native_interface,
                      jobject val, jint holderKind, jobject holder,
                      std::string holderClass, std::string fname,
                      std::string type, std::string callerClass,
                      std::string callerMethod, jint callerKind,
                      jobject caller);

void handleStoreFieldA(
                       JNIEnv *env, jclass native_interface, jint holderKind, jobject holder,
                       jobject newval,
                       jobject oldval,
                       std::string holderClass,
                       std::string fname,
                       std::string type,
                       std::string callerClass,
                       std::string callerMethod, jint callerKind,
                       jobject caller);

void handleModify(JNIEnv *env, jclass native_interface,
                  jint calleeKind,
                  jobject callee,
                  std::string calleeClass,
                  std::string fname,
                  jint callerKind,
                  jobject caller,
                  std::string callerClass);

void handleRead(JNIEnv *env, jclass native_interface,
                jint calleeKind,
                jobject callee,
                std::string calleeClass,
                std::string fname,
                jint callerKind,
                jobject caller,
                std::string callerClass);

std::string getTypeForObj(JNIEnv *jni_env, jobject obj);

/*
  returns a c++ string with content copied from a java str
*/
std::string toStdString(JNIEnv *env, jstring str) {
  //DBG("getting string "<<str);
  if (str == NULL) {
    return "NULL";
  }
  const char *c_str = env->GetStringUTFChars(str, NULL);
  //DBG("got "<<c_str);
  const std::string result(c_str);
  env->ReleaseStringUTFChars(str, c_str);
  return result;
}

/*******************************************************************/
/* Event Callbacks                                                 */
/*******************************************************************/

/*
 * Class:     NativeInterface
 * Method:    loadArrayA
 * Signature: ([Ljava/lang/Object;ILjava/lang/Object;Ljava/lang/String;Ljava/lang/String;ILjava/lang/Object;)V
 */
JNIEXPORT void JNICALL Java_NativeInterface_loadArrayA
(JNIEnv *env, jclass native_interface,
 jobjectArray arr,
 jint idx,
 jobject val,
 jstring _holderClass,
 jstring _callerMethod,
 jstring _callerClass,
 jint callerValKind,
 jobject caller) {
  REQUIRE_LIVE();
  LOCK;

  stringstream field;
  field<<"_"<<idx;
  std::string holderClass  = toStdString(env, _holderClass);
  std::string callerMethod = toStdString(env, _callerMethod);
  std::string callerClass  = toStdString(env, _callerClass);

  std::string elementType  = holderClass.substr(1);
  ASSERT_EQ("["+elementType, holderClass);
  handleLoadFieldA(env, native_interface, val, NativeInterface_SPECIAL_VAL_NORMAL, arr, holderClass, field.str(), elementType, callerClass, callerMethod, callerValKind, caller);

}

/*
 * Class:     NativeInterface
 * Method:    storeArrayA
 * Signature: (Ljava/lang/Object;[Ljava/lang/Object;ILjava/lang/Object;Ljava/lang/String;Ljava/lang/String;ILjava/lang/Object;)V
 */
JNIEXPORT void JNICALL Java_NativeInterface_storeArrayA
(JNIEnv *env, jclass native_interface,
 jobject newVal,
 jobjectArray arr,
 jint idx,
 jobject oldVal,
 jstring _holderClass,
 jstring _callerMethod,
 jstring _callerClass,
 jint callerValKind,
 jobject caller) {
  REQUIRE_LIVE();
  LOCK;

  stringstream field;
  field<<"_"<<idx;
  //std::string holderClass  = toStdString(env, _holderClass);
  std::string callerMethod = toStdString(env, _callerMethod);
  std::string callerClass  = toStdString(env, _callerClass);
  std::string holderClass =  toStdString(env, _holderClass);

  std::string elementType  = holderClass.substr(1);
  ASSERT_EQ("["+elementType, holderClass);

  handleStoreFieldA(env, native_interface,
                    NativeInterface_SPECIAL_VAL_NORMAL,
                    arr,
                    newVal,
                    oldVal,
                    holderClass,
                    field.str(),
                    elementType,
                    callerClass,
                    callerMethod,
                    callerValKind,
                    caller);
}

/*
 * Class:     NativeInterface
 * Method:    readArray
 * Signature: (Ljava/lang/Object;IILjava/lang/Object;Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_NativeInterface_readArray
(JNIEnv *env, jclass nativeInterface, jobject arr, jint idx, jint callerValKind, jobject caller, jstring callerClass) {
  REQUIRE_LIVE();
  LOCK;
  stringstream field;
  field<<"_"<<idx;

  handleRead(env, nativeInterface,
             NativeInterface_SPECIAL_VAL_NORMAL,
             arr,
             getTypeForObj(env, arr),
             field.str(),
             callerValKind,
             caller,
             toStdString(env, callerClass));
}

/*
 * Class:     NativeInterface
 * Method:    modifyArray
 * Signature: (Ljava/lang/Object;IILjava/lang/Object;Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_NativeInterface_modifyArray
(JNIEnv *env, jclass nativeInterface, jobject arr, jint idx, jint callerValKind, jobject caller, jstring callerClass) {
  REQUIRE_LIVE();
  LOCK;
  stringstream field;
  field<<"_"<<idx;

  handleModify(env, nativeInterface,
               NativeInterface_SPECIAL_VAL_NORMAL,
               arr,
               getTypeForObj(env, arr),
               field.str(),
               callerValKind,
               caller,
               toStdString(env, callerClass));

}


JNIEXPORT void JNICALL
Java_NativeInterface_methodExit(JNIEnv *env, jclass, jstring _mname, jstring _cname) {
#ifdef ENABLED
  REQUIRE_LIVE();
  LOCK;
  const auto mname = toStdString(env, _mname);
  const auto cname = toStdString(env, _cname);
  ASSERT(mname != "");
  DBG("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");
  DBG("Java_NativeInterface_methodExit:  ...::"<<mname<<"), thd: "<<getThreadName());
  ASSERT(mname != "ClassRep");
  doFramePop(mname, cname);
  DBG("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");

#endif
}

/**
   Get the currently running this (best effort, will return NULL at least for
   native methods and before the live phase). Must not be called from static
   method.
*/
jobject getThis() {
  LOCK;
  jobject ret;
  if (g_jvmti == NULL) {
    return NULL;
  }
  jvmtiError err = g_jvmti->GetLocalObject(NULL, //use current thread
                                           0,    //stack depth
                                           0,    //variable 0 -- this
                                           &ret);
  if (err == JVMTI_ERROR_OPAQUE_FRAME || JVMTI_ERROR_WRONG_PHASE) {
    return NULL;
  } else {
    ASSERT_NO_JVMTI_ERR(g_jvmti, err);
    return ret;
  }
}

std::string getTypeForObj(JNIEnv *jni_env, jobject obj) {
  ASSERT(obj != NULL);
  jclass klassKlass = jni_env->FindClass("java/lang/Class");
  jmethodID getName = jni_env->GetMethodID(klassKlass, "getName", "()Ljava/lang/String;");
  ASSERT(getName != NULL);

  jclass klass = jni_env->GetObjectClass(obj);
  jstring name = (jstring)jni_env->CallObjectMethod(klass, getName);
  if (name == NULL) {
    return "<anonymousClass>";
  }

  // we tag the string as not instrumented, as we don't want it to end up in the data!
  jvmtiError err = g_jvmti->SetTag(name, NativeInterface_SPECIAL_VAL_NOT_INSTRUMENTED);
  if (err == JVMTI_ERROR_INVALID_OBJECT || err == JVMTI_ERROR_WRONG_PHASE) {
    WARN("got JVMTI_ERROR_INVALID_OBJECT or JVMTI_ERROR_WRONG_PHASE");
  } else {
    ASSERT_NO_JVMTI_ERR(g_jvmti, err);
  }

  std::string nameStr = toStdString(jni_env, name);

  std::replace(nameStr.begin(), nameStr.end(), '.', '/');

  return nameStr;
}

std::string getClassName(JNIEnv *jni_env, jclass obj) {
  jclass klassKlass = jni_env->FindClass("java/lang/Class");
  ASSERT(klassKlass);
  jmethodID getName = jni_env->GetMethodID(klassKlass, "getName", "()Ljava/lang/String;");
  ASSERT(getName != NULL);

  jstring ret = (jstring)jni_env->CallObjectMethod(obj, getName);

  // we tag the string as not instrumented, as we don't want it to end up in the data!
  g_jvmti->SetTag(ret, NativeInterface_SPECIAL_VAL_NOT_INSTRUMENTED);

  return toInternalForm(toStdString(jni_env, ret));
}

std::string getToString(JNIEnv *jni_env, jobject obj) {
  jclass objKlass = jni_env->FindClass("java/lang/Object");
  ASSERT(objKlass);
  jmethodID toString = jni_env->GetMethodID(objKlass, "toString", "()Ljava/lang/String;");
  ASSERT(toString != NULL);

  jstring ret = (jstring)jni_env->CallObjectMethod(obj, toString);

  // we tag the string as not instrumented, as we don't want it to end up in the data!
  g_jvmti->SetTag(ret, NativeInterface_SPECIAL_VAL_NOT_INSTRUMENTED);

  return toStdString(jni_env, ret);
}

std::string getTypeForThis(JNIEnv *jni_env) {
  return getTypeForObj(jni_env, getThis());
}


std::string getTypeForObjKind(jvmtiEnv *jvmti_env, JNIEnv *jni_env, jobject obj, jint kind, jstring definitionKlass) {
  ASSERT(definitionKlass != NULL);
  switch (kind) {
  case NativeInterface_SPECIAL_VAL_NORMAL: {
    return getTypeForObj(jni_env, obj);
  }
  case NativeInterface_SPECIAL_VAL_THIS: {
    jobject _this = getThis();
    if (_this == NULL) {
      return toStdString(jni_env, definitionKlass);
    } else {
      return getTypeForObj(jni_env, _this);
    }
  }
  case NativeInterface_SPECIAL_VAL_STATIC: {
    return toCanonicalForm(toStdString(jni_env, definitionKlass));
  }
  default:
    ERR("Can't handle kind "<<kind);
  }
}

jobject getObjectForTag(JNIEnv *jni_env, jlong tag) {
  jint count = -1;
  jobject *obj_res = NULL;
  g_jvmti->GetObjectsWithTags(1,  // Tag count
                              &tag,
                              &count,
                              &obj_res,
                              NULL);
  if (count == 0) {
    // the object does not exist or has been GCd
    g_jvmti->Deallocate((unsigned char*)obj_res);
    return NULL;
  } else {
    ASSERT_EQ(count, 1);

    jobject ret = *obj_res;
    g_jvmti->Deallocate((unsigned char*)obj_res);
    return ret;
  }
}

std::string getTypeForTag(JNIEnv *jni_env, jlong tag) {
  auto obj = getObjectForTag(jni_env, tag);
  if (obj != NULL) {
    return getTypeForObj(jni_env, obj);
  } else {
    return "N/A";
  }

}

void tagUntaggedClasses(JNIEnv *jni);

void printUntaggedClasses(JNIEnv *jni) {
  jclass *klasses;
  jint klasses_count;
  jvmtiError err = g_jvmti->GetLoadedClasses(&klasses_count, &klasses);
  ASSERT_NO_JVMTI_ERR(g_jvmti, err);
  for (jint i=0; i<klasses_count; ++i) {
    long tag;
    err = g_jvmti->GetTag(klasses[i], &tag);
    ASSERT_NO_JVMTI_ERR(g_jvmti, err);
    auto className = getClassName(jni, klasses[i]);
    if (tag == 0) {
      DBG("stubbornly untagged class: "<<className);
    } else {
      DBG("             tagged class: "<<className<<", tag = "<<tag);
    }
  }
}

std::map<const std::string, const jlong> klassReps;

jlong getClassRepTag(JNIEnv *jni, const std::string &className) {
  DBG("getClassRepTag(.., "<<className<<")")
  if (klassReps.find(className) == klassReps.end()) {
    tagUntaggedClasses(jni);
  }

  if (klassReps.find(className) == klassReps.end()) {
    long tag = -1 * nextObjID.fetch_add(1);
    WARN("can't find class rep tag for "<<className<<", synthesizing "<<tag);
    klassReps.insert({ { className, tag } });
  }
  return klassReps[className];
}

long getTag(JNIEnv *jni, jint objkind, jobject jobj, std::string klass) {
  jlong tag = 0;
  switch (objkind) {
  case NativeInterface_SPECIAL_VAL_NORMAL: {
    if (jobj) {
      jvmtiError err = g_jvmti->GetTag(jobj, &tag);
      DBG("getting tag (" << klass << " @ " << tag << ") from JVMTI");
      ASSERT_NO_JVMTI_ERR(g_jvmti, err);
    }
  }
    break;
  case NativeInterface_SPECIAL_VAL_THIS: {
    jobject obj = getThis();
    if (obj == NULL) {
      return NativeInterface_SPECIAL_VAL_JVM; //this is emitted too often
    } else {
      return getTag(jni, NativeInterface_SPECIAL_VAL_NORMAL, obj, klass);
    }
  }
  case NativeInterface_SPECIAL_VAL_STATIC: {
    tag = getClassRepTag(jni, klass);
    DBG("getting tag (" << klass << " @ " << tag << ") from classRep");
    break;
  }
  case NativeInterface_SPECIAL_VAL_NOT_IMPLEMENTED: {
    ERR("SPECIAL_VAL_NOT_IMPLEMENTED");
    break;
  }
  default:
    ERR("Can not get tag for object with kind "<<objkind);
  }

  return tag;
}

std::vector<long> irregularlyTagged;

long getOrDoTagNonNull(JNIEnv *jni, jint objkind, jobject jobj, std::string klass) {
  DBG("kind="<<kindToStr(objkind)<<", klass="<<klass<<", jobj="<<jobj);
  ASSERT(jobj != NULL || objkind == NativeInterface_SPECIAL_VAL_STATIC && "can't handle NULL references");
  jlong tag = getTag(jni, objkind, jobj, klass);
  if (tag == 0) {
    tag = doTag(g_jvmti, jobj);
    if (isInLivePhase()) {
      // LOCK;
      // WARN("irregularly tagged object #"<<tag//<<" : "<<getTypeForTag(jni, tag)
      //      <<" (as string: "<<getToString(jni, jobj)<<") in live phase");
    } else {
      irregularlyTagged.push_back(tag);
    }
    DBG("setting tag (" << klass << " @ " << tag << ") using doTag");
    ASSERT(tag != 0);
  }
  return tag;
}

long getOrDoTag(JNIEnv *jni, jint objkind, jobject jobj, std::string klass) {
  if (jobj == NULL && objkind == NativeInterface_SPECIAL_VAL_NORMAL) {
    return 0;
  } else {
    return getOrDoTagNonNull(jni, objkind, jobj, klass);
  }
}

struct SourceLoc {
  std::string file;
  int line;
};

SourceLoc getSourceLoc(jint depth) {
  SourceLoc ret;
  if (isInLivePhase()) {
    jmethodID callingMethod;
    jlocation callsite;

    jvmtiError err = g_jvmti->GetFrameLocation(NULL, //use current thread
                                               depth,    //get the frame above
                                               &callingMethod,
                                               &callsite);
    if (err == JVMTI_ERROR_NO_MORE_FRAMES) {
      ret.file = "<no more frames>";
      ret.line = -1;
    } else {
      ASSERT_NO_JVMTI_ERR(g_jvmti, err);

      jvmtiLineNumberEntry *lineNumbers;

      jint lineNumberEntryCount;
      err = g_jvmti->GetLineNumberTable(callingMethod, &lineNumberEntryCount, &lineNumbers);
      if (err != JVMTI_ERROR_ABSENT_INFORMATION &&
          err != JVMTI_ERROR_NATIVE_METHOD) {
        ASSERT_NO_JVMTI_ERR(g_jvmti, err);
        jint lineNumber = -1;
        for (int i=0; i<lineNumberEntryCount; ++i) {
          // DBG("is it "<<lineNumbers[i].start_location<<"?");
          if (lineNumbers[i].start_location > callsite) {
            // DBG("it is!");
            break;
          } else {
            lineNumber = lineNumbers[i].line_number;
          }
        }

        g_jvmti->Deallocate((unsigned char*)lineNumbers);

        jclass declaring_class;
        err = g_jvmti->GetMethodDeclaringClass(callingMethod, &declaring_class);
        ASSERT_NO_JVMTI_ERR(g_jvmti, err);
        char *callsitefile;
        err = g_jvmti->GetSourceFileName(declaring_class, &callsitefile);
        ASSERT_NO_JVMTI_ERR(g_jvmti, err);

        ret.file = callsitefile;
        g_jvmti->Deallocate((unsigned char*)callsitefile);
        ret.line = lineNumber;
      } else {
        ret.file = "<absent information>";
        ret.line = -1;
      }
    }
  } else {
    ret.file = "<not instrumented>";
    ret.line = -1;
  }

  return ret;
}

bool tagFreshlyInitialisedObject(JNIEnv *jni,
                                 jobject callee,
                                 std::string threadName) {
  DBG("tagFreshlyInitialisedObject(..., threadName = " << threadName << ")");
  ASSERT(g_jvmti);
  ASSERT(callee);
  if (getTag(jni, NativeInterface_SPECIAL_VAL_NORMAL, callee, "class/unknown") != 0) {
    return false;
  }

  jlong tag;
  DBG("thread " << threadName << ": don't have an object ID");
  tag = nextObjID.fetch_add(1);
  DBG("tagging freshly initialised object with id " << tag);
  jvmtiError err = g_jvmti->SetTag(callee, tag);
  ASSERT_NO_JVMTI_ERR(g_jvmti, err);
  return true;
}

void printStack() {
  for (jint i=1; i<=15; ++i) {
    SourceLoc loc = getSourceLoc(i);
    if (loc.file == "<no more frames>") {
      break;
    }
    std::cout << "frame #"<<i<<": "<<loc.file<<":"<<loc.line << std::endl;
  }
}

JNIEXPORT void JNICALL
Java_NativeInterface_methodEnter(JNIEnv *env, jclass nativeinterfacecls,
                                 jstring name, jstring signature,
                                 jstring calleeClass, jint calleeKind,
                                 jobject callee, jobjectArray args) {
#ifdef ENABLED
  REQUIRE_LIVE();
  LOCK;
  DBG("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");
  std::string threadName = getThreadName();
  std::string calleeClassStr;
  std::string nameStr = toStdString(env, name);

  calleeClassStr = toStdString(env, calleeClass);
  DBG("Java_NativeInterface_methodEnter: "<<calleeClassStr
      <<"::" << nameStr
      <<", thd:"<<threadName);

  long calleeTag;
  DBG("calleeclass= " << calleeClassStr);
  if (calleeKind == NativeInterface_SPECIAL_VAL_STATIC) {
    callee = NULL;
    calleeTag = getClassRepTag(env, calleeClassStr);
    calleeClassStr = toStdString(env, calleeClass);
  } else {
    calleeTag = getOrDoTagNonNull(env, calleeKind, callee, toStdString(env, calleeClass));
  }
  DBG("callee tag = " << calleeTag);
  DBG("calleeKind = " << calleeKind);
  DBG("thread nam = " << threadName);
  {
    capnp::MallocMessageBuilder outermessage;
    AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
    capnp::MallocMessageBuilder innermessage;
    MethodEnterEvt::Builder msgbuilder =
    innermessage.initRoot<MethodEnterEvt>();
    msgbuilder.setName(nameStr);
    msgbuilder.setSignature(toStdString(env, signature));
    msgbuilder.setCalleeclass(toStdString(env, calleeClass));
    msgbuilder.setCalleetag(calleeTag);

    if (isInLivePhase()) {
      SourceLoc loc = getSourceLoc(2);
      msgbuilder.setCallsitefile(loc.file);
      msgbuilder.setCallsiteline(loc.line);
    } else {
      msgbuilder.setCallsitefile("<not instrumented>");
      msgbuilder.setCallsiteline(-1);
    }

    msgbuilder.setThreadName(threadName);
    anybuilder.setMethodenter(msgbuilder.asReader());

    if (traceToDisk) {
      capnp::writeMessageToFd(capnproto_fd, outermessage);
    }
  }

  {
    if (args != NULL) {
      DBG("emitting args "<<toStdString(env, calleeClass)<<"::"<<nameStr);
      const jsize N_ARGS = env->GetArrayLength((jarray)args);
      //  std::cout << "N_ARGS=" << N_ARGS << "\n";
      for (jsize i = 0; i < N_ARGS; ++i) {
        DBG("arg #"<<(i+1));
        jobject arg = env->GetObjectArrayElement(args, i);
        if (arg == NULL) {
          continue;
        }
        //FIXME: the caller must be the the object in the stackframe above us!

        long tag = getOrDoTag(env, NativeInterface_SPECIAL_VAL_NORMAL, arg, "java/lang/Object");
        if (tag == 0) {
          const auto klass = getTypeForObj(env, arg);

          tagFreshlyInitialisedObject(env, arg, getThreadName());
          tag = getTag(env, NativeInterface_SPECIAL_VAL_NORMAL, arg, klass);
          ASSERT(tag != 0);

          {
            capnp::MallocMessageBuilder outermessage;
            AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
            capnp::MallocMessageBuilder innermessage;
            MethodEnterEvt::Builder msgbuilder = innermessage.initRoot<MethodEnterEvt>();

            innermessage.initRoot<MethodEnterEvt>();
            msgbuilder.setName("<init>");
            msgbuilder.setSignature("(<unknown>)V");
            msgbuilder.setCalleeclass(klass);
            msgbuilder.setCalleetag(tag);
            SourceLoc loc = getSourceLoc(2);
            msgbuilder.setCallsitefile(loc.file);
            msgbuilder.setCallsiteline(loc.line);
            msgbuilder.setThreadName(getThreadName().c_str());

            anybuilder.setMethodenter(msgbuilder.asReader());

            if (traceToDisk) {
              capnp::writeMessageToFd(capnproto_fd, outermessage);
            }
          }

          {
            doFramePop("<init>", calleeClassStr);
          }
        }

        capnp::MallocMessageBuilder outermessage;
        AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
        capnp::MallocMessageBuilder innermessage;
        VarStoreEvt::Builder msgbuilder = innermessage.initRoot<VarStoreEvt>();

        msgbuilder.setNewval(tag);
        msgbuilder.setOldval((int64_t)0);
        msgbuilder.setVar((int8_t)i);
        msgbuilder.setCallermethod(nameStr);
        // the callee of the call is the caller of the the var store:
        msgbuilder.setCallerclass(calleeClassStr);
        msgbuilder.setCallertag(calleeTag);
        msgbuilder.setThreadName(getThreadName());

        anybuilder.setVarstore(msgbuilder.asReader());
        if (traceToDisk) {
          capnp::writeMessageToFd(capnproto_fd, outermessage);
        }
      }
      DBG("emitting args done");
    }
  }
  DBG("methodEnter done");
#endif // ifdef ENABLED
}

JNIEXPORT void JNICALL
Java_NativeInterface_afterInitMethod(JNIEnv *env, jclass native_interface,
                                     jobject callee, jstring calleeClass) {
#ifdef ENABLED
  REQUIRE_LIVE();
  LOCK;
  std::string calleeClassStr = toStdString(env, calleeClass);
  std::string threadName = getThreadName();
  DBG("afterInitMethod(..., callee="<<callee<<", calleeClass=" << calleeClassStr << ")");
  tagFreshlyInitialisedObject(env, callee, threadName);
  DBG("afterInitMethod done")
#endif // ifdef ENABLED
    }

JNIEXPORT void JNICALL Java_NativeInterface_newObj(JNIEnv *, jclass, jobject,
                                                   jstring, jstring, jstring,
                                                   jobject, jobject) {
#ifdef ENABLED
  ERR("newObj not implemented");
#endif // ifdef ENABLED
}


void handleStoreFieldA(JNIEnv *env, jclass native_interface, jint holderKind, jobject holder,
                       jobject newval,
                       jobject oldval,
                       std::string holderClass,
                       std::string fname,
                       std::string type,
                       std::string callerClass,
                       std::string callerMethod, jint callerKind,
                       jobject caller) {
#ifdef ENABLED
  LOCK;
  DBG("Java_NativeInterface_storeFieldA "<<holderClass<<"::"<<fname);

  auto threadName = getThreadName();
  long callerTag = getOrDoTagNonNull(env, callerKind, caller, callerClass);
  DBG("getting holder tag, holderKind="<<holderKind);
  long holderTag = getOrDoTagNonNull(env, holderKind, holder, holderClass);
  DBG("getting oldval tag");
  long oldvaltag = getOrDoTag(env, NativeInterface_SPECIAL_VAL_NORMAL, oldval, type.c_str());
  DBG("getting newval tag");
  long newvaltag = getOrDoTag(env, NativeInterface_SPECIAL_VAL_NORMAL, newval, type.c_str());
  DBG("callertag ="<<callerTag);
  DBG("holdertag ="<<holderTag);
  DBG("newvaltag ="<<newvaltag);
  DBG("oldvaltag ="<<oldvaltag);
  DBG("callermthd="<<callerClass<<"::"<<callerMethod);

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  FieldStoreEvt::Builder msgbuilder = innermessage.initRoot<FieldStoreEvt>();
  msgbuilder.setHolderclass(holderClass);
  msgbuilder.setHoldertag(holderTag);
  msgbuilder.setFname(fname);
  msgbuilder.setType(type);
  msgbuilder.setNewval(newvaltag);
  msgbuilder.setOldval(oldvaltag);
  msgbuilder.setCallermethod(callerMethod);
  msgbuilder.setCallerclass(callerClass);
  msgbuilder.setCallertag(callerTag);
  msgbuilder.setThreadName(getThreadName());

  anybuilder.setFieldstore(msgbuilder.asReader());

  if (traceToDisk) {
    capnp::writeMessageToFd(capnproto_fd, outermessage);
  }
#endif // ifdef ENABLED
}

JNIEXPORT void JNICALL Java_NativeInterface_storeFieldA(JNIEnv *env, jclass native_interface, jint holderKind, jobject holder,
                                                        jobject newVal, jobject oldval, jstring _holderClass, jstring _fname,
                                                        jstring _type, jstring _callerClass, jstring _callerMethod, jint callerKind,
                                                        jobject caller) {
#ifdef ENABLED
  REQUIRE_LIVE();
  LOCK;
  std::string callerClass  = toStdString(env, _callerClass);
  std::string holderClass  = toStdString(env, _holderClass);
  std::string fname        = toStdString(env, _fname);
  std::string type         = toStdString(env, _type);
  std::string callerMethod = toStdString(env, _callerMethod);

  handleStoreFieldA(env, native_interface, holderKind, holder, newVal, oldval, holderClass, fname,
                    type, callerClass, callerMethod, callerKind, caller);
#endif // ifdef ENABLED
}

JNIEXPORT void JNICALL
Java_NativeInterface_storeVar(JNIEnv *env, jclass native_interface,
                              jint newValKind, jobject newVal, jint oldValKind,
                              jobject oldval, jint var, jstring callerClass,
                              jstring callerMethod, jint callerKind,
                              jobject caller) {
#ifdef ENABLED
  REQUIRE_LIVE();
  LOCK;
  DBG("Java_NativeInterface_storeVar");
  DBG("callerKind = "<<kindToStr(callerKind));
  auto threadName = getThreadName();

  ASSERT(oldValKind != NativeInterface_SPECIAL_VAL_STATIC);
  ASSERT(newValKind != NativeInterface_SPECIAL_VAL_STATIC);

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  VarStoreEvt::Builder msgbuilder = innermessage.initRoot<VarStoreEvt>();

  msgbuilder.setCallerclass(toStdString(env, callerClass));
  long newValTag = getTag(env, newValKind, newVal,
                          msgbuilder.asReader().getCallerclass().cStr());
  DBG("newValtag="<<newValTag);
  msgbuilder.setNewval(newValTag);
  msgbuilder.setOldval(0 /* this feature is not used in the instrumentation*/);
  msgbuilder.setVar((int8_t)var);
  msgbuilder.setCallermethod(toStdString(env, callerMethod));

  msgbuilder.setCallertag(getOrDoTagNonNull(env,
                                     callerKind, caller, msgbuilder.asReader().getCallerclass().cStr()));
  msgbuilder.setThreadName(getThreadName());

  anybuilder.setVarstore(msgbuilder.asReader());
  if (traceToDisk) {
    capnp::writeMessageToFd(capnproto_fd, outermessage);
  }
  DBG("storeVar done");
#endif // ifdef ENABLED
}

JNIEXPORT void JNICALL
Java_NativeInterface_loadVar(JNIEnv *env, jclass native_interface, jint valKind,
                             jobject val, jint var, jstring callerClass,
                             jstring callerMethod, jint callerKind,
                             jobject caller) {
#ifdef ENABLED
  REQUIRE_LIVE();
  LOCK;
  auto threadName = getThreadName();
  DBG("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");
  DBG("Java_NativeInterface_loadVar " << var << " in "
      << toStdString(env, callerClass) << "::"
      << toStdString(env, callerMethod));
  ASSERT(valKind != NativeInterface_SPECIAL_VAL_STATIC);

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  VarLoadEvt::Builder msgbuilder = innermessage.initRoot<VarLoadEvt>();
  long valTag = getTag(env, valKind, val,
                       "no/var/class/available");
  long callerTag =
    getOrDoTagNonNull(env, callerKind, caller,
               toStdString(env, callerClass));
  //getTag(env, NativeInterface_SPECIAL_VAL_NORMAL, caller,
  //       toStdString(env, callerClass));

  msgbuilder.setVal(valTag);
  DBG("valTag    = " << valTag);
  DBG("callerTag = " << callerTag);
  DBG("callerKind = " << kindToStr(callerKind));

  msgbuilder.setVar((char)var);
  msgbuilder.setCallermethod(toStdString(env, callerMethod));
  msgbuilder.setCallerclass(toStdString(env, callerClass));
  msgbuilder.setCallertag(callerTag);
  msgbuilder.setThreadName(getThreadName());
  anybuilder.setVarload(msgbuilder.asReader());

  if (traceToDisk) {
    capnp::writeMessageToFd(capnproto_fd, outermessage);
  }
#endif // ifdef ENABLED
}

void handleModify(JNIEnv *env, jclass native_interface,
                  jint calleeKind,
                  jobject callee,
                  std::string calleeClass,
                  std::string fname,
                  jint callerKind,
                  jobject caller,
                  std::string callerClass) {
#ifdef ENABLED
  LOCK;
  auto threadName = getThreadName();
  DBG("Java_NativeInterface_modify");

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  ReadModifyEvt::Builder msgbuilder = innermessage.initRoot<ReadModifyEvt>();
  msgbuilder.setIsModify(true);
  msgbuilder.setCalleeclass(calleeClass);
  msgbuilder.setCalleetag(getOrDoTagNonNull(env,
                                     calleeKind, callee, msgbuilder.asReader().getCalleeclass().cStr()));
  msgbuilder.setFname(fname);
  msgbuilder.setCallerclass(callerClass);
  msgbuilder.setCallertag(getOrDoTagNonNull(env,
                                     callerKind, caller, msgbuilder.asReader().getCallerclass().cStr()));
  msgbuilder.setThreadName(getThreadName());

  anybuilder.setReadmodify(msgbuilder.asReader());

  if (traceToDisk) {
    capnp::writeMessageToFd(capnproto_fd, outermessage);
  }
  DBG("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");

#endif // ifdef ENABLED
}

JNIEXPORT void JNICALL
Java_NativeInterface_modify(JNIEnv *env, jclass native_interface,
                            jint calleeKind, jobject callee,
                            jstring calleeClass, jstring fname, jint callerKind,
                            jobject caller, jstring callerClass) {
#ifdef ENABLED
  REQUIRE_LIVE();
  LOCK;
  handleModify(env, native_interface,
               calleeKind,
               callee,
               toStdString(env, calleeClass),
               toStdString(env, fname), callerKind,
               caller,
               toStdString(env, callerClass));
#endif // ifdef ENABLED

}

void handleRead(JNIEnv *env, jclass native_interface,
                jint calleeKind,
                jobject callee,
                std::string calleeClass,
                std::string fname,
                jint callerKind,
                jobject caller,
                std::string callerClass) {
#ifdef ENABLED
  REQUIRE_LIVE();
  LOCK;
  auto threadName = getThreadName();
  DBG("Java_NativeInterface_read");

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  ReadModifyEvt::Builder msgbuilder = innermessage.initRoot<ReadModifyEvt>();
  msgbuilder.setIsModify(false);
  msgbuilder.setCalleeclass(calleeClass);
  msgbuilder.setCalleetag(getOrDoTagNonNull(env,
                                     calleeKind, callee, msgbuilder.asReader().getCalleeclass().cStr()));
  msgbuilder.setFname(fname);
  msgbuilder.setCallerclass(callerClass);
  msgbuilder.setCallertag(getOrDoTagNonNull(env,
                                     callerKind, caller, msgbuilder.asReader().getCallerclass().cStr()));
  msgbuilder.setThreadName(getThreadName());

  anybuilder.setReadmodify(msgbuilder.asReader());

  if (traceToDisk) {
    capnp::writeMessageToFd(capnproto_fd, outermessage);
  }
#endif // ifdef ENABLED
}


JNIEXPORT void JNICALL
Java_NativeInterface_read(JNIEnv *env, jclass nativeInterface, jint calleeKind,
                          jobject callee, jstring calleeClass, jstring fname,
                          jint callerKind, jobject caller, jstring callerClass) {
#ifdef ENABLED
  REQUIRE_LIVE();
  LOCK;
  handleRead(env, nativeInterface, calleeKind, callee, toStdString(env, calleeClass), toStdString(env, fname), callerKind, caller, toStdString(env, callerClass));
#endif // ifdef ENABLED
}

void handleLoadFieldA(JNIEnv *env, jclass native_interface,
                      jobject val, jint holderKind, jobject holder,
                      std::string holderClass, std::string fname,
                      std::string type, std::string callerClass,
                      std::string callerMethod, jint callerKind,
                      jobject caller) {
#ifdef ENABLED
  DBG("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");
  DBG("Java_NativeInterface_loadFieldA");
  DBG("callerKind = "<<kindToStr(callerKind));
  auto threadName = getThreadName();

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  FieldLoadEvt::Builder msgbuilder = innermessage.initRoot<FieldLoadEvt>();
  msgbuilder.setHolderclass(holderClass);
  msgbuilder.setHoldertag(getOrDoTagNonNull(env,
                                     holderKind, holder, msgbuilder.asReader().getHolderclass().cStr()));
  msgbuilder.setFname(fname);
  msgbuilder.setType(type);
  msgbuilder.setVal(getTag(env, NativeInterface_SPECIAL_VAL_NORMAL, val,
                           msgbuilder.asReader().getType().cStr()));
  msgbuilder.setCallermethod(callerMethod);
  msgbuilder.setCallerclass(callerClass);
  msgbuilder.setCallertag(getTag(env,
                                 callerKind, caller, msgbuilder.asReader().getCallerclass().cStr()));
  msgbuilder.setThreadName(getThreadName());

  anybuilder.setFieldload(msgbuilder.asReader());

  if (traceToDisk) {
    capnp::writeMessageToFd(capnproto_fd, outermessage);
  }
  DBG("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
#endif // ifdef ENABLED
}

JNIEXPORT void JNICALL
Java_NativeInterface_loadFieldA(JNIEnv *env, jclass native_interface,
                                jobject val, jint holderKind, jobject holder,
                                jstring _holderClass, jstring _fname,
                                jstring _type, jstring _callerClass,
                                jstring _callerMethod, jint callerKind,
                                jobject caller) {
#ifdef ENABLED
  REQUIRE_LIVE();
  LOCK;
  std::string holderClass = toStdString(env, _holderClass);
  std::string fname = toStdString(env, _fname);
  std::string type = toStdString(env, _type);
  std::string callerClass = toStdString(env, _callerClass);
  std::string callerMethod = toStdString(env, _callerMethod);

  handleLoadFieldA(env, native_interface, val, holderKind, holder, holderClass, fname, type, callerClass, callerMethod, callerKind, caller);
#endif // ifdef ENABLED
}

void JNICALL VMObjectAlloc(jvmtiEnv *jvmti_env, JNIEnv *env, jthread threadName,
                           jobject object, jclass object_klass, jlong size) {
#ifdef ENABLED
  ERR("not prepared for this");
#endif
}

/*
  Sent by the VM when a class is being loaded into the VM

  Transforms loaded classes, if the VM is initialized and if loader!=NULL

*/
void JNICALL
ClassFileLoadHook(jvmtiEnv *jvmti_env, JNIEnv *jni,
                  jclass class_being_redefined, jobject loader,
                  const char *_name, jobject protection_domain,
                  jint class_data_len, const unsigned char *class_data,
                  jint *new_class_data_len, unsigned char **new_class_data) {
#ifdef ENABLED
  LOCK;
  ASSERT(class_data);

  std::string name;
  if (_name) {
    name = _name;
  } else {
    // Names can be empty. Weird!
    // http://docs.oracle.com/javase/1.5.0/docs/guide/jvmti/jvmti.html#ClassFileLoadHook
    name = "";
  }
  DBG("ClassFileLoadHook: '" << name << "'");
  DBG("inserting "<<name);
  klassReps.insert({{name, (-1)*nextObjID.fetch_add(1)}});

  if (name == "java/lang/invoke/LambdaForm") {
    //these classes don't really exist, so they'll never be loaded. We'll give them
    //IDs anyway!
    klassReps.insert({{ "java/lang/invoke/LambdaForm$MH", (-1)*nextObjID.fetch_add(1)}});
    klassReps.insert({{ "java/lang/invoke/LambdaForm$BMH", (-1)*nextObjID.fetch_add(1)}});
    klassReps.insert({{ "java/lang/invoke/LambdaForm$DMH", (-1)*nextObjID.fetch_add(1)}});
    klassReps.insert({{ "java/lang/invoke/LambdaForm$LFI", (-1)*nextObjID.fetch_add(1)}});
    klassReps.insert({{ "java/lang/invoke/LambdaForm$NFI", (-1)*nextObjID.fetch_add(1)}});
  }

  if (class_being_redefined != NULL) {
    //this is a call from RedefineClass. The class has been transformed already;
    DBG("class '"<<name<<"' has been redefined -- len="<<class_data_len);
    return;
  }

  if (name == "NativeInterface"
      || name == "sun/reflect/generics/repository/ClassRepository" // http://www.docjar.com/docs/api/sun/reflect/generics/repository/ClassRepository.html: [the class] is designed to be used unchanged by at least core reflection and JDI.
      || name == "sun/misc/Launcher$AppClassLoader"
      || name == "java/lang/Object" // there seem to be objects that generate an exit in Object::<init> but don't generate a matching enter..
      ) {
    return;
  }

  if (!isInLivePhase()) {
    //check whether the class is marked as 'tricky'. If so, it should not be transformed now, but
    //redefined later.
    for (auto it = onlyDuringLivePhaseMatch.begin(); it != onlyDuringLivePhaseMatch.end(); ++it) {
      auto res = std::mismatch(it->begin(), it->end(), name.begin());

      if (true || res.first == it->end()) {
        DBG("postponing transformation of class "<<name<<" -- len="<<class_data_len<< ", due to match with "<<*it);
        // match string is a prefix of class name
        SpencerClassRedefinition redef;
        redef.name = name;
        redef.klassDef.klass = NULL;
        redef.klassDef.class_byte_count = class_data_len;
        ASSERT(class_data_len > 0);
        unsigned char *class_data_cpy = (unsigned char*)malloc((unsigned long)class_data_len);
        memcpy(class_data_cpy, class_data, (unsigned long)class_data_len);
        redef.klassDef.class_bytes = class_data_cpy;
        redefineDuringLivePhase.push_back(redef);
        return;
      }
    }
  }

  jvmtiPhase phase;
  if (jvmti_env->GetPhase(&phase) != JVMTI_ERROR_NONE) {
    ERR("can't get phase");
  }
  DBG("phase  = " << phase);
  DBG("loader = " << loader);
  DBG("g_init = " << g_init);

  DBG("instrumenting class " << name);
  transformClass(class_data, (uint32_t)class_data_len, new_class_data, (uint32_t*)new_class_data_len);

  int minLen = *new_class_data_len < class_data_len ? *new_class_data_len : class_data_len;
  ASSERT(minLen > 0)
    if ((class_data_len != *new_class_data_len) ||
        (memcmp(class_data, *new_class_data, (unsigned long)minLen) != 0)) {
      DBG("class "<<name<<" is instrumented: got changed class back");
    } else {
      DBG("class "<<name<<" is not instrumented: got unchanged class back");
    }

  // unsigned char **new_class_data_ignore;
  // recvClass(sock, new_class_data_ignore);
  //  */
  DBG("done");
#endif // ifdef ENABLED
}

struct FieldDescr {
  std::string name;
  long val;
};

std::vector<FieldDescr> getFieldsForTag(jvmtiEnv *env, JNIEnv *jni, long tag) {
  auto niKlass = jni->FindClass("NativeInterface");
  //jni->ExceptionDescribe();
  ASSERT(niKlass != NULL);

  jmethodID midGetFields = jni->GetStaticMethodID(niKlass, "getAllFieldsArrHelper", "(Ljava/lang/Object;)[Ljava/lang/Object;");
  jni->ExceptionDescribe();
  ASSERT(midGetFields != NULL);

  std::vector<FieldDescr> ret;

  jobjectArray fields = (jobjectArray)jni->CallStaticObjectMethod(niKlass, midGetFields, getObjectForTag(jni, tag));
  if (fields != NULL) {
    ASSERT(fields != NULL);
    jsize len = jni->GetArrayLength(fields);
    DBG(getTypeForTag(jni, tag)<<" has "<<(len/2)<<" fields");

    for (int i=0; i<len/2; ++i) {
      FieldDescr fd;
      jstring fieldName = (jstring)jni->GetObjectArrayElement(fields, (jsize)(i*2));
      jobject fieldVal = (jobject)jni->GetObjectArrayElement(fields, (jsize)(i*2+1));
      fd.name = toStdString(jni, fieldName);
      fd.val = getTag(jni, NativeInterface_SPECIAL_VAL_NORMAL, fieldVal, "java/lang/Object");

      /*
      if (fd.val == 0 && fieldVal != NULL) {
        WARN("have untagged fieldVal");
      }
      */

      ret.push_back(fd);
    }

  } else {
    DBG("got null for "<<getTypeForTag(jni, tag));
  }
  return ret;
}

jvmtiIterationControl JNICALL handleUntaggedKlass(jlong class_tag,
                                                   jlong size,
                                                   jlong *tag_ptr,
                                                   void *user_data) {
  std::vector<long> *freshlyTagged = (std::vector<long>*)user_data;
  *tag_ptr = -1 * nextObjID.fetch_add(1);
  freshlyTagged->push_back(*tag_ptr);
  return JVMTI_ITERATION_CONTINUE;
}
jvmtiIterationControl JNICALL handleUntaggedObject(jlong class_tag,
                                                   jlong size,
                                                   jlong *tag_ptr,
                                                   void *user_data) {
  std::vector<long> *freshlyTagged = (std::vector<long>*)user_data;
  *tag_ptr = nextObjID.fetch_add(1);
  DBG("tagging untagged obj with "<<*tag_ptr);
  freshlyTagged->push_back(*tag_ptr);
  return JVMTI_ITERATION_CONTINUE;
}

void tagUntaggedClasses(JNIEnv *jni) {
  DBG("tagging untagged classes..");
  jclass *klasses;
  jint klasses_count;
  jvmtiError err = g_jvmti->GetLoadedClasses(&klasses_count, &klasses);
  ASSERT_NO_JVMTI_ERR(g_jvmti, err);
  for (jint i=0; i<klasses_count; ++i) {
    long tag;
    err = g_jvmti->GetTag(klasses[i], &tag);
    ASSERT_NO_JVMTI_ERR(g_jvmti, err);

    auto className = getClassName(jni, klasses[i]);

    if (tag == 0) {
      tag = klassReps[className];
      if (tag == 0) {
        tag = -1 * nextObjID.fetch_add(1);
        klassReps.insert({ { className, tag } });
      }
      err = g_jvmti->SetTag(klasses[i], tag);
      ASSERT_NO_JVMTI_ERR(g_jvmti, err);
      DBG("tagged class: "<<className<<" as "<<tag);
    }
  }
}

/*
  The VM initialization event signals the completion of
  VM initialization.
  */
void JNICALL VMInit(jvmtiEnv *env, JNIEnv *jni, jthread threadName) {
#ifdef ENABLED
  LOCK;
  DBG("VMInit");
  {
    // tag objects that have not been tagged yet!

    // We start by iterating over all untagged classes, giving them negative indices.
    // We then iterate over all other untagged objects and give them positive indices.
    tagUntaggedClasses(jni);
    std::vector<long> freshlyTagged;

    jvmtiError err =
      env->IterateOverHeap(JVMTI_HEAP_OBJECT_UNTAGGED,
                           handleUntaggedObject,
                           &freshlyTagged);
    ASSERT_NO_JVMTI_ERR(env, err);
    DBG("got "<<freshlyTagged.size()<<" untagged objects");

    DBG("late initialising "<<freshlyTagged.size() <<" objects");
    for (auto it = freshlyTagged.begin(); it != freshlyTagged.end(); ++it) {
      DBG("freshlyTagged: "<<*it);
      capnp::MallocMessageBuilder outermessage;
      AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
      capnp::MallocMessageBuilder innermessage;
      LateInitEvt::Builder msgbuilder =
        innermessage.initRoot<LateInitEvt>();
      msgbuilder.setCalleetag(*it);
      auto klass = getTypeForTag(jni, *it);
      if (klass == "N/A") {
        continue;
      }
      msgbuilder.setCalleeclass(klass);

      auto fields = getFieldsForTag(env, jni, *it);

      auto fieldsBuilder = msgbuilder.initFields((unsigned int)fields.size());

      unsigned int i=0;
      for (auto it = fields.begin(); it != fields.end(); ++it) {
        fieldsBuilder[i].setName(it->name);
        fieldsBuilder[i].setVal(it->val);
        ++i;
      }

      anybuilder.setLateinit(msgbuilder.asReader());

      if (traceToDisk) {
        capnp::writeMessageToFd(capnproto_fd, outermessage);
      }
    }
    DBG("late initialising "<<freshlyTagged.size() <<" objects done");
  }

  g_init = true;

  {
    // redefine classes that we could not transform during the primordial
    // phase:
    for (auto redef = redefineDuringLivePhase.begin(); redef != redefineDuringLivePhase.end(); ++redef) {
      if (//redef->name == "java/lang/SystemClassLoaderAction" ||
          redef->name == "java/lang/Class" ||
          strstr(redef->name.c_str(), "java/lang/invoke/")
          //          || redef->name == "sun/reflect/Reflection"
          ) {
        DBG("never instrumenting "<<redef->name);
        continue;
      }
      DBG("redefining klass "<<redef->name);
      unsigned char *new_class_data;
      uint32_t new_class_data_len;
      transformClass(redef->klassDef.class_bytes, (uint32_t)redef->klassDef.class_byte_count, &new_class_data, &new_class_data_len);
      free((void*)redef->klassDef.class_bytes);
      redef->klassDef.class_bytes = new_class_data;
      redef->klassDef.class_byte_count = (jint)new_class_data_len;
      redef->klassDef.klass = jni->FindClass(redef->name.c_str());
      DBG("redefining class "<<redef->name<<" -- len="<<redef->klassDef.class_byte_count);
      jvmtiError err = g_jvmti->RedefineClasses(1, &redef->klassDef);
      if (err == JVMTI_ERROR_INVALID_CLASS) {
        WARN("could not redefine class "<<redef->name<<" (JVMTI_ERROR_INVALID_CLASS)");
      } else {
        ASSERT_NO_JVMTI_ERR(g_jvmti, err);
      }
    }
  }

#endif // ENABLED
}

void JNICALL VMDeath(jvmtiEnv *jvmti_env, JNIEnv *jni_env) {
#ifdef ENABLED
  DBG("VMDeath");
  g_dead = true;
  if (traceToDisk) {
    close(capnproto_fd);
  }

  for (auto const& it : klassReps) {
    DBG("klass "<<it.first<<" -> "<<it.second);
  }
#endif // ifdef ENABLED
}

void parse_options(std::string options) {
  //std::cout << "options: " << options << "\n";
  DBG("options="<<options);
  if (strstr(options.c_str(), "application_only")) {
    flag_application_only = true;
    ERR("DEPRECATED FLAG: application_only");
  }
  size_t p1 = options.find("tracefile=");
  if (p1 != string::npos) {
    std::string rest = options.substr(p1 + std::string("tracefile=").size());
    size_t p2 = rest.find(",");
    if (p2 != string::npos) {
      tracefilename = options.substr(p1, p2);
    } else {
      tracefilename = rest;
    }
  }

  if (tracefilename != "none") {
    traceToDisk = true;
  }
}

/*
  This method is invoked by the JVM early in it's
  initialization. No classes have been loaded and no
  objects created.

  Tries to set capabilities and callbacks for the agent
  If something goes wrong it will cause the JVM to
  disrupt the initialization.

  Return values:
  JNI_OK -> JVM will continue
  JNI_ERR -> JVM will disrupt initialization
*/
JNIEXPORT jint JNICALL Agent_OnLoad(JavaVM *vm, char *options, void *reserved) {
  DBG("Agent_OnLoad");

  {
    //    onlyDuringLivePhaseMatch.push_back("java/io/File");
    //    onlyDuringLivePhaseMatch.push_back("java/io/FileOutputStream");
    //onlyDuringLivePhaseMatch.push_back("java/io/PrintStream");
    //    onlyDuringLivePhaseMatch.push_back("java/lang/AbstractStringBuilder");

    {
      // these need to be disabled because they're used when switching to the live phase
      onlyDuringLivePhaseMatch.push_back("java/lang/reflect/Field");
      onlyDuringLivePhaseMatch.push_back("sun/reflect/");
    }

    onlyDuringLivePhaseMatch.push_back("java/lang/StringBuilder");
    onlyDuringLivePhaseMatch.push_back("java/lang/Class");
    onlyDuringLivePhaseMatch.push_back("java/lang/ClassLoader");
    onlyDuringLivePhaseMatch.push_back("java/util/AbstractCollection");
    onlyDuringLivePhaseMatch.push_back("java/util/AbstractList");
    //    onlyDuringLivePhaseMatch.push_back("java/lang/Float");
    onlyDuringLivePhaseMatch.push_back("java/lang/Object");
    onlyDuringLivePhaseMatch.push_back("java/lang/Shutdown");
    onlyDuringLivePhaseMatch.push_back("java/lang/String");
    onlyDuringLivePhaseMatch.push_back("java/lang/System");
    onlyDuringLivePhaseMatch.push_back("java/lang/Thread");
    onlyDuringLivePhaseMatch.push_back("java/lang/ref");
    onlyDuringLivePhaseMatch.push_back("java/security/AccessControlContext");
    //onlyDuringLivePhaseMatch.push_back("java/util/Arrays");
    // arst arst arst onlyDuringLivePhaseMatch.push_back("java/util/HashMap");
    onlyDuringLivePhaseMatch.push_back("java/util/Hashtable");
    onlyDuringLivePhaseMatch.push_back("java/util/LinkedList$ListItr");
    onlyDuringLivePhaseMatch.push_back("java/util/Locale/");
    onlyDuringLivePhaseMatch.push_back("java/util/Vector");
    onlyDuringLivePhaseMatch.push_back("sun/launcher/");
    onlyDuringLivePhaseMatch.push_back("sun/reflect/");
    onlyDuringLivePhaseMatch.push_back("sun/nio/cs");
    onlyDuringLivePhaseMatch.push_back("sun/util/PreHashedMap");
  }

  jvmtiError error;
  jint res;

  if (options != NULL) {
    parse_options(options);
  }
  if (traceToDisk) {
    capnproto_fd = open(tracefilename.c_str(), O_CREAT | O_WRONLY | O_TRUNC,
                        S_IRUSR | S_IWUSR);
    if (capnproto_fd == -1) {
      ERR("could not open log file '" << tracefilename << "'");
    }
  }

  // Get jvmti env
  res = vm->GetEnv((void **)&g_jvmti, JVMTI_VERSION);
  if (res != JNI_OK) {
    printf("ERROR GETTING JVMTI");
    return JNI_ERR;
  }

  // Set capabilities
  jvmtiCapabilities capabilities;
  memset(&capabilities, 0, sizeof(jvmtiCapabilities));
  capabilities.can_generate_all_class_hook_events = 1;
  capabilities.can_tag_objects = 1;
  capabilities.can_access_local_variables = 1;
  capabilities.can_generate_object_free_events = 1;
  capabilities.can_generate_vm_object_alloc_events = 1;
  capabilities.can_generate_exception_events = 1;
  capabilities.can_redefine_classes = 1;
  capabilities.can_redefine_any_class = 1;
  capabilities.can_get_line_numbers = 1;
  capabilities.can_get_source_file_name = 1;
  error = g_jvmti->AddCapabilities(&capabilities);
  ASSERT_NO_JVMTI_ERR(g_jvmti, error);

  // Set callbacks
  jvmtiEventCallbacks callbacks;
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.VMInit = &VMInit;
  callbacks.VMDeath = &VMDeath;
  callbacks.ClassFileLoadHook = &ClassFileLoadHook;
  callbacks.VMObjectAlloc = &VMObjectAlloc;
  error = g_jvmti->SetEventCallbacks(&callbacks, sizeof(callbacks));
  ASSERT_NO_JVMTI_ERR(g_jvmti, error);

  error = g_jvmti->SetEventNotificationMode(JVMTI_ENABLE, JVMTI_EVENT_VM_INIT,
                                            NULL);
  ASSERT_NO_JVMTI_ERR(g_jvmti, error);
  error = g_jvmti->SetEventNotificationMode(JVMTI_ENABLE, JVMTI_EVENT_VM_DEATH,
                                            NULL);
  ASSERT_NO_JVMTI_ERR(g_jvmti, error);

  error = g_jvmti->SetEventNotificationMode(
                                            JVMTI_ENABLE, JVMTI_EVENT_CLASS_FILE_LOAD_HOOK, NULL);
  ASSERT_NO_JVMTI_ERR(g_jvmti, error);
  error = g_jvmti->SetEventNotificationMode(JVMTI_ENABLE,
                                            JVMTI_EVENT_OBJECT_FREE, NULL);
  ASSERT_NO_JVMTI_ERR(g_jvmti, error);

  error = g_jvmti->CreateRawMonitor((char *)"Callbacks Lock", &g_lock);
  ASSERT_NO_JVMTI_ERR(g_jvmti, error);

  {
    DBG("extending bootstrap classloader search");
    //FIXME I think we don't need ./ any longer:
    error = g_jvmti->AddToBootstrapClassLoaderSearch("./");
    ASSERT_NO_JVMTI_ERR(g_jvmti, error); // make NativeInterface.class visible

    char *home;
    if ((home = getenv("HOME")) == NULL) {
      home = getpwuid(getuid())->pw_dir;

    }

    //FIXME: hard coded version number
    std::string jar = std::string(home)+"/.m2/repository/com/github/kaeluka/spencer-tracing/0.1.3-SNAPSHOT/spencer-tracing-0.1.3-SNAPSHOT-events.jar";

    error = g_jvmti->AddToBootstrapClassLoaderSearch(jar.c_str());
    ASSERT_NO_JVMTI_ERR(g_jvmti, error); // make NativeInterface.class visible
  }

  DBG("extending bootstrap classloader search: done");

  return JNI_OK;
}

/*
  This function is invoked by the JVM just before it unloads
  exports the events.
*/
JNIEXPORT void JNICALL Agent_OnUnload(JavaVM *vm) {
  DBG("Agent_OnUnload");
#ifdef ENABLED
  {
    //  int cnt = 0;
    //  for (auto it = instrumentedClasses.begin(); it != instrumentedClasses.end();
    //       ++it) {
    //    DBG("instrumented " << ++cnt << "/" << instrumentedClasses.size()
    //                        << ": class " << *it);
    //  }
    //  int cnt = 0;
    //  for (auto it = uninstrumentedClasses.begin();
    //       it != uninstrumentedClasses.end(); ++it) {
    //     DBG("uninstrumented "<<++cnt<<"/"<<uninstrumentedClasses.size()
    //                       <<": class "<<*it);
    //  }
    DBG("instrumented "<<instrumentedClasses.size()<<" classes, skipped "<<uninstrumentedClasses.size());
  }
#endif // ifdef ENABLED
}
