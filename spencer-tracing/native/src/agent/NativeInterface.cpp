#define ENABLED

//#define NODEBUG
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

using namespace std;

/*******************************************************************/
/* Global Data                                                     */
/*******************************************************************/

int capnproto_fd;
#define SOCKET_PORT "1345"

std::string getTracefileName() {
  /*
    std::shared_ptr<FILE> pipe(popen("./getBenchmarkDrive.sh", "r"), pclose);
    if (!pipe) return "ERROR";
    char buffer[128];
    std::string result = "";
    while (!feof(pipe.get())) {
      if (fgets(buffer, 128, pipe.get()) != NULL) {
        result += buffer;
      }
    }
    result.erase(std::remove(result.begin(), result.end(), '\n'), result.end());
    return result+"/prototracefile.log";
  */
  return "/tmp/tracefile";
}

std::string tracefilename = getTracefileName();

// global ref to jvmti enviroment
static jvmtiEnv *g_jvmti = NULL;
static bool flag_application_only = false;

// indicates JVM initialization
static bool g_init = false;
// indicates JVM death
static bool g_dead = false;

jvmtiError g_jvmtiError;

// true if the application's class loader has been active
static bool g_had_loader = false;

// true if the application's class
// loader has JUST gotten active and
// main hasn't been called yet:
static bool g_awaiting_main_call = false;

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

void doFramePop(std::string mname) {
  std::string threadName = getThreadName(g_jvmti, g_lock);//getOrDoTag(NativeInterface_SPECIAL_VAL_NORMAL, thread,
  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  MethodExitEvt::Builder msgbuilder = innermessage.initRoot<MethodExitEvt>();
                   //           "java/lang/Thread", thread, g_jvmti, g_lock);
  msgbuilder.setThreadName(threadName);
  msgbuilder.setName(mname);
  ASSERT(std::string("") != msgbuilder.getName().cStr());
  anybuilder.setMethodexit(msgbuilder.asReader());

  //temporary  popStackFrame(threadName);

  capnp::writeMessageToFd(capnproto_fd, outermessage);
}

bool isInLivePhase() {
  jvmtiPhase phase;
  jvmtiError err = g_jvmti->GetPhase(&phase);
  ASSERT_NO_JVMTI_ERR(g_jvmti, err);
  return (phase == JVMTI_PHASE_LIVE);
}

#define SKIP_BEFORE_LIVE {if (!isInLivePhase()) {return;} }

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

uint32_t recvClass(int sock, unsigned char **data) {
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

  long len = 0;
  for (int i = 0; i <= 7; ++i) {
    len += (x.bs[i] << (8 * (7 - i)));
  }

  DBG("received length " << len);

  *data = (unsigned char *)calloc(len, sizeof(unsigned char));
  rlen = recv(sock, *data, len * sizeof(unsigned char), MSG_WAITALL);
  DBG("rlen=" << rlen);
  ASSERT(rlen == len);
  closeSocket(sock);
  return rlen;
}

void sendClass(int sock, const unsigned char *data, uint32_t len,
               std::string name) {
  DBG("sending class " << name << ", length = " << len);
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
    stringstream field;
    field<<"_"<<idx;

    handleRead(env, nativeInterface,
               NativeInterface_SPECIAL_VAL_NORMAL,
               arr,
               "[no_arr_class_yet",
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
    stringstream field;
    field<<"_"<<idx;

    handleModify(env, nativeInterface,
                 NativeInterface_SPECIAL_VAL_NORMAL,
                 arr,
                 "[no_arr_class_yet",
                 field.str(),
                 callerValKind,
                 caller,
                 toStdString(env, callerClass));

}


JNIEXPORT void JNICALL
Java_NativeInterface_methodExit(JNIEnv *env, jclass, jstring _mname, jstring _cname) {
#ifdef ENABLED
  SKIP_BEFORE_LIVE;
  LOCK;
  const auto mname = toStdString(env, _mname);
  const auto cname = toStdString(env, _cname);
  //temporary  if (! isClassInstrumented(cname)) {
  //temporary    DODBG("NOT POPPING FRAME DUE TO CLASS "<<cname<<" BEING UNINSTRUMENTED");
  //temporary    return;
  //temporary  }
  ASSERT(mname != "");
  DBG("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");
  DBG("Java_NativeInterface_methodExit:  ...::"<<mname<<"), thd: "<<getThreadName(g_jvmti, g_lock));
  ASSERT(mname != "ClassRep");
  doFramePop(mname);
  DBG("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");

#endif
}

JNIEXPORT void JNICALL
Java_NativeInterface_methodEnter(JNIEnv *env, jclass nativeinterfacecls,
                                 jstring name, jstring signature,
                                 jstring calleeClass, jint calleeKind,
                                 jobject callee, jobjectArray args) {
#ifdef ENABLED
  SKIP_BEFORE_LIVE;
  LOCK;
  DBG("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");
  std::string threadName = getThreadName(g_jvmti, g_lock);
  std::string calleeClassStr = toStdString(env, calleeClass);
  DBG("Java_NativeInterface_methodEnter: "<<calleeClassStr
                                          <<"::" << toStdString(env, name)
                                          <<", thd:"<<threadName);
  pushStackFrame(env, threadName, calleeKind, callee, name, calleeClass, g_jvmti,
                 g_lock);

  long calleeTag;
  if (calleeKind == NativeInterface_SPECIAL_VAL_STATIC) {
    callee = NULL;
    calleeTag = getClassRepTag(calleeClassStr);
  } else {
    calleeTag = getTag(calleeKind, callee, toStdString(env, calleeClass),
                            threadName, g_jvmti);
  }
  //handleValStatic(env, &calleeKind, &callee, calleeClass);
  DBG("callee tag = " << calleeTag);
  DBG("thread tag = " << threadName);
  if (!flag_application_only) {
    // force calling init on the strings (which the JVM did for us
    // in uninstrumented code).
    jstring initName = env->NewStringUTF("<init>");
    jstring init_sig = env->NewStringUTF("()V");
    jstring string_class_name = env->NewStringUTF("java/lang/String");
    jclass string_class = env->FindClass("java/lang/String");

    if (g_awaiting_main_call && toStdString(env, name) == "main") {
      g_awaiting_main_call = false;

      ASSERT(string_class);
      jobjectArray init_args = env->NewObjectArray(0, string_class, NULL);
      ASSERT(initName && init_sig && init_args);
      const jsize N_ARGS = env->GetArrayLength((jobjectArray)args);

      for (jsize i = 0; i < N_ARGS; i++) {
        jstring arg = (jstring)env->GetObjectArrayElement(args, i);
        DBG("got arg " << arg);
        // DBG("pushing artificial <init> stack frame for arg "
        //    << toStdString(env, arg));
        Java_NativeInterface_methodEnter(
            env, nativeinterfacecls, initName, init_sig, string_class_name,
            NativeInterface_SPECIAL_VAL_NORMAL, arg, init_args);
        ASSERT_EQ(toStdString(env, initName), "<init>");
        doFramePop(toStdString(env, initName));

        //env->ReleaseObjectArrayElements(init_args);
      }
    }
  }
  {
    capnp::MallocMessageBuilder outermessage;
    AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
    capnp::MallocMessageBuilder innermessage;
    MethodEnterEvt::Builder msgbuilder =
        innermessage.initRoot<MethodEnterEvt>();
    msgbuilder.setName(toStdString(env, name));
    msgbuilder.setSignature(toStdString(env, signature));
    msgbuilder.setCalleeclass(toStdString(env, calleeClass));
    msgbuilder.setCalleetag(calleeTag);
    msgbuilder.setThreadName(threadName);

    anybuilder.setMethodenter(msgbuilder.asReader());

    // stacks[getOrDoTag(SPECIAL_VAL_NORMAL, thread,
    // "java/lang/Thread",
    //                  thread)]
    //    .push(std::make_pair(msgbuilder.asReader().getCalleetag(),
    //                         msgbuilder.asReader().getName()));

    // notify us once we exit the method (the method is in the frame
    // above the current one, as the current one is the callback):
    capnp::writeMessageToFd(capnproto_fd, outermessage);
    if (toStdString(env, calleeClass).c_str()[0] == '[') {
      // it's an array, which means that this ctor is fake and we need to simulate the frame pop!
      jstring initName = env->NewStringUTF("<init>");
      auto initNameStr = toStdString(env, initName);
      ASSERT_EQ(initNameStr, "<init>");
      doFramePop(initNameStr);
      // we do NOT call afterInitMethod, since the instrumenter emits this event.
      // afterInitMethod needs the actual reference to the array, we only have SPECIAL_VAL_THIS/0x0
    }
  }

  {
    if (args != NULL) {
      DBG("emitting args "<<toStdString(env, calleeClass)<<"::"<<toStdString(env, name));
      const jsize N_ARGS = env->GetArrayLength((jarray)args);
      //  std::cout << "N_ARGS=" << N_ARGS << "\n";
      for (jsize i = 0; i < N_ARGS; ++i) {
        DBG("arg #"<<(i+1));
        jobject arg = env->GetObjectArrayElement(args, i);
        if (arg == NULL) {
          continue;
        }
        capnp::MallocMessageBuilder outermessage;
        AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
        capnp::MallocMessageBuilder innermessage;
        VarStoreEvt::Builder msgbuilder = innermessage.initRoot<VarStoreEvt>();

        //FIXME: the caller must be the the object in the stackframe above us!

        msgbuilder.setNewval(getOrDoTag(NativeInterface_SPECIAL_VAL_NORMAL,
          arg,
          "java/lang/Object", threadName, g_jvmti,
          g_lock));
          msgbuilder.setOldval((int64_t)0);
          msgbuilder.setVar(i);
          msgbuilder.setCallermethod(toStdString(env, name));
          // the callee of the call is the caller of the the var store:
          msgbuilder.setCallerclass(toStdString(env, calleeClass));
          msgbuilder.setCallertag(calleeTag);
          msgbuilder.setThreadName(getThreadName(g_jvmti, g_lock));
          msgbuilder.setVar(i);

          anybuilder.setVarstore(msgbuilder.asReader());
          capnp::writeMessageToFd(capnproto_fd, outermessage);
        }
        DBG("emitting args done");
      }
} //*/
#endif // ifdef ENABLED
}

JNIEXPORT void JNICALL
Java_NativeInterface_afterInitMethod(JNIEnv *env, jclass native_interface,
                                     jobject callee, jstring calleeClass) {
#ifdef ENABLED
  SKIP_BEFORE_LIVE;
  LOCK;
  DBG("afterInitMethod");
  std::string calleeClassStr = toStdString(env, calleeClass);
  std::string threadName = getThreadName(g_jvmti, g_lock);
  const auto &runningClass = getRunningClass(threadName);
  //  if (!isClassInstrumented(runningClass) && runningClass!="JvmInternals") {
  //    ERR("class "<<runningClass<<" was instrumented for java, but not for native code!");
  //    return;
  //  }
  DBG("afterInitMethod(..., callee="<<callee<<", calleeClass=" << calleeClass << ")");

  if (calleeClassStr == "ClassRep" || isClassInstrumented(calleeClassStr)) {
    DBG("class "<<calleeClass<<" is instrumented");
    tagFreshlyInitialisedObject(g_jvmti, callee, getThreadName(g_jvmti, g_lock));
  } else {
    DBG("class "<<calleeClass<<" is not instrumented");
  }
#endif // ifdef ENABLED
}

JNIEXPORT void JNICALL Java_NativeInterface_newObj(JNIEnv *, jclass, jobject,
                                                   jstring, jstring, jstring,
                                                   jobject, jobject) {
#ifdef ENABLED
  SKIP_BEFORE_LIVE;

// LOCK;
// DBG("newObj");
#endif
}

void handleStoreFieldA(
    JNIEnv *env, jclass native_interface, jint holderKind, jobject holder,
    jobject newval,
    jobject oldval,
    std::string holderClass,
    std::string fname,
    std::string type,
    std::string callerClass,
    std::string callerMethod, jint callerKind,
    jobject caller) {
#ifdef ENABLED
  SKIP_BEFORE_LIVE;

  LOCK;
  DBG("Java_NativeInterface_storeFieldA "<<holderClass<<"::"<<fname);

//  handleValStatic(env, &holderKind, &holder, holderClass);
auto threadName = getThreadName(g_jvmti, g_lock);
//handleValStatic(env, &callerKind, &caller, callerClass);
//handleValStatic(env, &holderKind, &holder, holderClass);
DBG("getting caller tag");
long callerTag = getTag(callerKind, caller, callerClass, threadName, g_jvmti);
DBG("getting holder tag, holderKind="<<holderKind);
long holderTag = getTag(holderKind, holder, holderClass, threadName, g_jvmti);
DBG("getting oldval tag");
long oldvaltag = getTag(NativeInterface_SPECIAL_VAL_NORMAL, oldval,
                        type.c_str(), threadName, g_jvmti);
DBG("getting newval tag");
long newvaltag = getTag(NativeInterface_SPECIAL_VAL_NORMAL, newval,
                        type.c_str(), threadName, g_jvmti);
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
msgbuilder.setCallertag(getOrDoTag(
    callerKind, caller, msgbuilder.asReader().getCallerclass().cStr(), threadName,
    g_jvmti, g_lock));
msgbuilder.setThreadName(getThreadName(g_jvmti, g_lock)
  //getOrDoTag(NativeInterface_SPECIAL_VAL_NORMAL, thread,
  //           "java/lang/Thread", thread, g_jvmti,
  //           g_lock)
                      );

anybuilder.setFieldstore(msgbuilder.asReader());
capnp::writeMessageToFd(capnproto_fd, outermessage);
#endif // ifdef ENABLED
}

JNIEXPORT void JNICALL Java_NativeInterface_storeFieldA(
    JNIEnv *env, jclass native_interface, jint holderKind, jobject holder,
    jobject newVal, jobject oldval, jstring _holderClass, jstring _fname,
    jstring _type, jstring _callerClass, jstring _callerMethod, jint callerKind,
    jobject caller) {
#ifdef ENABLED
  SKIP_BEFORE_LIVE;
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
  SKIP_BEFORE_LIVE;
  LOCK;
  DBG("Java_NativeInterface_storeVar");
  auto threadName = getThreadName(g_jvmti, g_lock);

  ASSERT(oldValKind != NativeInterface_SPECIAL_VAL_STATIC);
  ASSERT(newValKind != NativeInterface_SPECIAL_VAL_STATIC);
  //handleValStatic(env, &callerKind, &caller, callerClass);
  //handleValStatic(env, &newValKind, &newVal, NULL);
  //handleValStatic(env, &oldValKind, &oldval, NULL);

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  VarStoreEvt::Builder msgbuilder = innermessage.initRoot<VarStoreEvt>();

  msgbuilder.setCallerclass(toStdString(env, callerClass));
  long newValTag = getTag(newValKind, newVal,
                          msgbuilder.asReader().getCallerclass().cStr(),
                          threadName, g_jvmti);
  DBG("newValtag="<<newValTag);
  msgbuilder.setNewval(newValTag);
  msgbuilder.setOldval(0 /* this feature is not used in the instrumentation*/);
  msgbuilder.setVar(var);
  msgbuilder.setCallermethod(toStdString(env, callerMethod));

  msgbuilder.setCallertag(getOrDoTag(
      callerKind, caller, msgbuilder.asReader().getCallerclass().cStr(), threadName,
      g_jvmti, g_lock));
  msgbuilder.setThreadName(getThreadName(g_jvmti, g_lock)
                          //getOrDoTag(NativeInterface_SPECIAL_VAL_NORMAL, thread,
                          //           "java/lang/Thread", thread, g_jvmti,
                          //           g_lock)
                        );

  anybuilder.setVarstore(msgbuilder.asReader());
  capnp::writeMessageToFd(capnproto_fd, outermessage);
  DBG("storeVar done");
#endif // ifdef ENABLED
}

JNIEXPORT void JNICALL
Java_NativeInterface_loadVar(JNIEnv *env, jclass native_interface, jint valKind,
                             jobject val, jint var, jstring callerClass,
                             jstring callerMethod, jint callerKind,
                             jobject caller) {
#ifdef ENABLED
  SKIP_BEFORE_LIVE;
  LOCK;
  auto threadName = getThreadName(g_jvmti, g_lock);
  DBG("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");
  DBG("Java_NativeInterface_loadVar " << var << " in "
                                      << toStdString(env, callerClass) << "::"
                                      << toStdString(env, callerMethod));
  ASSERT(valKind != NativeInterface_SPECIAL_VAL_STATIC);
  //handleValStatic(env, &callerKind, &caller, callerClass);
  //handleValStatic(env, &valKind, &val, NULL);

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  VarLoadEvt::Builder msgbuilder = innermessage.initRoot<VarLoadEvt>();
  long valTag = getTag(valKind, val,
                       "no/var/class/available", threadName, g_jvmti);
  long callerTag =
      getTag(NativeInterface_SPECIAL_VAL_NORMAL, caller,
             toStdString(env, callerClass), threadName, g_jvmti);

  msgbuilder.setVal(valTag);
  DBG("valTag    = " << valTag);
  DBG("callerTag = " << callerTag);

  msgbuilder.setVar((char)var);
  msgbuilder.setCallermethod(toStdString(env, callerMethod));
  msgbuilder.setCallerclass(toStdString(env, callerClass));
  msgbuilder.setCallertag(getOrDoTag(
      callerKind, caller, msgbuilder.asReader().getCallerclass().cStr(), threadName,
      g_jvmti, g_lock));
  msgbuilder.setThreadName(getThreadName(g_jvmti, g_lock)
                          //getOrDoTag(NativeInterface_SPECIAL_VAL_NORMAL, threadName,
                          //           "java/lang/Thread", threadName, g_jvmti,
                          //           g_lock)
                        );
  anybuilder.setVarload(msgbuilder.asReader());

  capnp::writeMessageToFd(capnproto_fd, outermessage);
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
  SKIP_BEFORE_LIVE;
  LOCK;
  auto threadName = getThreadName(g_jvmti, g_lock);
  DBG("Java_NativeInterface_modify");

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  ReadModifyEvt::Builder msgbuilder = innermessage.initRoot<ReadModifyEvt>();
  msgbuilder.setIsModify(true);
  DBG("...");
  msgbuilder.setCalleeclass(calleeClass);
  DBG("...");
  msgbuilder.setCalleetag(getOrDoTag(
      calleeKind, callee, msgbuilder.asReader().getCalleeclass().cStr(), threadName,
      g_jvmti, g_lock));
      DBG("...");
  msgbuilder.setFname(fname);
  DBG("...");
  msgbuilder.setCallerclass(callerClass);
  DBG("...");
  msgbuilder.setCallertag(getOrDoTag(
      callerKind, caller, msgbuilder.asReader().getCallerclass().cStr(), threadName,
      g_jvmti, g_lock));
      DBG("...");
  msgbuilder.setThreadName(getThreadName(g_jvmti, g_lock)
                          //getOrDoTag(NativeInterface_SPECIAL_VAL_NORMAL, threadName,
                          //           "java/lang/Thread", threadName, g_jvmti,
                          //           g_lock)
                          );

                          DBG("...");
  anybuilder.setReadmodify(msgbuilder.asReader());

  capnp::writeMessageToFd(capnproto_fd, outermessage);
  DBG("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");

#endif // ifdef ENABLED
}

JNIEXPORT void JNICALL
Java_NativeInterface_modify(JNIEnv *env, jclass native_interface,
                            jint calleeKind, jobject callee,
                            jstring calleeClass, jstring fname, jint callerKind,
                            jobject caller, jstring callerClass) {
#ifdef ENABLED
  SKIP_BEFORE_LIVE;
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
  SKIP_BEFORE_LIVE;
  LOCK;
  auto threadName = getThreadName(g_jvmti, g_lock);
  DBG("Java_NativeInterface_read");
  //handleValStatic(env, &calleeKind, &callee, calleeClass);
  //handleValStatic(env, &callerKind, &caller, callerClass);

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  ReadModifyEvt::Builder msgbuilder = innermessage.initRoot<ReadModifyEvt>();
  msgbuilder.setIsModify(false);
  msgbuilder.setCalleeclass(calleeClass);
  msgbuilder.setCalleetag(getOrDoTag(
      calleeKind, callee, msgbuilder.asReader().getCalleeclass().cStr(), threadName,
      g_jvmti, g_lock));
  msgbuilder.setFname(fname);
  msgbuilder.setCallerclass(callerClass);
  msgbuilder.setCallertag(getOrDoTag(
      callerKind, caller, msgbuilder.asReader().getCallerclass().cStr(), threadName,
      g_jvmti, g_lock));
  msgbuilder.setThreadName(getThreadName(g_jvmti, g_lock)
                          //getOrDoTag(NativeInterface_SPECIAL_VAL_NORMAL, threadName,
                          //           "java/lang/Thread", threadName, g_jvmti,
                          //           g_lock)
                        );

  anybuilder.setReadmodify(msgbuilder.asReader());

  capnp::writeMessageToFd(capnproto_fd, outermessage);
#endif // ifdef ENABLED
}


JNIEXPORT void JNICALL
Java_NativeInterface_read(JNIEnv *env, jclass nativeInterface, jint calleeKind,
                          jobject callee, jstring calleeClass, jstring fname,
                          jint callerKind, jobject caller, jstring callerClass) {
#ifdef ENABLED
  SKIP_BEFORE_LIVE;
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
  SKIP_BEFORE_LIVE;
  DBG("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");
  DBG("Java_NativeInterface_loadFieldA");
  auto threadName = getThreadName(g_jvmti, g_lock);
  //handleValStatic(env, &holderKind, &holder, holderClass);
  //handleValStatic(env, &callerKind, &caller, callerClass);

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  FieldLoadEvt::Builder msgbuilder = innermessage.initRoot<FieldLoadEvt>();
  DBG("blip")
  msgbuilder.setHolderclass(holderClass);
  DBG("blip")
  msgbuilder.setHoldertag(getOrDoTag(
      holderKind, holder, msgbuilder.asReader().getHolderclass().cStr(), threadName,
      g_jvmti, g_lock));
      DBG("blip")
  msgbuilder.setFname(fname);
  DBG("blip")
  msgbuilder.setType(type);
  DBG("blip")
  msgbuilder.setVal(getTag(NativeInterface_SPECIAL_VAL_NORMAL, val,
                           msgbuilder.asReader().getType().cStr(), threadName,
                           g_jvmti));
                           DBG("blip")
  msgbuilder.setCallermethod(callerMethod);
  DBG("blip")
  msgbuilder.setCallerclass(callerClass);
  DBG("blip")
  msgbuilder.setCallertag(getTag(
      callerKind, caller, msgbuilder.asReader().getCallerclass().cStr(), threadName,
      g_jvmti));
      DBG("blip")
  msgbuilder.setThreadName(getThreadName(g_jvmti, g_lock));
  DBG("blip")

  anybuilder.setFieldload(msgbuilder.asReader());
  DBG("blip")

  capnp::writeMessageToFd(capnproto_fd, outermessage);
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
  SKIP_BEFORE_LIVE;
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
  Catches event when the gc frees a tagged object
*/
void JNICALL cbObjectFree(jvmtiEnv *env, jlong tag) {
#ifdef ENABLED
  DBG("cbObjectFree");

  capnp::MallocMessageBuilder outermessage;
  AnyEvt::Builder anybuilder = outermessage.initRoot<AnyEvt>();
  capnp::MallocMessageBuilder innermessage;
  ObjFreeEvt::Builder msgbuilder = innermessage.initRoot<ObjFreeEvt>();
  msgbuilder.setTag(tag);

  anybuilder.setObjfree(msgbuilder.asReader());

  capnp::writeMessageToFd(capnproto_fd, outermessage);
#endif // ifdef ENABLED
}

/*******************************************************************/
/* JVMTI System Callbacks
   Callbacks from the JVM to the agent.

   * Agent_OnLoad is automatically invoked by the JVM if the
   agentflag is passed to JAVA.

   * The rest are set to be invoked in Agent_OnLoad. */
/*******************************************************************/

//bool isBlackListedClassName(const std::string &name) {
//  if ( // strstr(name.c_str(), "java/lang/invoke") ||
//          strstr(name.c_str(), "NativeInterface") ||
//          strstr(name.c_str(), "ClassRep") ||
//          //strstr(name.c_str(), "java/security") ||
//          //strstr(name.c_str(), "java/lang/") ||
//          //strstr(name.c_str(), "sun/reflect/")
//          // strstr(name.c_str(), "java/lang/Thread") ||
//          strstr(name.c_str(), "java/lang/ClassLoader") ||
//          strstr(name.c_str(), "NoClassDefFoundError") ||
//          strstr(name.c_str(), "java/lang/Object") ||
//          strstr(name.c_str(), "Exception") ||
//          // strstr(name.c_str(), "java/lang/Object") ||
//          // strstr(name.c_str(), "java/lang/ref/") ||
//          //||      strstr(name.c_str(), "java/lang/Class" )
//          ////      strstr(name.c_str(), "Throwable" )
//          false
//          ) {
//    return true;
//  } else {
//    return false;
//  }
//}

/*
  Sent by the VM when a classes is being loaded into the VM

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
  if (!_name) {
    return; // THIS HAPPENS! WHY? Maybe anonymous classes?! TODO Write test
  }
  ASSERT(_name);
  ASSERT(class_data);

  std::string name(_name);

  if (!g_init) {
    //if  (!isWhiteListedClassName(name)) {
    //  // when running tests, only instrument the application code
      DBG("skipping class "<<name<<" (still using bootstrap classloader)");
      if (!isClassInstrumented(name)) {
        // class is not in xbootclasspath!
        DBG("marking");
        markClassAsUninstrumented(name);
      }
      DBG("...returning");
      return;
  }

  if (loader && !g_had_loader) {
    g_had_loader = true;
    g_awaiting_main_call = true;
  }

  DBG("ClassFileLoadHook: " << name);

  jvmtiPhase phase;
  if (jvmti_env->GetPhase(&phase) != JVMTI_ERROR_NONE) {
    ERR("can't get phase");
  }
  DBG("phase  = " << phase);
  DBG("loader = " << loader);
  DBG("g_init = " << g_init);

  DBG("instrumenting class " << name);
  int sock;
  sock = setupSocket();
  sendClass(sock, class_data, class_data_len, name);

  sock = setupSocket();
  *new_class_data_len = recvClass(sock, new_class_data);
  int minLen = *new_class_data_len < class_data_len ? *new_class_data_len : class_data_len;
  if ((class_data_len != *new_class_data_len) ||
      (memcmp(class_data, *new_class_data, minLen) != 0)) {
    DBG("class "<<name<<" is instrumented: got changed class back");
    markClassAsInstrumented(name);
  } else {
    DBG("class "<<name<<" is not instrumented: got unchanged class back");
    //FIXME we have to count these too! if it's in xbootclasspath, it should be marked as instrumented,
    // otherwise not

  }

  // unsigned char **new_class_data_ignore;
  // recvClass(sock, new_class_data_ignore);
  DBG("done");
#endif // ifdef ENABLED
}

/*
  The VM initialization event signals the completion of
  VM initialization.

  * Sets init to 1 to indicate the VM init
*/
void JNICALL VMInit(jvmtiEnv *jvmti_env, JNIEnv *jni, jthread threadName) {
  #ifdef ENABLED
  DBG("VMInit");

  g_init = true;
  #endif // ENABLED
}

void JNICALL VMDeath(jvmtiEnv *jvmti_env, JNIEnv *jni_env) {
  #ifdef ENABLED
  DBG("VMDeath");
  g_dead = true;
  close(capnproto_fd);
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
  std::cout << "dumping output to file " << tracefilename << std::endl;
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
  //if (options == NULL){
  //  options = std::string("").c_str();
  //}
  // LOCK;

  jvmtiError error;
  jint res;

  markClassFilesAsInstrumented("../transformer/instrumented_java_rt/output");

  if (options != NULL) {
    parse_options(options);
  }
  // cerr << "agent started\n";
  // Get jvmti env
  capnproto_fd = open(tracefilename.c_str(), O_CREAT | O_WRONLY | O_TRUNC,
                      S_IRUSR | S_IWUSR);
  if (capnproto_fd == -1) {
    ERR("could not open log file '" << tracefilename << "'");
  }

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
  //  capabilities.can_access_local_variables = 1;
  capabilities.can_generate_object_free_events = 1;
  capabilities.can_generate_vm_object_alloc_events = 1;
  capabilities.can_generate_exception_events = 1;
  error = g_jvmti->AddCapabilities(&capabilities);
  ASSERT_NO_JVMTI_ERR(g_jvmti, error);

  // Set callbacks
  jvmtiEventCallbacks callbacks;
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.VMInit = &VMInit;
  callbacks.VMDeath = &VMDeath;
  callbacks.ClassFileLoadHook = &ClassFileLoadHook;
  callbacks.ObjectFree = &cbObjectFree;
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

  //#endif // ifdef ENABLED

  DBG("extending bootstrap classloader search");
  error = g_jvmti->AddToBootstrapClassLoaderSearch("./");
  ASSERT_NO_JVMTI_ERR(g_jvmti, error); // make NativeInterface.class visible
  error = g_jvmti->AddToBootstrapClassLoaderSearch("/Users/stebr742/.m2/repository/com/github/kaeluka/spencer-tracing-jni/0.1.2-SNAPSHOT/");
  ASSERT_NO_JVMTI_ERR(g_jvmti, error); // make NativeInterface.class visible

  DBG("extending bootstrap classloader search: done");
  return JNI_OK;
}

/*
  This function is invoked by the JVM just before it unloads
  exports the events.
*/
JNIEXPORT void JNICALL Agent_OnUnload(JavaVM *vm) {
  //DODBG("Agent_OnUnload");
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
    //     DODBG("uninstrumented "<<++cnt<<"/"<<uninstrumentedClasses.size()
    //                       <<": class "<<*it);
    //  }
    //DODBG("instrumented "<<instrumentedClasses.size()<<" classes, skipped "<<uninstrumentedClasses.size());
  }
#endif // ifdef ENABLED
}
