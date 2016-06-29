@0xe34bb656aa74c23e;

using Java = import "/java.capnp";
$Java.package("com.github.kaeluka.spencer");
$Java.outerClassname("Events");

struct TagObj {
  id    @0 :Int64;
  class @1 :Text;
}

struct MethodEnterEvt {
  name        @0 :Text;
  calleeclass @1 :Text;
  calleetag   @2 :Int64;
  signature   @3 :Text;
  threadName  @4 :Text;
}

struct MethodExitEvt {
  name       @0  :Text;
  threadName @1  :Text;
}

struct FieldStoreEvt {
  holderclass  @0 :Text;
  holdertag    @1 :Int64;
  newval       @2 :Int64;
  oldval       @3 :Int64;
  fname        @4 :Text;
  type         @5 :Text;
  callermethod @6 :Text;
  callerclass  @7 :Text;
  callertag    @8 :Int64;
  threadName   @9 :Text;
}

struct ObjAllocEvt {
  tag           @0 :Int64;
  class         @1 :Text;
  callermethod  @2 :Text;
  callerclass   @3 :Text;
  caller        @4 :Int64;
  threadName    @5 :Text;
}

struct ObjFreeEvt {
  tag @0 :Int64;
}

struct FieldLoadEvt {
  holderclass   @0 :Text;
  holdertag     @1 :Int64;
  val           @2 :Int64;
  fname         @3 :Text;
  type          @4 :Text;
  callermethod  @5 :Text;
  callerclass   @6 :Text;
  callertag     @7 :Int64;
  threadName    @8 :Text;
}

struct VarStoreEvt {
  callermethod  @0 :Text;
  callerclass   @1 :Text;
  callertag     @2 :Int64;
  newval        @3 :Int64;
  oldval        @4 :Int64;
  var           @5 :Int8;
  threadName    @6 :Text;
}

struct VarLoadEvt {
  callermethod  @0 :Text;
  callerclass   @1 :Text;
  callertag     @2 :Int64;
  val           @3 :Int64;
  var           @4 :Int8;
  threadName    @5 :Text;
}

struct ReadModifyEvt {
  isModify     @0 :Bool;
  calleeclass  @1 :Text;
  calleetag    @2 :Int64;
  fname        @3 :Text;
  callerclass  @4 :Text;
  callertag    @5 :Int64;
  threadName   @6 :Text;
}

struct AnyEvt {
  union {
    methodenter @0 :MethodEnterEvt;
    objfree     @1 :ObjFreeEvt;
    objalloc    @2 :ObjAllocEvt;
    fieldstore  @3 :FieldStoreEvt;
    fieldload   @4 :FieldLoadEvt;
    varstore    @5 :VarStoreEvt;
    varload     @6 :VarLoadEvt;
    readmodify  @7 :ReadModifyEvt;
    methodexit  @8 :MethodExitEvt;
  }
}
