@0xe34bb656aa74c23e;

using Java = import "/java.capnp";
$Java.package("com.github.kaeluka.spencer");
$Java.outerClassname("Events");

struct MethodEnterEvt {
  name         @0 :Text;
  calleeclass  @1 :Text;
  calleetag    @2 :Int64;
  signature    @3 :Text;
  threadName   @4 :Text;
  callsitefile @5 :Text;
  callsiteline @6 :Int64;
}

struct MethodExitEvt {
  name       @0  :Text;
  cname      @1  :Text;
  threadName @2  :Text;
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

struct FieldTuple {
  name @0 :Text;
  val  @1 :Int64;
}

struct LateInitEvt {
  calleetag     @0 :Int64;
  calleeclass   @1 :Text;
  fields        @2 :List(FieldTuple);
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
    lateinit    @1 :LateInitEvt;
    fieldstore  @2 :FieldStoreEvt;
    fieldload   @3 :FieldLoadEvt;
    varstore    @4 :VarStoreEvt;
    varload     @5 :VarLoadEvt;
    readmodify  @6 :ReadModifyEvt;
    methodexit  @7 :MethodExitEvt;
  }
}
