import com.github.kaeluka.spencer.tracing.NarSystem;

public class NativeInterface {

//  static {
//    NarSystem.loadLibrary();
//  }

  public static final int SPECIAL_VAL_NORMAL = 0; // normal
  public static final int SPECIAL_VAL_THIS = 1; //returned as callee id from constructors
  public static final int SPECIAL_VAL_STATIC = 2; //returned instead of oids when there is no object
  public static final int SPECIAL_VAL_NOT_IMPLEMENTED = 3;
  public static final int SPECIAL_VAL_JVM = 4; // the oid that represents the JVM "object" calling static void main(...)
  public static final int SPECIAL_VAL_NOT_INSTRUMENTED = 5; // the oid that represents the JVM "object" calling static void main(...)
  public static final int SPECIAL_VAL_MAX = 100;

  ////////////////////////////////////////////////////////////////

  public static native void loadArrayA(
  			Object[] arr,
  			int idx,
  			Object val,
        String calleeClass,
  			String callerMethod,
  			String callerClass,
  			int callerValKind,
  			Object caller);

  	public static native void storeArrayA(
  			Object newVal,
  			Object[] arr,
  			int idx,
  			Object oldVal,
        String holderClass,
  			String callerMethod,
  			String callerClass,
  			int callerValKind,
  			Object caller);

  	public static native void readArray(
  			Object arr,
  			int idx,
  			int callerValKind,
  			Object caller,
  			String callerClass);

  	public static native void modifyArray(
  			Object arr,
  			int idx,
  			int callerValKind,
  			Object caller,
  			String callerClass);

  ////////////////////////////////////////////////////////////////

  public static native void methodExit(
  String mname,
  String cname);

  public static native void methodEnter(
  String name,
  String signature,
  String calleeClass,
  int calleeValKind,
  Object callee,
  Object[] args);

  public static native void afterInitMethod(
  Object callee,
  String calleeClass);

  ////////////////////////////////////////////////////////////////

  public static native void newObj(
  Object created,
  String createdClass,
  String callerClass,
  String callermethod,
  int callerValKind,
  Object caller);

  ////////////////////////////////////////////////////////////////

  public static native void storeFieldA(
  int holderValKind,
  Object holder,
  Object newVal,
  Object oldVal,
  String holderClass,
  String fname,
  String type,
  String callerClass,
  String callermethod,
  int callerValKind,
  Object caller);

  public static native void loadFieldA(
  Object value,
  int holderValKind,
  Object holder,
  String holderClass,
  String fname,
  String type,
  String callerClass,
  String callermethod,
  int callerValKind,
  Object caller);

  ////////////////////////////////////////////////////////////////

  public static native void storeVar(int newvalkind,
  Object newVal,
  int oldvalkind,
  Object oldVal,
  int var,
  //String type,
  String callerClass,
  String callermethod,
  int callerValKind,
  Object caller);

  public static native void loadVar(int valkind,
  Object val,
  int var,
  //String type,
  String callerClass,
  String callermethod,
  int callerValKind,
  Object caller);

  ////////////////////////////////////////////////////////////////

  // an object WRITES a primitive field of another object
  public static native void modify(int calleeValKind,
  Object callee,
  String calleeClass,
  String fname,
  int callerValKind,
  Object caller,
  String callerClass);

  // an object READS a primitiv field of another object
  public static native void read(int calleeValKind,
  Object callee,
  String calleeClass,
  String fname,
  int callerValKind,
  Object caller,
  String callerClass);
}
