import java.util.Arrays;
import java.util.HashMap;

public class NativeInterface {
	public static final int SPECIAL_VAL_NORMAL = 0; // normal
	public static final int SPECIAL_VAL_THIS = 1; //returned as callee id from constructors
	public static final int SPECIAL_VAL_STATIC = 2; //returned instead of oids when there is no object
	public static final int SPECIAL_VAL_NOT_IMPLEMENTED = 3;
	public static final int SPECIAL_VAL_JVM = 4; // the oid that represents the JVM "object" calling static void main(...)
	public static final int SPECIAL_VAL_NOT_INSTRUMENTED = 5;
	public static final int SPECIAL_VAL_MAX = 100;

	private static final HashMap<Integer, String> kindToNames;

	static {
		kindToNames = new HashMap<>();
		kindToNames.put(SPECIAL_VAL_NORMAL,          "SPECIAL_VAL_NORMAL");
		kindToNames.put(SPECIAL_VAL_THIS,            "THIS");
		kindToNames.put(SPECIAL_VAL_STATIC,          "STATIC");
		kindToNames.put(SPECIAL_VAL_NOT_IMPLEMENTED, "NOT_IMPLEMENTED");
		kindToNames.put(SPECIAL_VAL_JVM,             "JVM");
		kindToNames.put(SPECIAL_VAL_JVM,             "NOT_INSTRUMENTED");
		kindToNames.put(SPECIAL_VAL_MAX,             "SPECIAL_VAL_MAX");
	}

	public static String valKindToString(int valKind) {
		return kindToNames.get(valKind);
	}

	public static String valAndValKindToString(Object val, String type, int valKind) {
		switch (valKind) {
		case SPECIAL_VAL_NORMAL:
			if (val != null) {
				return val.toString();
			} else {
				return "NULL:"+type+"\t";
			}
		case SPECIAL_VAL_THIS:
		case SPECIAL_VAL_STATIC:
			return valKindToString(valKind)+":"+type;
		default:
			return valKindToString(valKind)+"\t";
		}
	}

	////////////////////////////////////////////////////////////////
	
	public static void loadArrayA(
			Object[] arr,
			int idx,
			Object val,
			String calleeClass,
			String callerMethod,
			String callerClass,
			int callerValKind,
			Object caller) {
		final String elementType = calleeClass.substring(1);	
		NativeInterface.loadFieldA(val, SPECIAL_VAL_NORMAL, arr, calleeClass, "_"+idx, elementType, callerClass, callerMethod, callerValKind, caller);
	}

	public static void storeArrayA(
			Object newVal,
			Object[] arr,
			int idx,
			Object oldVal,
			String holderClass,
			String callerMethod,
			String callerClass,
			int callerValKind,
			Object caller) {
		final String elementType = holderClass.substring(1);
		NativeInterface.storeFieldA(SPECIAL_VAL_NORMAL, arr, newVal, oldVal, holderClass, "_"+idx, elementType, callerClass, callerMethod, callerValKind, caller);
	}

	public static void readArray(
			Object arr,
			int idx,
			int callerValKind,
			Object caller,
			String callerClass) {
		NativeInterface.read(SPECIAL_VAL_NORMAL, arr, arr.getClass().getName(), "_"+idx, callerValKind, caller, callerClass);
	}

	public static void modifyArray(
			Object arr,
			int idx,
			int callerValKind,
			Object caller,
			String callerClass) {
		NativeInterface.modify(SPECIAL_VAL_NORMAL, arr, arr.getClass().getName(), "_"+idx, callerValKind, caller, callerClass);
	}
	
	////////////////////////////////////////////////////////////////

	public static void methodExit(
			String mname, String cname) {
		System.err.println("methodExit(      "+cname+"::"+mname+")");
	}

	public static void methodEnter(
			String name,
			String signature,
			String calleeClass,
			int calleeValKind,
			Object callee,
			Object[] args) {
		String argsStr = (args==null) ? "[]" : Arrays.asList(args).toString();
//		if (args != null) {
//			for (Object arg : args) {
//				argsStr += ((arg != null) ? arg.toString() : "null") + " ";
//			}
//			argsStr = argsStr.trim();
//		}
		System.err.println("methodEnter(     "+
				valAndValKindToString(callee, calleeClass, calleeValKind)+" . "+name+signature+", "+
				"args="+argsStr+")");
	}

	public static void afterInitMethod(
			Object callee,
			String calleeClass) {
		System.err.println("afterInitMethod( "+
				callee+", "+
				"calleeClass="+calleeClass+", "+
				")");
	}

	////////////////////////////////////////////////////////////////

	public static void newObj(
			Object created,
			String createdClass,
			String callerClass,
			String callerMethod,
			int callerValKind,
			Object caller) {
		System.err.println("newObj("+
				"created="+created+", "+
				"createdClass="+createdClass+", "+
				"callerClass="+callerClass+", "+
				"callerMethod="+callerMethod+", "+
				"callerValKind="+valKindToString(callerValKind)+", "+
				"caller="+caller+", "+
				")");
	}

	////////////////////////////////////////////////////////////////
	
	public static void storeFieldA(
			int holderValKind,
			Object holder,
			Object newVal,
			Object oldVal,
			String holderClass,
			String fieldName,
			String type,
			String callerClass,
			String callerMethod,
			int callerValKind,
			Object caller) {
		System.err.println("storeFieldA(     "+
				valAndValKindToString(holder, holderClass, holderValKind)+
				" . "+fieldName+				
				" = "+newVal+", "+
				"oldVal="+oldVal+", "+
//				"holderClass="+holderClass+", "+
				"fieldType="+type+", "+
//				"callerClass="+callerClass+", "+
				"callerMethod="+callerMethod+", "+
				"caller="+valAndValKindToString(caller, callerClass, callerValKind)+", "+
				")");
	}

	public static void loadFieldA(
			Object value,
			int holderValKind,
			Object holder,
			String holderClass,
			String fieldName,
			String type,
			String callerClass,
			String callerMethod,
			int callerValKind,
			Object caller) {
		System.err.println("loadFieldA(      "+
				valAndValKindToString(holder, holderClass, holderValKind)+
				" . "+fieldName+
				" :: "+type+", "+
				"value= "+value+", "+
//				"holderClass="+holderClass+", "+
//				"callerClass="+callerClass+", "+
				"callerMethod="+callerMethod+", "+
				"caller="+valAndValKindToString(caller, callerClass, callerValKind)+", "+
				")");
	}

	////////////////////////////////////////////////////////////////

	public static void storeVar(
			int newValKind,
			Object newVal,
			int oldValKind,
			Object oldVal,
			int var,
			String callerClass,
			String callerMethod,
			int callerValKind,
			Object caller) {
		System.err.println("storeVar(        "+
				valAndValKindToString(newVal, "<UnknownClass>", newValKind)+", "+
				"oldVal="+valAndValKindToString(oldVal, "<UnknownClass>", oldValKind)+", "+
				"var="+var+", "+
//				"callerClass="+callerClass+", "+
				"callermethod="+callerMethod+", "+
				"caller="+valAndValKindToString(caller, callerClass, callerValKind)+", "+
				")");
	}

	public static void loadVar(
			int valKind,
			Object val,
			int var,
			//String type,
			String callerClass,
			String callerMethod,
			int callerValKind,
			Object caller) {
		System.err.println("loadVar(         "+
				"val="+valAndValKindToString(val, "<UnknownClass>", valKind)+", "+
				"var="+var+", "+
//				"callerClass="+callerClass+", "+
				"callerMethod="+callerMethod+", "+
				"caller="+valAndValKindToString(caller, callerClass, callerValKind)+", "+
				")");
	}

	////////////////////////////////////////////////////////////////

	// an object WRITES a primitive field of another object
	public static void modify(
			int calleeValKind,
			Object callee,
			String calleeClass,
			String fieldName,
			int callerValKind,
			Object caller,
			String callerClass) {
		System.err.println("modify(          "+
				valAndValKindToString(callee, calleeClass, calleeValKind)+
				" . "+fieldName+", "+
//				"calleeClass="+calleeClass+", "+
				"caller="+valAndValKindToString(caller, callerClass, callerValKind)+", "+
//				"callerClass="+callerClass+", "+
				")");
	}

	// an object READS a primitive field of another object
	public static void read(
			int calleeValKind,
			Object callee,
			String calleeClass,
			String fieldName,
			int callerValKind,
			Object caller,
			String callerClass) {
		System.err.println("read(            "+
				valAndValKindToString(callee, calleeClass, calleeValKind)+
				" . "+fieldName+", "+
//				"calleeClass="+calleeClass+", "+
				"caller="+valAndValKindToString(caller, callerClass, callerValKind)+", "+
//				"callerClass="+callerClass+
				")");
	}
}

