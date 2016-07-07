package com.github.kaeluka.spencer.instrumentation;

import java.io.File;

public class Util {
	public static boolean isClassNameBlacklisted(String name) {
		final String[] blackList = {
/*				"java/lang/Object",
				"java/lang/Class",
				"java/lang/Class$",
				"java/lang/ClassLoader",
				"java/lang/Thread",
//				"java/lang/invoke",
				"java/security/AccessControlContext",
				"NativeInterface",
				"java/util/AbstractCollection.class",
				"java/util/AbstractList.class",
				"java/util/HashMap.class",
				"java/util/Vector.class",
				"java/util/LinkedList$ListItr",
				"java/lang/Shutdown",
				"java/lang/System",
//				"java/security/Permission",
				"java/lang/String",
				"java/lang/Float", //NEW
				"java/lang/ref", //NEW
				"sun/nio/cs",    //NEW
				"java/io/File",  //NEW //happens with static field instr in ctors
				"java/io/FileOutputStream",  //NEW
				"java/io/PrintStream",  //NEW
				"java/util/Hashtable", //NEW
				"sun/util/PreHashedMap", //NEW
//				"java/lang/Properties",
//				"java/util/HashTable",
				"sun/launcher/",*/
		};
		for (String b : blackList) {
			if (name.contains(b)) {
				return true;
			}
		}

		return false;
	}

	public static boolean isInXBootclassPath(String typeDescr) {
		final File file = new File("instrumented_java_rt/output/"+typeDescr+".class");
		final boolean exists = file.exists();
		return exists;
	}
}
