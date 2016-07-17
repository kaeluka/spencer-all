package com.github.kaeluka.spencer.instrumentation;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Instrument {
	public static final boolean enabled  = true; //System.getProperty("org.spencer.instrumentation.enable",                    "true"). equals("true");
	public static final boolean tracing  = System.getProperty("org.spencer.instrumentation.tracing.enable",            "true"). equals("false");
	public static final boolean checking = System.getProperty("org.spencer.instrumentation.checking.enable",           "true"). equals("true");

	public static final boolean instrumentFields  = true;//System.getProperty("org.spencer.instrumentation.fields.enable",    "true"). equals("true");
	public static final boolean instrumentVars    = true;//System.getProperty("org.spencer.instrumentation.variables.enable", "true"). equals("true");;
	public static final boolean instrumentMethods = true;//System.getProperty("org.spencer.instrumentation.methods.enable",   "true"). equals("true");;

	protected static final boolean enableComments = System.getProperty("org.spencer.instrumentation.comments.enable",  "false").equals("true");
	public static final boolean loudWarnings = System.getProperty("org.spencer.instrumentation.warnings.loud",         "true"). equals("true");

	public static final int SPECIAL_VAL_NORMAL = 0; // normal
	public static final int SPECIAL_VAL_THIS   = 1; /*
	 * returned as callee id from
	 * constructors
	 */
	public static final int SPECIAL_VAL_STATIC = 2; /*
	 * returned instead of oids
	 * when there is no object
	 */
	public static final int SPECIAL_VAL_NOT_IMPLEMENTED = 3;/*
	œ * the reference
	 * should not be
	 * used
	 */

	public static final int SPECIAL_VAL_JVM = 4; /*
	 * the oid that represents the
	 * JVM "object" calling static
	 * void main(...)
	 */

	public static final int SPECIAL_VAL_MAX = 5;
	private static List<String> errors = new ArrayList<>();

	public static String getClassName(byte[] byteCode) {
		return new ClassReader(byteCode).getClassName();
	}

	public static boolean isInterface(final byte[] byteCode) {
        return (new ClassReader(byteCode).getAccess() & Opcodes.ACC_INTERFACE) != 0;
	}

	private static void println(final PrintStream out, final String msg) {
		if (out != null) {
			println(out, msg);
		}
	}

	public static byte[] transform(byte[] byteCode, final PrintStream out,
								   final ClassHierarchy hierarchy) {
		final ClassReader classReader = new ClassReader(byteCode);
		final String className = classReader.getClassName();

        if (!Instrument.enabled) {
            println(out, "returning original bytecode (disabled)");
            return byteCode;
        }

		if (Util.isClassNameBlacklisted(className)) {
            println(out, "returning original bytecode (class "+className+" blacklisted)");
            return byteCode;
        }

		hierarchy.registerClass(byteCode);

		final ClassWriter classwriter = new HierarchyClassWriter(classReader,
				hierarchy);
		final ClassVisitor checkingwriter = Instrument.checking ? check(classwriter)
				: classwriter;
		final ClassVisitor tracingwriter = Instrument.tracing ? trace(checkingwriter, className) : checkingwriter;
		classReader.accept(new InstrumentationVisitor(tracingwriter),
				ClassReader.EXPAND_FRAMES);
		println(out, "returning transformed class for "+className);
		return classwriter.toByteArray();
	}

	private static boolean logError(final String className, RuntimeException ex) {
		return Instrument.errors.add(className+": "+ex.getMessage());
	}

	public static List<String> getErrors() {
		return Instrument.errors;
	}

	private static ClassVisitor check(ClassVisitor cv) {
		return new CheckClassAdapter(cv, false);
	}

	private static ClassVisitor trace(ClassVisitor cv, String classname) {

		try {
			final File outfile = new File("log/output/"
					+ classname + ".bytecode");
			if (!outfile.getParentFile().exists()) {
				outfile.getParentFile().mkdirs();
			}
			//			return new TraceClassVisitor(
			//					new TraceClassVisitor(cv, new PrintWriter(outfile)), new PrintWriter(
			//							out));
			return new TraceClassVisitor(cv, new PrintWriter(outfile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}
}
