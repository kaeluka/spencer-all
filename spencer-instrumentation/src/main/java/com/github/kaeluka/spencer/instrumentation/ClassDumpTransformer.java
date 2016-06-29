//package org.spencer.org.spencer.instrumentation;
//
//import org.apache.commons.io.IOUtils;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//
//public class ClassDumpTransformer {
//	public final static String dumpedClass  = "java/util/zip/ZipEntry";
//	public final static boolean fromRunTime = true;
//
//	private static byte[] readDumpFile(String dumpFileName) {
//		File file = null;
//		try {
//			file = new File(dumpFileName);
//			FileInputStream in = new FileInputStream(file);
//			return IOUtils.toByteArray(in);
//		} catch (FileNotFoundException e) {
//			System.err.println("File  not found: "+file.getAbsolutePath());
//			System.exit(1);
//			return null;
//		} catch (IOException e) {
//			System.err.println("File exists, but error ("+e.toString()+") reading from file "+file.getAbsolutePath());
//			System.exit(1);
//			return null;
//		}
//	}
//
//	public static void main(String[] args) {
//
//		final byte[] originalByteCode = (ClassDumpTransformer.fromRunTime) ?
//				readDumpFile("instrumented_java_rt/input/"+ClassDumpTransformer.dumpedClass+".class") :
//				readDumpFile("log/input/"+ClassDumpTransformer.dumpedClass.replace('.', '_')+".class");
//		final byte[] transformedByteCode = Instrument.transform(originalByteCode);
//		for (byte b : originalByteCode) {
//			System.err.print((int)b+" ");
//		}
//		System.out.println("");
//		for (byte b : transformedByteCode) {
//			System.err.print((int)b+" ");
//		}
//
//	}
//}
