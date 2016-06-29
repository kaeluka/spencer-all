//package transformer.testing;
//
//import Instrument;
//import org.spencer.org.spencer.instrumentation.TransformerServer;
//
//import java.io.IOException;
//import java.lang.reflect.Constructor;
//import java.lang.reflect.InvocationTargetException;
//import java.net.URL;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//
//public class TestTransformer {
//
//	static class InstrumentingClassLoader extends ClassLoader {
//		public InstrumentingClassLoader() {
//			super();
//		}
//
//		private Class<?> fallback(String name) throws ClassNotFoundException {
////			System.err.println("falling back for "+name);
//			return super.loadClass(name);
//		}
//
//		@Override
//		public Class<?> loadClass(String name) throws ClassNotFoundException {
//			if (name.startsWith("NativeInterface")) {
//				return this.fallback(name);
//			}
//			try {
//				final Class<?> systemClass = this.findSystemClass(name);
//				final String descr = systemClass.getName().replace(".", "/");
//				final URL resource = this.getResource(descr+".class");
//				if (resource == null) {
//					return this.fallback(name);
//				}
//				try {
//					//					System.err.println("name         is: "+name);
//					//					System.err.println("system class is: "+systemClass.getName());
//					//					System.err.println("resource     is: "+resource);
//					final byte[] inputByteCode = Files.readAllBytes(Paths.get(resource.getPath()));
//					TransformerServer.dumpClassDataToFile(inputByteCode, "input");
//					final byte[] outputByteCode = Instrument.transform(inputByteCode);
//					TransformerServer.dumpClassDataToFile(outputByteCode, "output");
//					return doLoadClass(outputByteCode);
//				} catch (IOException e) {
//					//					e.printStackTrace();
//					// TODO: we also reach this when a resource is hidden in a jar! We should get it out in such cases.
//					return this.fallback(name);
//				}
//			} catch (ClassNotFoundException ex) {
//				return this.fallback(name);
//			}
//		}
//
//		private Class<?> doLoadClass(byte[] b) {
//			//override classDefine (as it is protected) and define the class.
//			Class<?> clazz = null;
//			try {
//				Class<?> cls = Class.forName("java.lang.ClassLoader");
//				java.lang.reflect.Method method =
//						cls.getDeclaredMethod("defineClass", new Class[] { String.class, byte[].class, int.class, int.class });
//
//				// protected method invocation
//				method.setAccessible(true);
//				try {
//					Object[] args = new Object[] { null, b, new Integer(0), new Integer(b.length)};
//					clazz = (Class<?>) method.invoke(this, args);
//				} finally {
//					method.setAccessible(false);
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//				System.exit(1);
//			}
//			return clazz;
//		}
//	}
//
//	public static <T> T newInstrumented(Class<T> cls) {
//		try {
//			Constructor<?> ctor = cls.getConstructor();
//			ctor.setAccessible(true);
//			return (T)ctor.newInstance();
//		} catch (InstantiationException e) {
//			e.printStackTrace();
//		} catch (IllegalAccessException e) {
//			e.printStackTrace();
//		} catch (NoSuchMethodException e) {
//			e.printStackTrace();
//		} catch (SecurityException e) {
//			e.printStackTrace();
//		} catch (IllegalArgumentException e) {
//			e.printStackTrace();
//		} catch (InvocationTargetException e) {
//			e.printStackTrace();
//		}
//		throw new IllegalArgumentException("could not instrument class");
//	}
//
//	public static void main(String[] args) {
//
//		ClassLoader loader = new InstrumentingClassLoader();
//
//		try {
//			try {
//				loader.loadClass("TestPlayground").newInstance();
//			} catch (InstantiationException | IllegalAccessException e) {
//				e.printStackTrace();
//			}
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		}
//	}
//}
