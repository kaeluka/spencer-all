package com.github.kaeluka.spencer.instrumentation.testing;

public class TestPlayground {
	private static int tesssst() {
//		try {
			throw new IllegalArgumentException();
//		} catch (IllegalArgumentException ex) {
//			System.err.println("caught by application");
////			continuehere: the exception is not caught with the org.spencer.instrumentation on!
////			return -12;
//		}
//		return 12;
	}
	
	public TestPlayground() {
		try {
			tesssst();
		} catch (Exception ex) {
			System.out.println("caught by caller");
		}
	}
}