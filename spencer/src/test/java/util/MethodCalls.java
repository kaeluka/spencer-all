package util;

public class MethodCalls {

    public static void main(String[] args) {
        System.out.println("Hello, Methodcalls");
        MethodCalls.Foo();
    }

    public static int Foo() {
        return MethodCalls.Bar();
    }

    private static int Bar() {
        return 0;
    }
}
