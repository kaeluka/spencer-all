package test;

public class Hello {
  public static void main(String args[]) throws Throwable {
    System.out.println("Hello, world");
    Thread.sleep(300000);
    System.out.println(Hello.class.getClassLoader());
  }
}
