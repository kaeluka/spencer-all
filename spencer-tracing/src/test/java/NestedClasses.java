package test;
import java.util.LinkedList;

public class NestedClasses {
    public static void main(String[] args) {
      System.out.println("NestedClasses running..");
      Outer outer = new Outer();
      outer.setInner();
      System.out.println("NestedClasses done..");
    }
}

class Outer {
  // If this line is commented out, the test runs:
  Inner i;// = new Inner();
  void setInner() {
    this.i = new Inner();
  }
  class Inner {
    public Inner() {

    }
  }
}
