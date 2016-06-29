package test;

public class HeapMoves {
  static Foo a;
  static Foo b;

  private static void makeFoos(final int N, boolean heapUnique) {
    for (int i=0; i<N; ++i) {
      HeapMoves.a = new Foo();
      HeapMoves.b = HeapMoves.a;
      if (heapUnique) {
        HeapMoves.b.toString();
      } else {
        HeapMoves.a.toString();
      }
    }
  }

  public static void main(String args[]) {
    makeFoos(10, true);
    makeFoos(10, false);
  }

  private static class Foo {
    @Override
    public String toString() {
      return "just a Foo";
    }
  }
}
