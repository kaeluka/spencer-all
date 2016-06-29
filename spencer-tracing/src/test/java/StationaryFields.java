package test;

public class StationaryFields {

  static Box[] statBoxes;

  public static void main(String args[]) {
    //System.out.println("hello");
    //java.util.concurrent.ConcurrentHashMap<Integer, String> l = new java.util.concurrent.ConcurrentHashMap();
    //l.put(120, "yeassh?");
    //l.put(13, "yeah?");
    //System.out.println(l);
    // Integer foo = new Integer(12);
    statBoxes = makeSomeStationaryThings(10);
    //makeSomeNonStationaryThings(10);
  }

  private static Box[] makeSomeStationaryThings(final int n) {
    Box boxes[] = new Box[n];
    for (int i=0; i<n; ++i) {
      boxes[i] = new Box();
      boxes[i].i = i+1;
      boxes[i].i = -(i+1);
    }
    long sum = 0;

    for (int i=0; i<n; ++i) {
      sum += boxes[i].i;
    }

    System.out.println(boxes[0].i);
    return boxes;
  }

  private static Box[] makeSomeNonStationaryThings(int n) {
    Box boxes[] = makeSomeStationaryThings(n);
    for (Box b : boxes) {
      b.i = 0;
    }
    return boxes;
  }
}

class Box {
  Integer i;
}
