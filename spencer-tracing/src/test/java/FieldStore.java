package test;
import java.util.LinkedList;

public class FieldStore {
    public static void main(String[] args) {
        //System.out.println("======= creating List");
        List lst = null;

        /*
        java.util.LinkedList<Banan> ll = new LinkedList<>();
        for (int i=0; i<3; ++i) {
          ll.add(new Banan());
        }
        //*/

        for (int i=0; i<30; ++i) {
            //System.out.println("=======iteration # " + i);
            List newlst = new List(new Banan(i));
            newlst.nxt = null; // is that it?
            lst = newlst;
        }
//        lst.print();
        //*/

        /*
        for (int i = 1; i < 3; ++i) {
            List newlst = new List();
            newlst.elt = i;
            newlst.nxt = lst;
            lst = newlst;
        }
        //*/

        /*
        int xy = 10;
        try {
            throw new RuntimeException(xy+"");
        } catch (Exception ignore) {
        } finally {
            System.out.println("finally"+xy);
        }
        //*/

        /*
        if (lst != null) {
            System.out.println("======== 1");
            int x = List.i;
            List.i++;
            lst.print();
        }
        //*/
    }
}

class List {
    //public static int i = -10;
    public List nxt;
    public Bananable elt;

    public List(Bananable _elt) {
      //this.elt = _elt;
    }

    public List(Bananable _elt, List _nxt) {
      this.nxt = _nxt;
      this.elt = _elt;
    }

    public void print() {
        System.out.println(this.elt);
        if (this.nxt != null) {
            this.nxt.print();
        }
    }
}

interface Bananable {
    public void printme();
}

//class ClassWithStaticInitialiser {
//  private static int myInt;
//
//  static {
//    System.out.println("initialising "+ClassWithStaticInitialiser.class.toString());
//    myInt = -12;
//  }
//
//  public void print() {
//    System.out.println("myInt = "+myInt);
//  }
//}

class Banan implements Bananable {
  public Banan(int i){
    System.out.println(i);
  }
  public void printme() {
      System.out.println(this);
  }
}

//class MyBanan extends Banan {
//  public MyBanan() {
//    super();
//    System.out.println("creating MyBanan");
//  }
//}
