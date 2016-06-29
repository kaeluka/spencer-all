package test;
import java.util.LinkedList;

class Lists {
    public static void main(String args[]) {
        System.out.println("hello, lists");
        LinkedList<M> lst = new LinkedList<>();
        for (int i=0; i<2; ++i) {
            final M m = new M(12);
            lst.add(m);
            m.mutate();
        }
    }
}
