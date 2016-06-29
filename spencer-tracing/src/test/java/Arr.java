package test;
import java.util.LinkedList;

class Arr {
    int x;
    public static void main(String args[]) {
        System.out.println("Hello, Arrays");
        final int N = 2;

        /////// PRIMITIVE ARRAYS:
        /// immutable booleans, stationary ints, mutable chars
        boolean bs[] = null;
        for (int i=0; i<N; ++i) {
            bs = new boolean[10];
        }

        int ints[] = null;
        for (int i=0; i<N; ++i) {
            ints = new int[10];
            ints[0] = 5;
            ints[3] = 7;
        }

        char cs[] = null;
        for (int i=0; i<N; ++i) {
            cs = new char[10];
            System.out.println(cs[0]);
            cs[0] = 'a';
            cs[3] = 'b';
        }

        /////// REFERENCE ARRAYS:
        /// stationary [test/S, mutable [test/M
        S ss[] = null;
        for (int i=0; i<N; ++i) {
            ss = new S[10];
            for (int j=0; j<ss.length; ++j) {
                ss[j] = new S(j);
            }
            ss[0].x = 12;
            ss[3].x = 12;
        }

        M ms[] = null;
        for (int i=0; i<N; ++i) {
            ms = new M[10];
            for (int j=0; j<ms.length; ++j) {
                ms[j] = new M(j);
            }
            System.out.println(ms[0]);
            ms[1] = new M(10);
            ms[3] = new M(30);
        }

    }
}
