package test;
import java.util.LinkedList;

class Immutable {
    int x;
    public static void main(String args[]) {
        System.out.println("HELLO, MUT");
        final int N = 2;
        //100% of Is shall be immutable:
        I myI = null;
        for (int i=0; i<N; ++i) {
            myI = new I(i);
        }


        //50% of Ms mutated via field access ..
        M myM = null;
        for (int i=0; i<N/2; ++i) {
            myM = new M(i);
            myM.x++;
        }
        //.. and the other 50% are mutated via method call:
        for (int i=N/2; i<N; ++i) {
            myM = new M(i);
            myM.mutate();
        }

        // 100% of Ss are stationary, but not immutable:
        S myS = null;
        for (int i=0; i<N; ++i) {
            myS = new S(i);
            myS.x = i;
            System.out.println(myS.x);
        }
    }

}

class I {
    int x;
    I(int _x) {
        this.x = _x;
    }

    void mutate() {
        x += 12;
    }
}

class S {
    int x;
    S(int _x) {
        this.x = _x;
    }
    void mutate() {
        x += 12;
    }
}

class M {
    int x;
    M(int _x) {
        this.x = _x;
    }
    void mutate() {
        x += 12;
    }
}
