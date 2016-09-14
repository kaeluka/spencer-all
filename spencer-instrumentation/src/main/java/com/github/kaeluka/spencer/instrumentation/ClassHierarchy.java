package com.github.kaeluka.spencer.instrumentation;

import org.objectweb.asm.ClassReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ClassHierarchy {
    private final HashMap<String, String> hierarchy;

    public ClassHierarchy() {
        this.hierarchy = new HashMap<>();
        this.hierarchy.put("java/lang/Object;", null);
    }

    void registerClass(byte classData[]) {
        final ClassReader classReader = new ClassReader(classData);
        final String className = classReader.getClassName();
        final String superName = classReader.getSuperName();
        this.hierarchy.put(className, superName);
    }

    List<String> getSuperClasses(String klass) {
        List<String> ret = new ArrayList<>();
        do {
            ret.add(klass);
            klass = this.hierarchy.get(klass);
        } while (klass != null);
        return ret;
    }

    String getCommonSuperClass(final String a, final String b) {
        if (this.getSuperClasses(a).contains(b)) {
            return b;
        } else if (this.getSuperClasses(b).contains(a)) {
            return a;
        } else {
            assert a != null;
            assert b != null;
            if (!this.hierarchy.containsKey(a) || !this.hierarchy.containsKey(b)) {
                throw new IllegalArgumentException("class hierarchy does not contain" +
                        " both " + a + " and " + b + "\n" +
                        "hierarchy contains: " + this.hierarchy);
            }
            String x = a;
            String y = b;
            while (x != null && !x.equals(y)) {
                x = this.hierarchy.get(x);
                y = this.hierarchy.get(y);
                assert x != null;
                assert y != null;
            }
//            System.out.println("common superclass of " + a + " and " + b + " is " + x);
            if (x != null) {
                return x;
            } else {
                throw new NullPointerException();
            }
        }
    }
}
