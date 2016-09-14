package com.github.kaeluka.spencer.instrumentation;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;

public class HierarchyClassWriter extends ClassWriter {
    private final ClassHierarchy hierarchy;

    HierarchyClassWriter(final ClassReader classReader,
                         final ClassHierarchy hierarchy) {
        super(classReader, ClassWriter.COMPUTE_FRAMES);
        this.hierarchy = hierarchy;
    }

    @Override
    protected String getCommonSuperClass(final String type1, final String type2) {
        try {
            return super.getCommonSuperClass(type1, type2);
        } catch (RuntimeException ex1) {
            //System.out.println("delegating to ClassHierarchy due to");
            try {
                return this.hierarchy.getCommonSuperClass(type1, type2);
            } catch (Exception ex2) {
                //System.out.println("threw again, returning java/lang/Object");
                //System.exit(1);
                return "java/lang/Object";
            }
        }
    }
}
