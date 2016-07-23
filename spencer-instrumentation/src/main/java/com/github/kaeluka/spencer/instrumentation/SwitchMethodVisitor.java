package com.github.kaeluka.spencer.instrumentation;

import org.objectweb.asm.*;

/**
 * Will delegate all events to the a MethodVisitor, leaving the b MethodVisitor
 * untouched.
 * emitSwitchVMs
 */

/*
class SwitchMethodVisitor extends MethodVisitor {

    private static final int SWITCH_INSN = 0xfe; //IMPDEP_1 opcode
    private boolean isOn = true;

    private MethodVisitor a;
    private MethodVisitor b;

    SwitchMethodVisitor(final MethodVisitor a, final MethodVisitor b) {
        super(Opcodes.ASM5);
        this.a = a;
        this.b = b;
    }

    static void emitSwitchMVs(MethodVisitor mv) {
        if (! (mv instanceof SwitchMethodVisitor)) {
            throw new AssertionError();
        }
        mv.visitInsn(SWITCH_INSN);
    }

    private void switchMVs() {
        final MethodVisitor tmp = this.a;
        this.a = this.b;
        this.b = tmp;
    }

    @Override
    public void visitParameter(final String name, final int access) {
        a.visitParameter(name, access);
    }

    @Override
    public AnnotationVisitor visitAnnotationDefault() {
        return a.visitAnnotationDefault();
    }

    @Override
    public AnnotationVisitor visitAnnotation(final String desc, final boolean visible) {
        return a.visitAnnotation(desc, visible);
    }

    @Override
    public AnnotationVisitor visitTypeAnnotation(final int typeRef, final TypePath typePath, final String desc, final boolean visible) {
        return a.visitTypeAnnotation(typeRef, typePath, desc, visible);
    }

    @Override
    public AnnotationVisitor visitParameterAnnotation(final int parameter, final String desc, final boolean visible) {
        return a.visitParameterAnnotation(parameter, desc, visible);
    }

    @Override
    public void visitAttribute(final Attribute attr) {
        a.visitAttribute(attr);
    }

    @Override
    public void visitCode() {
        a.visitCode();
    }

    @Override
    public void visitFrame(final int type, final int nLocal, final Object[] local, final int nStack, final Object[] stack) {
        a.visitFrame(type, nLocal, local, nStack, stack);
    }

    @Override
    public void visitInsn(final int opcode) {
        if (opcode == SwitchMethodVisitor.SWITCH_INSN) {
            this.switchMVs();
        } else {
            a.visitInsn(opcode);
        }
    }

    @Override
    public void visitIntInsn(final int opcode, final int operand) {
        a.visitIntInsn(opcode, operand);
    }

    @Override
    public void visitVarInsn(final int opcode, final int var) {
        a.visitVarInsn(opcode, var);
    }

    @Override
    public void visitTypeInsn(final int opcode, final String type) {
        a.visitTypeInsn(opcode, type);
    }

    @Override
    public void visitFieldInsn(final int opcode, final String owner, final String name, final String desc) {
        a.visitFieldInsn(opcode, owner, name, desc);
    }

    @Override
    @Deprecated
    public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc) {
        a.visitMethodInsn(opcode, owner, name, desc);
    }

    @Override
    public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc, final boolean itf) {
        a.visitMethodInsn(opcode, owner, name, desc, itf);
    }

    @Override
    public void visitInvokeDynamicInsn(final String name, final String desc, final Handle bsm, final Object... bsmArgs) {
        a.visitInvokeDynamicInsn(name, desc, bsm, bsmArgs);
    }

    @Override
    public void visitJumpInsn(final int opcode, final Label label) {
        a.visitJumpInsn(opcode, label);
    }

    @Override
    public void visitLabel(final Label label) {
        a.visitLabel(label);
    }

    @Override
    public void visitLdcInsn(final Object cst) {
        a.visitLdcInsn(cst);
    }

    @Override
    public void visitIincInsn(final int var, final int increment) {
        a.visitIincInsn(var, increment);
    }

    @Override
    public void visitTableSwitchInsn(final int min, final int max, final Label dflt, final Label... labels) {
        a.visitTableSwitchInsn(min, max, dflt, labels);
    }

    @Override
    public void visitLookupSwitchInsn(final Label dflt, final int[] keys, final Label[] labels) {
        a.visitLookupSwitchInsn(dflt, keys, labels);
    }

    @Override
    public void visitMultiANewArrayInsn(final String desc, final int dims) {
        a.visitMultiANewArrayInsn(desc, dims);
    }

    @Override
    public AnnotationVisitor visitInsnAnnotation(final int typeRef, final TypePath typePath, final String desc, final boolean visible) {
        return a.visitInsnAnnotation(typeRef, typePath, desc, visible);
    }

    @Override
    public void visitTryCatchBlock(final Label start, final Label end, final Label handler, final String type) {
        a.visitTryCatchBlock(start, end, handler, type);
    }

    @Override
    public AnnotationVisitor visitTryCatchAnnotation(final int typeRef, final TypePath typePath, final String desc, final boolean visible) {
        return a.visitTryCatchAnnotation(typeRef, typePath, desc, visible);
    }

    @Override
    public void visitLocalVariable(final String name, final String desc, final String signature, final Label start, final Label end, final int index) {
        a.visitLocalVariable(name, desc, signature, start, end, index);
    }

    @Override
    public AnnotationVisitor visitLocalVariableAnnotation(final int typeRef, final TypePath typePath, final Label[] start, final Label[] end, final int[] index, final String desc, final boolean visible) {
        return a.visitLocalVariableAnnotation(typeRef, typePath, start, end, index, desc, visible);
    }

    @Override
    public void visitLineNumber(final int line, final Label start) {
        a.visitLineNumber(line, start);
    }

    @Override
    public void visitMaxs(final int maxStack, final int maxLocals) {
        a.visitMaxs(maxStack, maxLocals);
    }

    @Override
    public void visitEnd() {
        a.visitEnd();
    }
}

*/