package com.github.kaeluka.spencer.instrumentation;

import org.objectweb.asm.*;
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.AnalyzerAdapter;
import org.objectweb.asm.commons.JSRInlinerAdapter;

import java.util.Arrays;
import java.util.List;


public class InstrumentationVisitor extends ClassVisitor implements Opcodes {
    private String classname;
    private String sourceFileName;

    public InstrumentationVisitor(ClassVisitor cv) {
        super(Opcodes.ASM5, cv);
    }

    public String getClassName() {
        return this.classname;
    }

    @Override
    public void visitSource(String source, String debug) {
        this.sourceFileName = source;
        super.visitSource(source, debug);
    }

    protected String getSourceFileName() {
        return this.sourceFileName;
    }

    private static boolean isArrayType(String desc) {
        return desc.charAt(0) == '[';
    }

    private static boolean isObjectType(String desc) {
        //			System.out.println(((desc.charAt(0) == 'L') ? "object type: " : "not object type: ") + desc);
        return desc.charAt(0) == 'L';
    }

    @Override
    public void visit(int version, int access, String name, String signature,
                      String superName, String[] interfaces) {
        this.classname = Type.getType(name).getInternalName().replace('.', '/');
        super.visit(version, access, name, signature, superName, interfaces);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
                                     String signature, String[] exceptions) {
        MethodVisitor acc = super.visitMethod(access, name, desc, signature,
                exceptions);

        if ((access & Opcodes.ACC_SYNTHETIC) != 0) {
            return acc;
        }

        if (Instrument.instrumentMethods) {
            // TODO: connecting the analyzer should be factored out (duplicated below)!
            final MethodMV methodMv = new MethodMV(acc, this.getClassName(), name,
                    access, desc);
            MethodVisitor exitHandler = ExitHandler.mk(
                    methodMv, access, name,
                    desc, signature, exceptions, (mv, reason) -> {
                        mv.visitLdcInsn("reason for exit: "+reason);
                        mv.visitInsn(POP);
                        mv.visitLdcInsn(name);
                        mv.visitLdcInsn(this.getClassName());
                        mv.visitMethodInsn(INVOKESTATIC, "NativeInterface", "methodExit",
                                "(Ljava/lang/String;Ljava/lang/String;)V", false);
                    });
            AnalyzerAdapter analyzer = new AnalyzerAdapter(this.classname,
                    access, name, desc, exitHandler);
            methodMv.setAnalyzer(analyzer);
            acc = analyzer;
        }

        if (Instrument.instrumentVars) {
            VarMV varmv = new VarMV(acc, this.getClassName(), name, access,
                    desc);
            AnalyzerAdapter analyzer = new AnalyzerAdapter(this.classname,
                    access, name, desc, varmv);
            varmv.setAnalyzer(analyzer);
            acc = analyzer;
        }
        if (Instrument.instrumentFields) {
            FieldMV fieldmv = new FieldMV(acc, this.getClassName(), name,
                    access, desc);
            AnalyzerAdapter analyzer = new AnalyzerAdapter(this.classname,
                    access, name, desc, fieldmv);
            fieldmv.setAnalyzer(analyzer);
            acc = analyzer;
        }

        return new JSRInlinerAdapter(acc, access, name, desc, signature,
                exceptions);
    }

    /* (non-Javadoc)
     * @see org.objectweb.asm.ClassVisitor#visitEnd()
     */
    @Override
    public void visitEnd() {
        super.visitEnd();
    }


    public class InstrumentationMV extends AdviceAdapter {
        private final String methodname;
        private final String signature;
        private final String classDescr;


        // sometimes, we need to track the variables that have been assigned, as
        // otherwise, we might log the old value on unitialised
        // vars. The JVM doesn't want this.
        AnalyzerAdapter analyzer;

        private int lastVisitedLine = -1;
        protected String getClassDescr() {
            return this.classDescr;
        }

        protected String getLastVisitedLocation() {
            return "("+InstrumentationVisitor.this.getSourceFileName()+":"+this.lastVisitedLine
                    + ")";
        }

        @SuppressWarnings("unused")
        protected void emitPrintTopOfStack() {
            this.emitPrintTopOfStack("out");
        }

        protected void emitPrintTopOfStack(String stream) {
            // .. top
            super.visitInsn(DUP); // .. top top
            super.visitFieldInsn(GETSTATIC, "java/lang/System", stream, "Ljava/io/PrintStream;"); // .. top top out
            super.visitInsn(SWAP); // .. top out top
            super.visitMethodInsn(INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/Object;)V", false);
            // .. top
        }

        protected String getMethodName() {
            return this.methodname;
        }

        /* (non-Javadoc)
         * @see org.objectweb.asm.MethodVisitor#visitLineNumber(int, org.objectweb.asm.Label)
         */
        @Override
        public void visitLineNumber(int line, Label start) {
            this.lastVisitedLine = line;
            super.visitLineNumber(line, start);
        }


        protected boolean isStatic() {
            return (this.methodAccess & ACC_STATIC) != 0;
        }

        protected String getMethodSignature() {
            return this.signature;
        }

        public void setAnalyzer(AnalyzerAdapter analyzer) {
            this.analyzer = analyzer;
        }

        //		protected void pushClassDescr() {
        //			super.visitLdcInsn(this.classDescr);
        //		}

        protected void runtimeWarning(String c) {
            final String msg = "====== " + this.getClass().getSimpleName()+" ("+InstrumentationVisitor.this.getSourceFileName()+":"+this.lastVisitedLine
                    + ") WARNING: " + c;
            this.emitPrintln(msg);
        }

        protected void instrumentationWarning(String c) {
            final String msg = "====== " + this.getClass().getSimpleName()+" ("+InstrumentationVisitor.this.getSourceFileName()+":"+this.lastVisitedLine
                    + ") WARNING: " + c;
            if (Instrument.enableComments && Instrument.loudWarnings) {
                System.err.println(msg);
            }
        }

        protected void emitPrintln(final String msg) {
            super.visitLdcInsn(msg);
            this.emitPrintTopOfStack("err");
            super.pop();
        }

        protected void comment(String c) {
            final String msg = "====== " + this.getClass().getSimpleName()+" ("+InstrumentationVisitor.this.getSourceFileName()+":"+this.lastVisitedLine
                    + ") COMMENT: " + c;
            if (Instrument.enableComments) {
                if (Instrument.loudWarnings) {
                    System.out.println(msg);
                }
                super.visitLdcInsn(msg);
                super.visitInsn(POP);
            }
        }

        protected void pushThisKindAndObj() {
//            throw new RuntimeException("no locals in "+this.getClassDescr()+"::"+this.getMethodName());
            if (this.analyzer.locals == null) {
                //This code is unreachable
                if (this.isStatic()) {
                    super.visitLdcInsn(Instrument.SPECIAL_VAL_STATIC);
                    super.visitInsn(ACONST_NULL);
                } else {
//					if ("<init>".equals(this.getMethodName())) {
//						super.visitLdcInsn(Instrument.SPECIAL_VAL_THIS);
//                      super.visitInsn(ACONST_NULL);
//					} else {
                    super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                    super.visitVarInsn(ALOAD, 0);
//					}
                }

            } else
            if (this.isStatic()) {
                super.visitLdcInsn(Instrument.SPECIAL_VAL_STATIC);
                super.visitInsn(ACONST_NULL);
            } else if (getTypeOfLocal(0) == UNINITIALIZED_THIS) {
                super.visitLdcInsn(Instrument.SPECIAL_VAL_THIS);
                super.visitInsn(ACONST_NULL);
            } else {
                assert (!this.isStatic());
                super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                super.visitVarInsn(ALOAD, 0);
            }
        }

        /**
         * @param idx
         */
        public Object getTypeOfLocal(final int idx) {
            return this.analyzer.locals.get(idx);
        }

        /**
         * pushes the kind (SPECIAL_VAL_XXX) of the given var and then the value
         * (if needed)
         *
         * @param var
         *            the varible index
         */
        protected void pushKindAndObjectInVar(int var) {
            comment("setting up pushing var " + var);
            comment("stack  = " + this.analyzer.stack);
            if (this.analyzer.locals != null) {
                comment("locals = " + this.analyzer.locals);
                comment("size   = " + this.analyzer.locals.size());
            }
            assert (this.analyzer != null);
            if (this.analyzer.locals == null) {
                // The instruction is unreachable. Do whatever!
                super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                super.visitInsn(ACONST_NULL);
                return;
            }

            final Object type = var < this.analyzer.locals.size() ? this.analyzer.locals.get(var) : ".";

            if (type == UNINITIALIZED_THIS) {
                super.visitLdcInsn(Instrument.SPECIAL_VAL_THIS);
                super.visitInsn(ACONST_NULL);
                return;
            } else if (type == Opcodes.NULL) {
                super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                super.visitInsn(ACONST_NULL);
            } else if (type instanceof String && ((String) type).equals(".")) {
                super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                super.visitInsn(ACONST_NULL);
            } else {
                if ((type != INTEGER) && (type != FLOAT) && (type != DOUBLE)
                        && (type != LONG) && (type != NULL)
                        && (type != UNINITIALIZED_THIS) && (type != TOP)) {
                    super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                    super.visitVarInsn(ALOAD, var);
                } else {
                    if (type == TOP) {
                        // the value is unitialised! Emit NULL!
                        super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                        super.visitInsn(ACONST_NULL);
                    } else if (type == UNINITIALIZED_THIS) {
                        super.visitLdcInsn(Instrument.SPECIAL_VAL_THIS);
                        super.visitInsn(ACONST_NULL);
                    } else {
                        runtimeWarning("can not instrument primitives (type = "+type+")! - "+(type != INTEGER));
                        throw new RuntimeException("can not instrument primitive types");
                    }
                }

            }
        }

        protected void pushKindAndObjectAtDepth(int depth) {
            assert (this.analyzer != null);
            if (this.analyzer.stack == null) {
                // The instruction is unreachable. Do whatever!
                super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                super.visitInsn(ACONST_NULL);
                return;
            }

            final Object type = this.analyzer.stack.get(this.analyzer.stack
                    .size() - 1 - depth);
            if (type == UNINITIALIZED_THIS) {
                super.visitLdcInsn(Instrument.SPECIAL_VAL_THIS);
                super.visitInsn(ACONST_NULL);
                return;
            }/*
			 * else if (this.isStatic()) {
			 * super.visitLdcInsn(Instrument.SPECIAL_VAL_STATIC);
			 * super.visitInsn(ACONST_NULL); return; }
			 */else {
                switch (depth) {
                    case 0:
                        super.visitInsn(DUP);
                        break;
                    case 1:
                        super.visitInsn(DUP2);
                        super.visitInsn(POP);
                    default:
                        throw new RuntimeException(
                                "don't know how to handle depth " + depth);
                }
                super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                super.visitInsn(SWAP);
            }
        }

        protected void pushCallerClassStr() {
            super.visitLdcInsn(getClassName());
        }

        protected void pushFakeKindAndVal() {
            super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
            this.visitLdcInsn(ACONST_NULL);
        }

        @Deprecated
        protected void pushThreadObj() {
        }

        public InstrumentationMV(MethodVisitor mv, String _classDescr,
                                 String name, int access, String signature) {
            super(Opcodes.ASM5, mv, access, name, signature);
            this.classDescr = Type.getType(_classDescr).getInternalName();
            this.methodname = name;
            this.signature = signature;
        }
    }

    public class FieldMV extends InstrumentationMV {
        public FieldMV(MethodVisitor mv, String classDescr, String name,
                       int access, String sig) {
            super(mv, classDescr, name, access, sig);
        }

        @Override
        public void visitInsn(int opcode) {
            int valSize = 1;

            switch (opcode) {
                case AASTORE:
                    emitStoreArrayA();
                    break;
                case LASTORE:
                case DASTORE:
                    valSize = 2;
                case IASTORE:
                case FASTORE:
                case BASTORE:
                case CASTORE:
                case SASTORE:
                    emitStoreArrayPrimitive(valSize);
                    break;
                case AALOAD:
                    emitLoadArrayA();
                    break;
                case LALOAD:
                case IALOAD:
                case FALOAD:
                case DALOAD:
                case BALOAD:
                case CALOAD:
                case SALOAD:
                    emitLoadArrayPrimitive();
                    break;
            }
            super.visitInsn(opcode);
        }

        private void emitLoadArrayPrimitive() {
            // .. arr, idx
            super.visitInsn(DUP2);
            // .. arr, idx, arr, idx
            this.pushThisKindAndObj();
            // .. arr, idx, arr, idx, callerValKind, caller
            this.pushCallerClassStr();
            // .. arr, idx, arr, idx, callerValKind, caller, callerClass
            super.visitMethodInsn(INVOKESTATIC,
                    "NativeInterface", "readArray", "("
                            + "Ljava/lang/Object;"  // arr
                            + "I"                   // idx
                            + "I"                   // callerValKind
                            + "Ljava/lang/Object;"  // caller
                            + "Ljava/lang/String;"  // callerClass
                            +")V",
                    false);
        }

        private void emitStoreArrayPrimitive(int valSize) {
            if (valSize == 1) {
                // .. arr, idx, val
                super.visitInsn(DUP_X2);
                super.visitInsn(POP);
                // .. val, arr, idx
            } else {
                // .. arr, idx, val1, val2
                super.visitInsn(DUP2_X2);
                super.visitInsn(POP2);
                // .. val1, val2, arr, idx
            }
            // .. [val1, val2 | val], arr, idx
            super.visitInsn(DUP2);
            // .. [val1, val2 | val], arr, idx, arr, idx
            this.pushThisKindAndObj();
            // .. [val1, val2 | val], arr, idx, arr, idx, kind, caller
            this.pushCallerClassStr();
            // .. [val1, val2 | val], arr, idx, arr, idx, kind, caller, callerClass
            super.visitMethodInsn(INVOKESTATIC,
                    "NativeInterface", "modifyArray", "("
                            + "Ljava/lang/Object;"  // arr
                            + "I"                   // idx
                            + "I"                   // callerValKind
                            + "Ljava/lang/Object;"  // caller
                            + "Ljava/lang/String;"  // callerClass
                            +")V",
                    false);
            if (valSize == 1) {
                // .. val, arr, idx
                super.visitInsn(DUP2_X1);
                super.pop2();
            } else {
                // .. val1, val2, arr, idx
                super.visitInsn(DUP2_X2);
                super.visitInsn(POP2);
            }
        }

        private void emitStoreArrayA() {
            final List<Object> stack = ((AnalyzerAdapter)mv).stack;
            String holderClass;
            if (stack != null) {
                holderClass = (String)stack.get(stack.size()-3);
            } else {
//				runtimeWarning("this code should be unreachable (stack is null)");
                holderClass = "[Ljava/lang/Object;";
            }
            if (! holderClass.startsWith("[") && stack != null) {
                throw new IllegalStateException("callee class must be an array!");
            }
            // .. arr, idx, newVal
            super.visitInsn(DUP_X2);
            // .. newVal, arr, idx, newVal
            super.visitInsn(DUP_X2);
            super.visitInsn(POP);
            // .. newVal, newVal, arr, idx
            super.visitInsn(DUP2_X2);
            // .. arr, idx, newVal, newVal, arr, idx
            super.visitInsn(DUP2);
            super.visitInsn(AALOAD);
            // .. arr, idx, newVal, newVal, arr, idx, oldVal
            this.visitLdcInsn(holderClass);
            this.pushMethodNameStr();
            this.pushCallerClassStr();
            this.pushThisKindAndObj();
            // .. arr, idx, newVal, newVal, arr, idx, oldVal, holderClass, methName, callerClass, callerKind, caller
            super.visitMethodInsn(INVOKESTATIC, "NativeInterface", "storeArrayA", "("
                    +"Ljava/lang/Object;"  // newVal
                    +"[Ljava/lang/Object;" // arr
                    +"I"                   // idx
                    +"Ljava/lang/Object;"  // oldVal
                    +"Ljava/lang/String;"  // holderClass,
                    +"Ljava/lang/String;"  // callerMethod,
                    +"Ljava/lang/String;"  // callerClass,
                    +"I"                   // callerValKind,
                    +"Ljava/lang/Object;"  // caller,
                    +")V", false);
        }

        private void emitLoadArrayA() {
            final List<Object> stack = ((AnalyzerAdapter)mv).stack;
            String holderClass;
            if (stack != null) {
                holderClass = (String)stack.get(stack.size()-2);
            } else {
//				runtimeWarning("this code should be unreachable (stack is null)");
                holderClass = "[Ljava/lang/Object;";
            }
            if (! holderClass.startsWith("[") && stack != null) {
                throw new IllegalStateException("callee class must be an array!");
            }
            // .. arrayref, index
            super.visitInsn(DUP2);
            // .. arrayref, index, arrayref, index
            super.visitInsn(DUP2);
            super.visitInsn(AALOAD);
            // .. arrayref, index, arrayref, index, val
            this.visitLdcInsn(holderClass);
            // .. arrayref, index, arrayref, index, val, holderClass
            this.visitLdcInsn(this.getMethodName());
            // .. arrayref, index, arrayref, index, val, holderClass, callerMethod
            this.pushCallerClassStr();
            // .. arrayref, index, arrayref, index, val, holderClass, callerMethod, callerClass
            this.pushThisKindAndObj();
            // .. arrayref, index, arrayref, index, val, holderClass, callerMethod, callerClass, callerValKind, caller
            super.visitMethodInsn(INVOKESTATIC, "NativeInterface", "loadArrayA",
                    "("
                            + "[Ljava/lang/Object;"// arr
                            + "I" 				   // idx
                            + "Ljava/lang/Object;" // val
                            + "Ljava/lang/String;" // holderClass
                            + "Ljava/lang/String;" // callerMethod
                            + "Ljava/lang/String;" // callerClass
                            + "I"                  // callerValKind
                            + "Ljava/lang/Object;" // caller
                            +")V",
                    false);
        }


        @Override
        public void visitFieldInsn(int opcode, String owner, String name,
                                   String desc) {
            if (name.contains("$")) {
                comment("skipping access to synthetic field: '" + name + "'");
            } else {
                // when stack is null, the insn is unreachable
                if (this.analyzer.stack != null) {
                    boolean fieldIsStatic = false;
                    switch (opcode) {
                        case GETSTATIC:
                            fieldIsStatic = true;
                        case GETFIELD:
                        {
                            if (isObjectType(desc) || isArrayType(desc)) {
                                if ((this.analyzer.stack.size() > 0 && this.analyzer.stack.get(0)!=UNINITIALIZED_THIS) ||
                                        this.analyzer.stack.size() == 0) {
                                    this.emitLoadFieldACode(owner, name, desc, fieldIsStatic);
                                } else {
//								runtimeWarning("not instrumenting field '"+name+"' due to being loaded before super(...) call.");
                                }
                            } else {
                                emitReadCode(owner, name, desc, fieldIsStatic);
                            }
                            break;
                        }
                        case PUTSTATIC:
                            fieldIsStatic = true;
                        case PUTFIELD: {
                            if (isObjectType(desc) || isArrayType(desc)) {
                                // the field is a reference type
                                comment("Setting up call to STOREFIELD("
                                        + (fieldIsStatic ? "static"
                                        : "non-static") + "), field name: " + name);
                                emitStoreFieldACode(owner, name, desc,
                                        fieldIsStatic);
                            } else {
                                emitModifyCode(owner, name, desc, fieldIsStatic);
                            }
                            break;
                        }
                        default:
                            break;
                    }
                }
            }
            super.visitFieldInsn(opcode, owner, name, desc);
        }

        private void emitReadCode(String owner, String name, String desc,
                                  boolean fieldIsStatic) {
            emitReadModifyCode(owner, name, desc, fieldIsStatic, true);
        }

        private void emitModifyCode(String owner, String name, String desc,
                                    boolean fieldIsStatic) {
            emitReadModifyCode(owner, name, desc, fieldIsStatic, false);
        }

        private void emitReadModifyCode(String owner, String name, String desc,
                                        boolean fieldIsStatic, boolean read) {
            // the field is a primitive type
            final String opName = (read)?"read":"modify";
            comment("Setting up call to "+opName+"ing "+name+" ("
                    + desc + ")");
            //			System.out.println("emitting "+opName+": "+owner+", "+name+", "+desc+", "+((fieldIsStatic)?"static":"not static"));
            // stack: .. callee newVal?
            if (!read && !fieldIsStatic) {
                //we ALSO have the new value on the stack
                // stack: .. callee newVal
                this.comment("top:"+this.analyzer.stack.get(this.analyzer.stack.size()-1));
                this.swap(Type.getType("Ljava/lang/Object;"),Type.getType(desc));
            }
            // stack: .. newVal? callee
            // Object calleeValKind, callee
            if (!fieldIsStatic) {
                super.visitInsn(DUP); // callee, callee
                super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL); // callee, newval, callee, SPECIAL_VAL_NORMAL
                super.visitInsn(SWAP); // callee, newval, SPECIAL_VAL_NORMAL, callee
            } else {
                super.visitLdcInsn(Instrument.SPECIAL_VAL_STATIC);
                super.visitInsn(ACONST_NULL);
            }
            // stack: .. newVal?, callee, calleeValKind, callee
            // String calleeclass,
            super.visitLdcInsn(owner);
            // String fname
            super.visitLdcInsn(name);
            // int callerValKind, Object caller,
            pushThisKindAndObj();
            // String callerclass,
            pushCallerClassStr();
            super.visitMethodInsn(INVOKESTATIC,
                    "NativeInterface", opName, "("
                            + "I"
                            + "Ljava/lang/Object;"
                            + "Ljava/lang/String;"
                            + "Ljava/lang/String;"
                            + "I"
                            + "Ljava/lang/Object;"
                            + "Ljava/lang/String;"
                            +")V",
                    false);
            if (!read && !fieldIsStatic) {
                this.swap(Type.getType(desc),Type.getType("Ljava/lang/Object;"));
            }
            comment("done with call to "+opName);
        }

        private void emitLoadFieldACode(String holderClass, String fName,
                                        String type, boolean fieldIsStatic) {
            if (fieldIsStatic) {

                //Object value,
                super.visitFieldInsn(GETSTATIC, holderClass, fName, type);
                //int holderValKind,
                super.visitLdcInsn(Instrument.SPECIAL_VAL_STATIC);
                //Object holder,
                super.visitInsn(ACONST_NULL);
                //String holderClass,
                super.visitLdcInsn(holderClass);
                //String fieldName,
                super.visitLdcInsn(fName);
                //String type,
                super.visitLdcInsn(type);
                //String callerClass,
                this.pushCallerClassStr();
                //String callerMethod,
                this.pushMethodNameStr();
                //int callerValKind, Object caller,
                this.pushThisKindAndObj();
                super.visitMethodInsn(INVOKESTATIC, "NativeInterface", "loadFieldA",
                        "("+
                                "Ljava/lang/Object;"   // val
                                + "I"                  // holderValKind
                                + "Ljava/lang/Object;" // holder
                                + "Ljava/lang/String;" // holderclass
                                + "Ljava/lang/String;" // fname
                                + "Ljava/lang/String;" // type
                                + "Ljava/lang/String;" // callerclass
                                + "Ljava/lang/String;" // callermethod
                                + "I"                  // callerValkind
                                + "Ljava/lang/Object;" // caller
                                +")V",
                        false);

            } else {
                //..holder
//				System.out.println("..holder "+((AnalyzerAdapter)mv).stack);
                if (this.analyzer.stack.get(this.analyzer.stack.size()-1) != UNINITIALIZED_THIS) {
                    super.visitInsn(DUP);//..holder,holder
                    super.visitFieldInsn(Opcodes.GETFIELD, holderClass, fName, type);//..holder,val
                    super.visitInsn(DUP2); //..holder,val,holder,val
                    super.visitInsn(POP);  //..holder,val,holder
                    super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL); //..holder,val,holder,SPECIAL_VAL_NORMAL
                    super.visitInsn(SWAP); //..holder,val,SPECIAL_VAL_NORMAL,holder
//					System.out.println("was init");
                } else {
                    super.visitInsn(DUP);//..holder,holder
                    super.visitFieldInsn(Opcodes.GETFIELD, holderClass, fName, type);//..holder,val
                    //..holder,val
                    super.visitLdcInsn(Instrument.SPECIAL_VAL_THIS); //..holder,val,SPECIAL_VAL_THIS
                    super.visitInsn(ACONST_NULL);                    //..holder,val,SPECIAL_VAL_THIS,NULL
                    System.out.println("was uninit");
                }
                //..holder,val,SPECIAL_VAL_xxx,NULL|holder,holder
                final String mName = this.getMethodName();
                final String loc = this.getLastVisitedLocation();

                super.visitLdcInsn(holderClass); //..holder,val,SPECIAL_VAL_xxx,NULL|holder,holderclass
                super.visitLdcInsn(fName);       //..holder,val,SPECIAL_VAL_xxx,NULL|holder,holderclass,fname
                super.visitLdcInsn(type);        //..holder,val,SPECIAL_VAL_xxx,NULL|holder,holderclass,fname,type
                pushCallerClassStr();            //..holder,val,SPECIAL_VAL_xxx,NULL|holder,holderclass,fname,type,callerclass
                pushMethodNameStr();             //..holder,val,SPECIAL_VAL_xxx,NULL|holder,holderclass,fname,type,callerclass,mname
                pushThisKindAndObj();            //..holder,val,SPECIAL_VAL_xxx,NULL|holder,holderclass,fname,type,callerclass,mname,thiskind,this
                mv.visitMethodInsn(INVOKESTATIC,
                        "NativeInterface", "loadFieldA",
                        "("
                                + "Ljava/lang/Object;" // val
                                + "I"                  // holderValKind
                                + "Ljava/lang/Object;" // holder
                                + "Ljava/lang/String;" // holderclass
                                + "Ljava/lang/String;" // fname
                                + "Ljava/lang/String;" // type
                                + "Ljava/lang/String;" // callerclass
                                + "Ljava/lang/String;" // callermethod
                                + "I"                  // callerValkind
                                + "Ljava/lang/Object;" // caller
                                +")V",
                        false);
                comment("done with call to LOADFIELD");

            }
        }

        private void pushMethodNameStr() {
            super.visitLdcInsn(this.getMethodName());
        }

        private void emitStoreFieldACode(String owner, String name,
                                         String desc, boolean fieldIsStatic) {
            if (!fieldIsStatic) {
                if ("barbar".equals(name)) {
                    System.out.println("here we go"+this.getMethodName());
                }
                // STACK: .. ownerobj, newval
                final Object newValType = ((AnalyzerAdapter)this.mv).stack
                        .get(((AnalyzerAdapter)this.mv).stack.size() - 1);
                if (newValType != UNINITIALIZED_THIS) {
                    comment("target is not uninit_this");
                    comment("stack=" + this.analyzer.stack);
                    super.visitInsn(DUP2);
                    // STACK: .. ownerobj, newval, ownerobj, newval
                    super.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                    // STACK: .. ownerobj, newval, ownerobj, newval, SPECIAL_VAL_NORMAL
                    super.visitInsn(DUP_X2);
                    // STACK: .. ownerobj, newval, SPECIAL_VAL_NORMAL, ownerobj, newval, SPECIAL_VAL_NORMAL
                    super.visitInsn(POP);
                    // STACK: .. ownerobj, newval, SPECIAL_VAL_NORMAL, ownerobj, newval
                } else {
                    super.dup();
                    super.visitInsn(ACONST_NULL);
                    // STACK: .. ownerobj, newval, newval, newval, null
                    super.swap();
                    // STACK: .. ownerobj, newval, newval, null, newval
                    super.visitLdcInsn(Instrument.SPECIAL_VAL_THIS);
                    // STACK: .. ownerobj, newval, newval, null, newval, SPECIAL_VAL_THIS
                    super.dupX2();
                    // STACK: .. ownerobj, newval, newval, SPECIAL_VAL_THIS, null, newval, SPECIAL_VAL_THIS
                    super.pop();
                    // STACK: .. ownerobj, newval, newval, SPECIAL_VAL_THIS, null, newval
                }
                // STACK: .. ownerobj, newval, holderkind,
                // holderobj, newval
                // Object old_val:
                if (this.getMethodName().equals("<init>")) {
                    super.visitInsn(ACONST_NULL);
                } else {
                    if (this.analyzer.stack
                            .get(this.analyzer.stack.size() - 2) != UNINITIALIZED_THIS) {
                        super.visitInsn(DUP2);
                        super.visitInsn(POP);
                        super.visitFieldInsn(GETFIELD,
                                owner, name, desc);
                    } else {
                        throw new RuntimeException("can't instrument putfield's old_val (is `this`)");
                        //											super.visitInsn(ACONST_NULL);
                    }
                }
                // STACK: .. ownerobj, newval,
                // SPECIAL_VAL_NORMAL, ownerobj, newval,
                // oldval
            } else {
                // STACK: .. newval
                // Object holder + Object newval:
                super.visitInsn(DUP);
                super.visitInsn(ACONST_NULL);
                super.visitInsn(SWAP);
                // STACK: .. newval ownerobj=NULL newval

                // holderKind
                super.visitLdcInsn(Instrument.SPECIAL_VAL_STATIC);
                // STACK: .. newval ownerobj=NULL newval
                // SPECIAL_VAL_STATIC
                super.visitInsn(DUP_X2);
                super.visitInsn(POP);
                // STACK: .. newval SPECIAL_VAL_STATIC
                // ownerobj=NULL newval

                // Object old_val
                super.visitFieldInsn(GETSTATIC, owner,
                        name, desc);
                // STACK: .. newval SPECIAL_VAL_STATIC
                // ownerobj=NULL newval oldval
            }
            // String holderclass
            super.visitLdcInsn(owner);
            // String fname
            super.visitLdcInsn(name);
            // String type
            super.visitLdcInsn(desc);
            // String callerclass
            pushCallerClassStr();
            // String callermethod
            pushMethodNameStr();
            // Object caller
            pushThisKindAndObj();

            super.visitMethodInsn(INVOKESTATIC,
                    "NativeInterface", "storeFieldA",
                    "("
                            + "I"                  // holderValKind
                            + "Ljava/lang/Object;" // holder
                            + "Ljava/lang/Object;" // newval
                            + "Ljava/lang/Object;" // oldval
                            + "Ljava/lang/String;" // holderclass
                            + "Ljava/lang/String;" // fname
                            + "Ljava/lang/String;" // type
                            + "Ljava/lang/String;" // callerclass
                            + "Ljava/lang/String;" // callermethod
                            + "I"                  // callerValkind
                            + "Ljava/lang/Object;" // caller
                            +")V",
                    false);
            comment("done with call to STOREFIELD");
        }

        //		@Override
        //		public void visitMaxs(int stack, int locals) {
        //			// overapproximation
        //			super.visitMaxs(stack + 40, locals + 10);
        //		}
    }

    private class MethodMV extends InstrumentationMV {

        public MethodMV(MethodVisitor mv, String classDescr, String name,
                        int access, String sig) {
            super(mv, classDescr, name, access, sig);
        }

        @Override
        public void onMethodEnter() {
            if ("<init>".equals(this.getMethodName())) {
                this.visitVarInsn(ALOAD,0);
                super.visitLdcInsn(this.getClassDescr());
                super.visitMethodInsn(INVOKESTATIC, "NativeInterface",
                        "afterInitMethod",
                        "(Ljava/lang/Object;"
                                + "Ljava/lang/String;"
                                +")V",
                        false);
            }
//            emitMethodEnter();
        }

        private void emitMethodEnter() {
            comment("setting up call to METHODENTER "+this.getClassDescr()+"::"+this.getMethodName()+this.getMethodSignature());
            // ///// generate call to `methodEnter`
            // String name
            super.visitLdcInsn(this.getMethodName());
            // String signature
            if (this.getMethodSignature() == null) {
                super.visitLdcInsn("<none available>");
            } else {
                super.visitLdcInsn(this.getMethodSignature());
            }

            // String calleeclass
            super.visitLdcInsn(InstrumentationVisitor.this.classname);
            // Object calleeValKind, callee
            pushThisKindAndObj();
            pushReftypeArgs();
            // call methodEnter:
            super.visitMethodInsn(INVOKESTATIC, "NativeInterface",
                    "methodEnter", "(Ljava/lang/String;" // name
                            + "Ljava/lang/String;"   // signature
                            + "Ljava/lang/String;"   // calleeclass
                            + "I"                    // calleeValKind
                            + "Ljava/lang/Object;"   // callee
                            + "[Ljava/lang/Object;"  // args
                            +")V" // thread
                    , false);
        }

        private void pushReftypeArgs() {
            final List<Type> params = Arrays.asList(Type.getArgumentTypes(this.getMethodSignature()));
            comment("loading reftype args locals="+params);

            if (params == null) {
                // this location is unreachable!
                super.visitLdcInsn(0);
                super.visitTypeInsn(ANEWARRAY, "java/lang/Object");
                return;
            }
            final int firstParam = this.isStatic() ? 0 : 1;
            // we make an array that's large enough for 'this', but 'this' will always be
            // null. This allows the native interface to recover the original var index
            // easily:
            super.visitLdcInsn(params.size() + (this.isStatic() ? 0 : 1));
            int doubleSizeVars = 0;
            super.visitTypeInsn(ANEWARRAY, "java/lang/Object");
            for (int slot = 0; slot < params.size(); ++slot) {
                Type type = params.get(slot);
                super.visitInsn(DUP);
                super.visitIntInsn(SIPUSH, firstParam+slot);
                if (type instanceof Type) {
                    if (type.toString().length() == 1 /*it's a primitive*/) {
                        super.visitInsn(ACONST_NULL);
                    } else {
                        super.visitVarInsn(ALOAD, firstParam+slot+doubleSizeVars);
                    }
                    if (((Type)type).getSize() == 2) {
                        doubleSizeVars++;
                    }
                } else {
                    comment("type "+type+" is not a string, it's a "+type.getClass());
                    super.visitInsn(ACONST_NULL);
                }
                super.visitInsn(AASTORE);
            }
            comment("loading reftype args: done");
        }
        /* (non-Javadoc)
         * @see org.objectweb.asm.commons.AdviceAdapter#visitIntInsn(int, int)
         */
        @Override
        public void visitIntInsn(int opcode, int operand) {
            // fake ctor call to primitive array
            super.visitIntInsn(opcode, operand);
            if (opcode == NEWARRAY) {
                String arrayType;
                switch (operand) {
                    case T_BOOLEAN:
                        arrayType = "[Z";
                        break;
                    case T_CHAR:
                        arrayType = "[C";
                        break;
                    case T_BYTE:
                        arrayType = "[B";
                        break;
                    case T_FLOAT:
                        arrayType = "[F";
                        break;
                    case T_LONG:
                        arrayType = "[J";
                        break;
                    case T_DOUBLE:
                        arrayType = "[D";
                        break;
                    case T_SHORT:
                        arrayType = "[S";
                        break;
                    case T_INT:
                        arrayType = "[I";
                        break;
                    default:
                        throw new IllegalArgumentException("don't understand primitive type operand "+operand);
                }
                super.visitInsn(DUP);
                super.visitLdcInsn(arrayType);
                super.visitMethodInsn(INVOKESTATIC, "NativeInterface",
                        "afterInitMethod",
                        "(Ljava/lang/Object;"
                                + "Ljava/lang/String;"
                                +")V",
                        false);
                super.visitInsn(DUP);
                super.visitLdcInsn("<init>");
                super.visitInsn(SWAP);
                super.visitLdcInsn("(I)V");
                super.visitInsn(SWAP);
                this.visitLdcInsn(arrayType);
                super.visitInsn(SWAP);
                this.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                super.visitInsn(SWAP);
                this.visitInsn(ACONST_NULL);
                super.visitMethodInsn(INVOKESTATIC, "NativeInterface",
                        "methodEnter",
                        "(Ljava/lang/String;"    // name
                                + "Ljava/lang/String;"   // signature
                                + "Ljava/lang/String;"   // calleeclass
                                + "I"                    // calleeValKind
                                + "Ljava/lang/Object;"   // callee
                                + "[Ljava/lang/Object;"  // args
                                +")V" // thread
                        , false);
            }
        }

        @Override
        public void visitTypeInsn(final int opcode, final String type) {
            super.visitTypeInsn(opcode, type);
            switch (opcode) {
                case ANEWARRAY:
                    super.visitInsn(DUP);
                    super.visitLdcInsn("[Ljava/lang/Object;");
                    super.visitMethodInsn(INVOKESTATIC, "NativeInterface",
                            "afterInitMethod",
                            "(Ljava/lang/Object;"
                                    + "Ljava/lang/String;"
                                    +")V",
                            false);
                    super.visitInsn(DUP);
                    super.visitLdcInsn("<init>");
                    super.visitInsn(SWAP);
                    super.visitLdcInsn("(I)V");
                    super.visitInsn(SWAP);
                    if (isArrayType(type)) {
                        this.visitLdcInsn("["+type+";");
                    } else {
                        this.visitLdcInsn("[L"+type+";");
                    }
                    super.visitInsn(SWAP);
                    this.visitLdcInsn(Instrument.SPECIAL_VAL_NORMAL);
                    super.visitInsn(SWAP);
                    this.visitInsn(ACONST_NULL);
                    super.visitMethodInsn(INVOKESTATIC, "NativeInterface",
                            "methodEnter",
                            "(Ljava/lang/String;"    // name
                                    + "Ljava/lang/String;"   // signature
                                    + "Ljava/lang/String;"   // calleeclass
                                    + "I"                    // calleeValKind
                                    + "Ljava/lang/Object;"   // callee
                                    + "[Ljava/lang/Object;"  // args
                                    +")V" // thread
                            , false);
            }
        }
    }

    private class VarMV extends InstrumentationMV {

        public VarMV(MethodVisitor mv, String classDescr, String name,
                     int access, String sig) {
            super(mv, classDescr, name, access, sig);
        }

        @Override
        public void visitVarInsn(int opcode, int var) {
			/*
			 * if (getMethodName().equals("<init>")) {
			 * warning("skipping (STORE|LOAD)VAR due to being in constructor");
			 * } else
			 */{
                // System.out.println("visiting var "+var+", with opcode "+opcode);
                switch (opcode) {
                    case ILOAD:
                        break;
                    case LLOAD:
                        break;
                    case FLOAD:
                        break;
                    case DLOAD:
                        break;
                    case ALOAD:
                        if (!this.isThisVar(var)) {
                            comment("setting up call to LOADVAR " + var);
                            // System.out.println("uninitTy "+
                            // this.analyzer.uninitializedTypes);
                            // valkind + val,
                            this.pushKindAndObjectInVar(var);
                            // int var,
                            super.visitLdcInsn(var);
                            // String callerclass,
                            pushCallerClassStr();
                            // String callermethod,
                            super.visitLdcInsn(getMethodName());
                            // Object callerValKind+caller,
                            pushThisKindAndObj();
                            super.visitMethodInsn(INVOKESTATIC, "NativeInterface",
                                    "loadVar", "(" + "I" // valkind
                                            + "Ljava/lang/Object;" // Val
                                            + "I" // var
                                            + "Ljava/lang/String;" // callerclass
                                            + "Ljava/lang/String;" // callermethod
                                            + "I" // callerValKind
                                            + "Ljava/lang/Object;" // caller
                                            +")V",// thread
                                    false);
                            comment("done with call to LOADVAR");
                            // */
                        }
                        break;
                    case ISTORE:
                        break;
                    case LSTORE:
                        break;
                    case FSTORE:
                        break;
                    case DSTORE:
                        break;
                    case ASTORE:
                        comment("setting up call to STOREVAR " + var);
                        // Object newVal
                        this.pushKindAndObjectAtDepth(0);
                        //arst pushFakeKindAndVal();
                        // Object oldvalkind, oldval
                    {
                        //FIXME when pushing the old val, we sometimes get "Accessing value from uninitialized register 2"
                        //this.pushKindAndObjectInVar(var);
                        super.visitLdcInsn(Instrument.SPECIAL_VAL_NOT_IMPLEMENTED);
                        super.visitInsn(ACONST_NULL);
                    }
                    // int var,
                    super.visitLdcInsn(var);
                    // String callerclass,
                    pushCallerClassStr();
                    // String callermethod,
                    super.visitLdcInsn(getMethodName());
                    // Object callerValKind+caller,
                    pushThisKindAndObj();
                    super.visitMethodInsn(INVOKESTATIC, "NativeInterface",
                            "storeVar", "(" + "I" + "Ljava/lang/Object;" // newVal
                                    + "I" + "Ljava/lang/Object;" // oldVal
                                    + "I" // var
                                    + "Ljava/lang/String;" // callerclass
                                    + "Ljava/lang/String;" // callermethod
                                    + "I" // callerValKind
                                    + "Ljava/lang/Object;" // caller
                                    + ")V", // thread
                            false);
                    comment("done with call to STOREVAR");
                    // */
                    break;
                    case RET:
                        break;
                    default:
                        // throw new
                        // RuntimeException("opcode "+opcode+" is illegal for visitVarInsn");
                }
            }
            super.visitVarInsn(opcode, var);
        }

        /**
         * Returns whether the `var` is the variable known as `this` in Java
         *
         * @param var
         *            the index of the variable in question
         * @return true if the variable is `this`, false otherwise
         */
        private boolean isThisVar(int var) {
            return (!this.isStatic()) && var == 0;
        }
    }
}