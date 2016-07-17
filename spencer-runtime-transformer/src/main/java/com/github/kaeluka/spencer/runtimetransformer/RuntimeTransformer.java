package com.github.kaeluka.spencer.runtimetransformer;

import com.github.kaeluka.spencer.instrumentation.Instrument;
import com.github.kaeluka.spencer.instrumentation.Util;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class RuntimeTransformer {

    public static void deleteTransformedRuntime() {
        try {
            FileUtils.deleteDirectory(new File(getTargetDir()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        if (args.length >= 1) {
            throw new IllegalArgumentException("usage: java "
                    +RuntimeTransformer.class.getName()+"\n"+
                "Args received: "+Arrays.asList(args));
        }
        final String targetDir = getTargetDir();
        System.out.println("checking for transformed runtime in "+targetDir);
        if (new File(targetDir).exists()) {
            System.out.println("Already have instrumented runtime in "+ targetDir +". Not instrumenting!");
        } else {
            transformJavaRuntime();
        }
    }

    public static void transformJavaRuntime() throws MalformedURLException {
        final File targetDir = new File(getTargetDir());

        final String objectClassFile =
                RuntimeTransformer
                        .class
                        .getClassLoader()
                        .getResource("java/lang/Object.class").toString();
        final String rtJarFile = objectClassFile.toString().split("!")[0].replace("jar:file:", "");

        System.out.println("transforming classes in "+rtJarFile);
        System.out.println("writing transformed files to "+targetDir.getAbsolutePath());
        System.out.println("this could take several minutes...");

        targetDir.mkdirs();

        final long startTime = System.nanoTime();
        final URL url = new URL("file:"+rtJarFile);
        transformAndDumpClassesToDir(url, targetDir.getAbsolutePath());
        final long diff = ((System.nanoTime()-startTime)/1000000000);
        System.out.println("transformation of "+url.getFile()+" took "+diff+"sec.");
        for (String err : Instrument.getErrors()) {
            System.err.println("Could not instrument: "+err);
        }

        final String name = "java/lang/AbstractStringBuilder";
        System.out.println(name+" is instrumented: "+!Util.isClassNameBlacklisted(name));
    }

    private static String getTargetDir() {
//        final URL resource = RuntimeTransformer.class.getClassLoader().getResource(".");
        final String sep = System.getProperty("file.separator");

        final String ret = System.getProperty("user.home")+sep+".spencer/instrumented_java_rt";
//        assert resource != null;
//        System.out.println(resource);
//        return resource.getPath()+"/instrumented_java_rt";
        return ret;
    }

    private static void transformAndDumpClassesToDir(final URL url, String dir) {
        try {
            if (url.getFile().endsWith(".jar")) {
                JarFile jarFile = new JarFile(url.getPath());
                final ArrayList<JarEntry> entries = Collections.list(jarFile.entries());
                int cnt = 0, skipped = 0, instrumented = 0;

                entries.stream().parallel()
                        .filter(e -> !Util.isClassNameBlacklisted(e.getName()))
                        .forEach(e -> {
                            try {
                                final File originalFile = new File(dir+"/input/"+e.getName());
                                if (e.isDirectory()) {
                                    originalFile.mkdirs();
                                    new File(dir+"/output/"+e.getName()).mkdirs();
                                } else {
                                    originalFile.getParentFile().mkdirs();
                                    originalFile.createNewFile();

                                    FileOutputStream originalDestStream = new FileOutputStream(originalFile);
                                    final InputStream sourceStream = jarFile.getInputStream(e);
                                    final byte[] originalByteCode = IOUtils.toByteArray(sourceStream);
                                    originalDestStream.write(originalByteCode);

                                    if (e.getName().endsWith(".class")) {
                                        final byte[] transformedByteCode = Instrument.transform(originalByteCode, System.out);
                                        if (Arrays.equals(originalByteCode, transformedByteCode)) {
                                            if (!Instrument.isInterface(originalByteCode)) {
                                                throw new IllegalStateException("bytecode for "+e.getName()+" not changed");
                                            }
                                        } else {
                                            final File transformedFile = new File(dir+"/output/"+e.getName());
                                            transformedFile.getParentFile().mkdirs();
                                            assert transformedFile.createNewFile();
                                            FileOutputStream transformedDestStream = new FileOutputStream(transformedFile);
                                            transformedDestStream.write(transformedByteCode);
                                            transformedDestStream.close();
//										instrumented++;
                                        }
                                    }
                                    originalDestStream.close();
                                }
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }
                        });
                jarFile.close();
//				System.out.println("When instrumenting java core runtime classes:\n"
//						+ "skipped "+skipped+" classes, instrumented "+instrumented+" classes ("+(instrumented*100/(skipped+instrumented))+"%)");
                System.out.println("When instrumenting java core runtime classes:\n"
                        + "skipped "+skipped);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
