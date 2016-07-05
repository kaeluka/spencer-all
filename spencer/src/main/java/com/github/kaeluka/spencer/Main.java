package com.github.kaeluka.spencer;
import com.github.kaeluka.spencer.server.TransformerServer;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.stream.Collectors;

public class Main {

    private static final Options spencerOptions = initSpencerOptions();

    private static Options initSpencerOptions() {
        final Options options = new Options();
        options.addOption(
                Option
                        .builder("full")
                        .desc("Instrument as many classes in the Java runtime as possible.")
                        .build());
        options.addOption(
                Option
                        .builder("verbose")
                        .desc("Log output to stdout.")
                        .build());

        options.addOption(
                Option
                        .builder("help")
                        .desc("Display this help.")
                        .build());

        return options;
    }

    public static void main(String[] _args) throws IOException, InterruptedException {
        final ArrayList<String> args = tokenizeArgs(_args);
        final CommandLine spencerArgs = parseSpencerArgs(popSpencerArgs(args));

        new Thread(() -> {
            try {
                TransformerServer.main(new String[0]);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }).start();

        //waiting for server to start
        TransformerServer.awaitRunning();

        final Process process = getProcess(args, spencerArgs.hasOption("full"));
        process.waitFor();

        TransformerServer.tearDown();
    }

    private static List<String> popSpencerArgs(List<String> args) {
        if (args.contains("--")) {
            final ArrayList<String> ret = new ArrayList<>();
            int i = 0;

            for (String arg : args) {
                i++;
                if (arg.equals("--")) {
                    break;
                } else {
                    ret.add(arg);
                }
            }

            for ( ; i>0; --i) {
                args.remove(0);
            }

            return ret;
        } else {
            System.out.println("all args ("+args+") go to spencer");
            final ArrayList<String> ret = new ArrayList<>(args);
            args.clear();
            return ret;
        }
    }

    private static CommandLine parseSpencerArgs(List<String> spencerArgs) {
//        System.out.println("spencer args are: "+spencerArgs);
        final DefaultParser defaultParser = new DefaultParser();
        final CommandLine parsed;
        try {
            String[] args = Arrays.copyOf(spencerArgs.toArray(), spencerArgs.size(), String[].class);
            parsed = defaultParser.parse(Main.spencerOptions, args);

            if (parsed.hasOption("help")) {
                printHelp();
                System.exit(0);
            }

            return parsed;

        } catch (ParseException e) {
            System.err.println(e.getMessage());
//            Main.spencerOptions.
            new HelpFormatter().printUsage(new PrintWriter(System.err), 0, "spencer", Main.spencerOptions);
            //printHelp();
            System.err.println("quitting");
//            System.out.println(Main.spencerOptions.toString());
            System.exit(1);
            return null;
        }

    }

    private static void printHelp() {
        final HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(
                "spencer",
                "A JVM with memory instrumentation.",
                spencerOptions,
                "stephan.brandauer@it.uu.se",
                true);
    }

    private static Process getProcess(final ArrayList<String> jvmArgs, final boolean fullInstrumentation) throws IOException {

//        jvmArgs = jvmArgs.stream()
//                .filter(arg -> arg.replaceAll(" ", "").length() > 0)
//                .collect(Collectors.toList());

        final String sep = System.getProperty("file.separator");

        final ArrayList<String> argStrings = new ArrayList<>();

        //executable:
        argStrings.add(System.getProperty("java.home") + sep + "bin" + sep + "java");

        if (fullInstrumentation) {
            // the transformed runtime
            final File file = new File(System.getProperty("user.home") + ("/.spencer/instrumented_java_rt/output"));
            if (! file.isDirectory()) {
                throw new IllegalStateException("can not run full instrumentation: file "+file.getAbsolutePath()+" does not exist!");
            }
            argStrings.add("-Xbootclasspath/p:" + System.getProperty("user.home") + ("/.spencer/instrumented_java_rt/output".replaceAll("/", sep)));
        }

        System.out.println("cp is: "+System.getProperty("java.class.path"));
        java.io.InputStream is = Main.class.getResourceAsStream("/dependencies.properties");

        java.util.Properties p = new Properties();
        p.load(is);
        final String nativeInterfaceLocation = p.getProperty("com.github.kaeluka:spencer-tracing-java:jar");
        System.out.println("com.github.kaeluka:spencer-tracing-java:jar = "+ nativeInterfaceLocation);

        argStrings.add("-Xbootclasspath/p:" + nativeInterfaceLocation);

        //FIXME hard-coded OS
        final String tracingNativePom = p.getProperty("com.github.kaeluka:spencer-tracing-osx:pom");
        if (tracingNativePom == null) {
            throw new IllegalStateException("could not find pom.xml for spencer-tracing-osx project");
        }
        final String tracingJni = tracingNativePom.replace("pom.xml", "target/spencer-tracing-osx.so");
//        final File file = new File(tracingJni);
//        if (!file.exists()) {
//            throw new IllegalStateException("Could not find agent binary: "+file.getAbsolutePath() + " wtf");
//        }
        argStrings.add("-agentpath:" + tracingJni + "=tracefile=/tmp/tracefile");

        for (String arg : jvmArgs) {
            argStrings.addAll(Arrays.asList(arg.split(" ")));
        }
        //argStrings.addAll(Arrays.asList(args));
        ProcessBuilder processBuilder = new ProcessBuilder(
                argStrings);

//        System.out.println("running command: "+processBuilder.command());

        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectInput(ProcessBuilder.Redirect.INHERIT);

        return processBuilder.start();
    }

    private static ArrayList<String> tokenizeArgs(final String[] args) {
//        System.out.println("tokenizing "+Arrays.asList(args));
        ArrayList<String> ret = new ArrayList<>();
        for (String arg : args) {
            final String[] split = arg.split(" ");
//            for (int i=0; i<split.length; ++i) {
//                split[i] = split[i].trim();
//            }
            ret.addAll(Arrays.asList(split));
        }
        return ret;
    }

    private static void printClassPath() {
        final List<URL> urLs = Arrays.asList(((URLClassLoader) Main.class.getClassLoader()).getURLs());
        System.out.println("class path: "+ urLs);
    }
}
