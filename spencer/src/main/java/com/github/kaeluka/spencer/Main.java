package com.github.kaeluka.spencer;
import com.github.kaeluka.spencer.server.TransformerServer;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.net.URLClassLoader;
import java.util.stream.Collectors;

public class Main {

    private static final Options spencerOptions = initSpencerOptions();

    private static Options initSpencerOptions() {
        final Options options = new Options();

        final OptionGroup ioOptions = new OptionGroup();

        ioOptions.addOption(Option
                .builder("tracedir")
                .hasArg()
                .argName("path/to/dir")
                .optionalArg(false)
                .desc("The directory to write to (default: /tmp).")
                .build());

        ioOptions.addOption(Option
                .builder("notrace")
                .desc("For debugging, disable tracing to disk.")
                .build());

        options.addOptionGroup(ioOptions);

        options.addOption(Option
                .builder("verbose")
                .desc("Show diagnostic output.")
                .build());
        options.addOption(Option
                .builder("help")
                .desc("Display this help.")
                .build());

        return options;
    }

    public static void main(String[] _args) throws IOException, InterruptedException {
        final ArrayList<String> args = tokenizeArgs(_args);
        final CommandLine spencerArgs = parseSpencerArgs(popSpencerArgs(args));
        assert spencerArgs != null;

        logIfVerbose("spencerArgs: ", spencerArgs);
        final Iterator<Option> argIter = spencerArgs.iterator();
        while (argIter.hasNext()) {
            logIfVerbose(argIter.next().toString(), spencerArgs);
        }

        logIfVerbose("tracedir:"+spencerArgs.getOptionValue("tracedir"), spencerArgs);

        new Thread(() -> {
            try {
                TransformerServer.main(new String[0]);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }).start();

        //waiting for server to start
        TransformerServer.awaitRunning();

        final Process process = getProcess(args, spencerArgs);
        final int exitStatus = process.waitFor();

        TransformerServer.tearDown();

        System.exit(exitStatus);
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

            final String tracedir = parsed.getOptionValue("tracedir");
            if (tracedir != null) {
                final File dir = new File(tracedir);
                if ((! dir.exists()) || (! dir.isDirectory())) {
                    System.err.println(
                            "ERROR: trace directory "
                                    +tracedir
                                    +" does not exist or is no directory");
                    System.exit(1);
                }
            }

            return parsed;

        } catch (ParseException e) {
            System.err.println("ERROR: "+e.getMessage());
            printHelp();
            System.err.println("quitting");
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

    private static void logIfVerbose(String txt, CommandLine spencerArgs) {
        if (spencerArgs.hasOption("verbose")) {
            System.out.println(txt);
        }
    }

    private static Process getProcess(final ArrayList<String> jvmArgs, CommandLine spencerArgs) throws IOException {

        final String sep = System.getProperty("file.separator");

        final ArrayList<String> argStrings = new ArrayList<>();

        //executable:
        argStrings.add(System.getProperty("java.home") + sep + "bin" + sep + "java");

        logIfVerbose("cp is: "+System.getProperty("java.class.path"), spencerArgs);
        final URL resource = getAgentLib();
        assert resource != null;

        final File tempAgentFile = Files.createTempFile("spencer-tracing-agent", ".jnilib").toFile();
        assert tempAgentFile.createNewFile();
        //tempAgentFile.deleteOnExit();

        FileUtils.copyURLToFile(resource, tempAgentFile);

        logIfVerbose("agent binary: "+tempAgentFile, spencerArgs);

        if (!tempAgentFile.exists()) {
            throw new IllegalStateException("could not find agent binary: "+tempAgentFile);
        }

        final String tracedirOpt = spencerArgs.getOptionValue("tracedir");
        if (spencerArgs.hasOption("notrace")) {
            argStrings.add("-agentpath:" + tempAgentFile.getAbsolutePath() + "=tracefile=none");
        } else {
            final String tracedir = (tracedirOpt == null) ?
                    "/tmp" :
                    tracedirOpt;
            argStrings.add("-agentpath:"
                    + tempAgentFile.getAbsolutePath()
                    + "=tracefile="+tracedir+"/tracefile");
        }

        argStrings.add("-Xverify:none");

        for (String arg : jvmArgs) {
            argStrings.addAll(Arrays.asList(arg.split(" ")));
        }
        //argStrings.addAll(Arrays.asList(args));
        ProcessBuilder processBuilder = new ProcessBuilder(
                argStrings);

        logIfVerbose("running command: "+processBuilder.command(), spencerArgs);

        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectInput(ProcessBuilder.Redirect.INHERIT);

        return processBuilder.start();
    }

    private static URL getAgentLib() throws IOException {
        java.io.InputStream is = Main.class.getResourceAsStream("/dependencies.properties");

        java.util.Properties p = new Properties();
        p.load(is);

        String aol = p.getProperty("nar.aol");
	assert aol != null;
	aol = aol.replace(".", "-");
        final String version="0.1.3-SNAPSHOT";

        final String lib = "/nar/spencer-tracing-" + version + "-" + aol + "-jni" + "/lib/" + aol + "/jni/libspencer-tracing-" + version + ".jnilib";
        URL resource = Main.class.getResource(lib);
        if (resource == null) {
            resource = Main.class.getResource(lib.replace(".jnilib", ".so"));
        }
        if (resource == null) {
            throw new AssertionError("could not find agent lib: "+lib);
        }
        return resource;
    }

    private static ArrayList<String> tokenizeArgs(final String[] args) {
        ArrayList<String> ret = new ArrayList<>();
        for (String arg : args) {
            final String[] split = arg.split(" ");
            ret.addAll(Arrays.asList(split));
        }
        return ret;
    }

//    private static void printClassPath(CommandLine spencerArgs) {
//        final List<URL> urLs = Arrays.asList(((URLClassLoader) Main.class.getClassLoader()).getURLs());
//        logIfVerbose("class path: "+ urLs, spencerArgs);
//    }
}
