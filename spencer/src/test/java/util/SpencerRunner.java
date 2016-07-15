package util;

import java.nio.file.Path;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;



public class SpencerRunner {
    public static RunResult runWithArgs(final List<String> _args) {
        List<String> args = new ArrayList<String>();
        args.add("../../spencer");
        Path tempDir;
        try {
            tempDir = getTempDir();
            args.add("-tracedir "+tempDir.toString());
            args.add("--");
            args.addAll(_args);
            ProcessBuilder p = new ProcessBuilder(
                    args);
            p.directory(new File("target/test-classes"));
            final Process process = p.start();

            final InputStreamReader stdoutReader =
                    new InputStreamReader(process.getInputStream());
            final BufferedReader bufferedStdoutReader = new BufferedReader(stdoutReader);
            String line;

            final StringBuilder stdout = new StringBuilder();
            while ((line = bufferedStdoutReader.readLine()) != null) {
                stdout.append("\n");
                stdout.append(line);
            }

            final InputStreamReader stderrReader =
                    new InputStreamReader(process.getErrorStream());
            final BufferedReader bufferedStderrReader = new BufferedReader(stderrReader);

            final StringBuilder stderr = new StringBuilder();
            while ((line = bufferedStderrReader.readLine()) != null) {
                stderr.append("\n");
                stderr.append(line);
            }
            final int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RunFailureException(exitCode,
                        stdout.toString(),
                        stderr.toString());
            } else {
                return new RunResult(
                        stdout.toString(),
                        stderr.toString(),
                        tempDir);
            }

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private static Path getTempDir() throws IOException {
        final Path ret = Files.createTempDirectory("test");
        ret.toFile().deleteOnExit();
        return ret;
    }

    public static class RunResult {
        public final String stdout;
        public final String stderr;
        public final Path tracedir;

        private RunResult(final String stdout,
                          final String stderr,
                          final Path tracedir) {
            assert stdout != null;
            assert stderr != null;
            assert tracedir != null;
            this.stdout = stdout;
            this.stderr = stderr;
            this.tracedir = tracedir;
        }
    }

    public static class RunFailureException extends RuntimeException {

        private final int exitCode;

        public RunFailureException(int exitCode,
                                   final String stdout,
                                   final String stderr) {
            super("program failed with exit code "+exitCode
                    + "\n\n########## stdout:\n"
                    + stdout
                    + "\n\n########## stderr:\n"
                    + stderr);
            assert exitCode != 0;
            this.exitCode = exitCode;
        }
    }
}
