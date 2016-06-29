package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class SpencerRunner {
    public static String runWithArgs(final List<String> _args) {
        List<String> args = new ArrayList<String>();
        args.add("../../spencer");
        args.addAll(_args);
        ProcessBuilder p = new ProcessBuilder(
                args);
        p.directory(new File("target/test-classes"));
//        p.redirectOutput();
        try {
            final Process process = p.start();
//            process.waitFor();

            final InputStreamReader inputStreamReader =
                    new InputStreamReader(process.getInputStream());
            final BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line;
            final StringBuilder spencerOut = new StringBuilder();
            while ((line = bufferedReader.readLine()) != null) {
                spencerOut.append("\n"+line);
            }
            return spencerOut.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
