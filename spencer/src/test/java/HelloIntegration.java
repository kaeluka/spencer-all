import org.junit.After;
import org.junit.Test;
import util.SpencerRunner;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HelloIntegration {
    @Test
    public void Hello() {
        final List<String> args =
                Arrays.asList("--", "util.Foo");
        final String output = SpencerRunner.runWithArgs(args);
        assertThat(output, containsString("Hello, Foo"));
    }
}
