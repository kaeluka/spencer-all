import org.junit.After;
import org.junit.Test;
import util.SpencerRunner;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PrintingHelloIntegration {
    @Test
    public void Hello() {
        final SpencerRunner.RunResult runResult =
                SpencerRunner.runWithArgs(Collections.singletonList("util.Foo"));
        assert runResult != null;
        assertThat(runResult.stdout, containsString("Hello, Foo"));
    }
}
