import org.junit.Test;
import util.SpencerRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class CallsIntegration {
    @Test
    public void calls() {
        SpencerRunner.runWithArgs(Collections.singletonList("util.MethodCalls"));
    }
}
