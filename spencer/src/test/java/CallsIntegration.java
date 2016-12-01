import com.github.kaeluka.spencer.Events;
import com.github.kaeluka.spencer.Events.AnyEvt.Which;
import com.github.kaeluka.spencer.analysis.CountEvents;
import com.github.kaeluka.spencer.analysis.Util;
import com.github.kaeluka.spencer.tracefiles.TraceFileIterator;
import org.junit.Before;
import org.junit.Test;
import util.SpencerRunner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class CallsIntegration {

    private SpencerRunner.RunResult runResult;

    @Before
    public void prepare() {
        this.runResult = SpencerRunner.runWithArgs(
                Collections.singletonList("util.MethodCalls"));
        assert this.runResult != null;
    }

    /**
     * check that method enters and exits form a nested call structure by
     * making sure that the method exit's name always matches the top frame
     * on the call stack
     *
    @Test
    public void callStructureMatches() throws FileNotFoundException {

        final TraceFileIterator it = getTraceFileIterator(runResult.getTracefile());
        Util.assertProperCallStructure(it);
        }*/

    @Test
    public void callsInRightOrder() throws FileNotFoundException {
        final TraceFileIterator it = getTraceFileIterator(this.runResult.getTracefile());
        assertThat("MethodCalls::main class call must be called",
                seekCall(it, "MethodCalls", "main"), is(true));
        assertThat("System.out.println must be called",
                seekCall(it, "PrintStream", "println"), is(true));
        assertThat("MethodCalls::Foo class call must be called",
                seekCall(it, "MethodCalls", "Foo"), is(true));
        assertThat("MethodCalls::Bar class call must be called",
                seekCall(it, "MethodCalls", "Bar"), is(true));
    }

    private boolean seekCall(final TraceFileIterator it, final String cname, final String mname) {
        boolean found = false;
        while (it.hasNext()) {
            final Events.AnyEvt.Reader next = it.next();
            if (next.isMethodenter()) {
                final Events.MethodEnterEvt.Reader methodenter = next.getMethodenter();
                if (methodenter.getCalleeclass().toString().endsWith(cname) &&
                        methodenter.getName().toString().equals(mname)) {
                    found = true;
                    break;
                }
            }
        }
        return found;
    }

    private static TraceFileIterator getTraceFileIterator(
            Path tracefile) throws FileNotFoundException {
        return new TraceFileIterator(new FileInputStream(tracefile.toFile()));
    }
}
