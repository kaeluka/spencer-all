import com.github.kaeluka.spencer.Events;
import com.github.kaeluka.spencer.Events.AnyEvt.Which;
import com.github.kaeluka.spencer.analysis.CountEvents;
import com.github.kaeluka.spencer.tracefiles.EventsUtil;
import com.github.kaeluka.spencer.tracefiles.TraceFileIterator;
import org.junit.Before;
import org.junit.Test;
import util.SpencerRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Stack;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;

public class CallsIntegration {

    private SpencerRunner.RunResult runResult;

    @Before
    public void prepare() {
        this.runResult = SpencerRunner.runWithArgs(
                Collections.singletonList("util.MethodCalls"));
        assert this.runResult != null;
    }

    @Test
    public void callNumbersMatch() {
        TraceFileIterator it = getTraceFileIterator();
        final HashMap<Events.AnyEvt.Which, Object> events = CountEvents.analyse(it);

//        assertThat("must have some var loads",
//                (Integer)events.get(Which.VARLOAD),
//                greaterThan(0));
//        assertThat("must have some var stores",
//                (Integer)events.get(Which.VARSTORE),
//                greaterThan(0));
//        assertThat("must have some field loads",
//                (Integer)events.get(Which.FIELDLOAD),
//                greaterThan(0));
//        assertThat("must have some field stores",
//                (Integer)events.get(Which.FIELDSTORE),
//                greaterThan(0));
        assertThat("must have some method enters",
                (Integer)events.get(Which.METHODENTER),
                greaterThan(0));
        assertThat("Method enters and method exits must match in numbers",
                events.get(Events.AnyEvt.Which.METHODENTER),
                is(events.get(Events.AnyEvt.Which.METHODEXIT)));
    }


    /**
     * check that method enters and exits form a nested call structure by
     * making sure that the method exit's name always matches the top frame
     * on the call stack
     */
    @Test
    public void callStructureMatches() {
        final HashMap<String, Stack<Events.MethodEnterEvt.Reader>> callStacks = new HashMap<>();
        final TraceFileIterator it = getTraceFileIterator();
        while (it.hasNext()) {
            final Events.AnyEvt.Reader evt = it.next();

            if (evt.isMethodenter()) {
                final Events.MethodEnterEvt.Reader methodenter = evt.getMethodenter();
                final String thdName = methodenter.getThreadName().toString();
                if (! callStacks.containsKey(thdName)) {
                    callStacks.put(thdName,
                            new Stack<>());
                }

                callStacks.get(thdName).push(methodenter);
            } else if (evt.isMethodexit()) {
                final Events.MethodExitEvt.Reader methodexit = evt.getMethodexit();
                final String thdName = methodexit.getThreadName().toString();

                assertThat(callStacks, hasKey(thdName));
                final Events.MethodEnterEvt.Reader poppedFrame =
                        callStacks.get(thdName).pop();

                assertThat("popped frame's name must match last entered frame",
                        poppedFrame.getName().toString(),
                        is(methodexit.getName().toString()));
            }
        }
    }

    @Test
    public void callsInRightOrder() {
        final TraceFileIterator it = getTraceFileIterator();
        boolean found = false;
        while (it.hasNext()) {
            final Events.AnyEvt.Reader next = it.next();
            if (next.isMethodenter() &&
                    next.getMethodenter().getCalleeclass().toString().endsWith("MethodCalls")) {
                found = true;
                break;
            }
        }

        assertThat("the MethodCalls class call must be instrumented",
                found, is(true));
    }

    private TraceFileIterator getTraceFileIterator() {
        return new TraceFileIterator(this.runResult.getTracefile().toFile());
    }
}
