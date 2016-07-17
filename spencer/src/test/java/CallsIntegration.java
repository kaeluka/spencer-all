import com.github.kaeluka.spencer.Events;
import com.github.kaeluka.spencer.Events.AnyEvt.Which;
import com.github.kaeluka.spencer.analysis.CountEvents;
import com.github.kaeluka.spencer.tracefiles.EventsUtil;
import com.github.kaeluka.spencer.tracefiles.TraceFileIterator;
import org.junit.Before;
import org.junit.Test;
import util.SpencerRunner;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Stack;

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

    @Test
    public void callNumbersMatch() {
        TraceFileIterator it = getTraceFileIterator(this.runResult.getTracefile());
        final HashMap<String, HashMap<Events.AnyEvt.Which, Object>>
                eventsPerThread = CountEvents.analyse(it);

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
        for (String thdName : eventsPerThread.keySet()) {
            if (!thdName.startsWith("<")) {
                final HashMap<Which, Object> events = eventsPerThread.get(thdName);
                assertThat("must have some method enters for thread '" + thdName + "'",
                        (Integer) events.get(Which.METHODENTER),
                        greaterThan(0));
                assertThat("Method enters and method exits must match in numbers " +
                                "for thread '" + thdName + "'",
                        (Integer) events.get(Events.AnyEvt.Which.METHODENTER),
                        lessThanOrEqualTo((Integer) events.get(Events.AnyEvt.Which.METHODEXIT)));
            }
        }
    }


    /**
     * check that method enters and exits form a nested call structure by
     * making sure that the method exit's name always matches the top frame
     * on the call stack
     */
    @Test
    public void callStructureMatches() {
        class IndexedEnter {
            private final long idx;
            private final Events.MethodEnterEvt.Reader enter;

            private IndexedEnter(final long idx, final Events.MethodEnterEvt.Reader enter) {
                this.idx = idx;
                this.enter = enter;
            }
        };

        final HashMap<String, Stack<IndexedEnter>> callStacks = new HashMap<>();
        final TraceFileIterator it = getTraceFileIterator(runResult.getTracefile());
        long cnt = 0;

        while (it.hasNext()) {
            final Events.AnyEvt.Reader evt = it.next();
            cnt++;

            if (evt.isMethodenter()) {
                final Events.MethodEnterEvt.Reader methodenter = evt.getMethodenter();
                final String thdName = methodenter.getThreadName().toString();
                if (!callStacks.containsKey(thdName)) {
                    callStacks.put(thdName,
                            new Stack<>());
                }
                callStacks.get(thdName).push(new IndexedEnter(cnt, methodenter));

            } else if (evt.isMethodexit()) {
                final Events.MethodExitEvt.Reader methodexit = evt.getMethodexit();
                final String thdName = methodexit.getThreadName().toString();
                assertThat(callStacks, hasKey(thdName));
                final IndexedEnter poppedFrame = callStacks.get(thdName).pop();

                assertThat("popped frame's (#" + cnt
                                + ") name must match last entered frame (#"
                                + poppedFrame.idx + ") in thread '"
                                + thdName + "'\n"
                                + "last entered frame: "
                                + EventsUtil.methodEnterToString(poppedFrame.enter),
                        poppedFrame.enter.getName().toString(),
                        is(methodexit.getName().toString()));
            }
        }
    }

    @Test
    public void callsInRightOrder() {
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
            Path tracefile) {
        return new TraceFileIterator(tracefile.toFile());
    }
}
