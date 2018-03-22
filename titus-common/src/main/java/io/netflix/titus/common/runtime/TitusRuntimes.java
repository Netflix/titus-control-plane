package io.netflix.titus.common.runtime;

import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import io.netflix.titus.common.util.code.CodeInvariants;
import io.netflix.titus.common.util.code.LoggingCodePointTracker;
import io.netflix.titus.common.util.time.Clocks;
import rx.schedulers.TestScheduler;

public final class TitusRuntimes {

    private TitusRuntimes() {
    }

    public static TitusRuntime internal() {
        return new DefaultTitusRuntime(
                new LoggingCodePointTracker(),
                CodeInvariants.codeInvariants(),
                new DefaultRegistry(),
                Clocks.system(),
                false
        );
    }

    public static TitusRuntime test() {
        return new DefaultTitusRuntime(
                new LoggingCodePointTracker(),
                CodeInvariants.codeInvariants(),
                new DefaultRegistry(),
                Clocks.test(),
                false
        );
    }

    public static TitusRuntime test(TestScheduler testScheduler) {
        return new DefaultTitusRuntime(
                new LoggingCodePointTracker(),
                CodeInvariants.codeInvariants(),
                new DefaultRegistry(),
                Clocks.testScheduler(testScheduler),
                false
        );
    }
}
