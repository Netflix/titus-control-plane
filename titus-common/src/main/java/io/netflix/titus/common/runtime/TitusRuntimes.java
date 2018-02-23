package io.netflix.titus.common.runtime;

import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.common.framework.fit.Fit;
import io.netflix.titus.common.framework.fit.FitComponent;
import io.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import io.netflix.titus.common.util.code.CodePointTracker;
import io.netflix.titus.common.util.code.LoggingCodePointTracker;

public final class TitusRuntimes {

    private TitusRuntimes() {
    }

    public static TitusRuntime internal() {
        return new DefaultTitusRuntime(new DefaultRegistry()) {
            private final LoggingCodePointTracker loggingCodePointTracker = new LoggingCodePointTracker();
            private final FitComponent rootFitComponent = Fit.newFitComponent("application");

            @Override
            public CodePointTracker getCodePointTracker() {
                return loggingCodePointTracker;
            }

            @Override
            public FitComponent getFit() {
                return rootFitComponent;
            }
        };
    }
}
