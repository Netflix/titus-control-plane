/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.common.runtime.internal;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.common.environment.MyEnvironment;
import com.netflix.titus.common.environment.MyEnvironments;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.internal.DefaultLocalScheduler;
import com.netflix.titus.common.runtime.SystemAbortEvent;
import com.netflix.titus.common.runtime.SystemAbortListener;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ReflectionExt;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.code.CodePointTracker;
import com.netflix.titus.common.util.code.LoggingCodeInvariants;
import com.netflix.titus.common.util.code.LoggingCodePointTracker;
import com.netflix.titus.common.util.code.SpectatorCodePointTracker;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.time.Clocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;
import rx.Observable;

@Singleton
public class DefaultTitusRuntime implements TitusRuntime {

    static final Duration LOCAL_SCHEDULER_LOOP_INTERVAL_MS = Duration.ofMillis(100);

    public static final String SYSTEM_EXIT_ON_FAILURE = "systemExitOnFailure";

    private static final Logger logger = LoggerFactory.getLogger(DefaultTitusRuntime.class);

    public static final String FIT_ACTIVATION_PROPERTY = "titus.runtime.fit.enabled";

    private static final String METRICS_RUNTIME_ROOT = "titus.system.";
    private static final String METRICS_PERSISTENT_STREAM = METRICS_RUNTIME_ROOT + "persistentStream";

    private static final String UNKNOWN = "UNKNOWN";

    private static final long INITIAL_RETRY_DELAY_MS = 10;
    private static final long MAX_RETRY_DELAY_MS = 10_000;

    private static final Duration LOCAL_SCHEDULER_LOOP_INTERVAL = Duration.ofMillis(100);

    private final MyEnvironment myEnvironment;
    private final CodePointTracker codePointTracker;
    private final CodeInvariants codeInvariants;
    private final SystemLogService systemLogService;
    private final boolean systemExitOnFailure;
    private final SystemAbortListener systemAbortListener;
    private final Registry registry;
    private final Clock clock;
    private final FitFramework fitFramework;
    private final DefaultLocalScheduler localScheduler;

    @Inject
    public DefaultTitusRuntime(MyEnvironment myEnvironment,
                               CodeInvariants codeInvariants,
                               SystemLogService systemLogService,
                               @Named(SYSTEM_EXIT_ON_FAILURE) boolean systemExitOnFailure,
                               SystemAbortListener systemAbortListener,
                               Registry registry) {
        this(
                myEnvironment,
                new SpectatorCodePointTracker(registry),
                codeInvariants,
                systemLogService,
                systemExitOnFailure,
                systemAbortListener,
                LOCAL_SCHEDULER_LOOP_INTERVAL,
                registry,
                Clocks.system(),
                "true".equals(System.getProperty(FIT_ACTIVATION_PROPERTY, "false"))
        );
    }

    public DefaultTitusRuntime(MyEnvironment myEnvironment,
                               CodePointTracker codePointTracker,
                               CodeInvariants codeInvariants,
                               SystemLogService systemLogService,
                               boolean systemExitOnFailure,
                               SystemAbortListener systemAbortListener,
                               Duration localSchedulerLoopInterval,
                               Registry registry,
                               Clock clock,
                               boolean isFitEnabled) {
        this.myEnvironment = myEnvironment;
        this.codePointTracker = codePointTracker;
        this.codeInvariants = codeInvariants;
        this.systemLogService = systemLogService;
        this.systemExitOnFailure = systemExitOnFailure;
        this.systemAbortListener = systemAbortListener;
        this.registry = registry;
        this.clock = clock;
        this.fitFramework = isFitEnabled ? FitFramework.newFitFramework() : FitFramework.inactiveFitFramework();
        this.localScheduler = new DefaultLocalScheduler(localSchedulerLoopInterval, Schedulers.parallel(), clock, registry);
    }

    @Override
    public MyEnvironment getMyEnvironment() {
        return myEnvironment;
    }

    @Override
    public <T> Observable<T> persistentStream(Observable<T> source) {
        String callerName;
        List<Tag> commonTags;
        Optional<StackTraceElement> callerStackTraceOpt = ReflectionExt.findCallerStackTrace();
        if (callerStackTraceOpt.isPresent()) {
            StackTraceElement callerStackTrace = callerStackTraceOpt.get();
            commonTags = Arrays.asList(
                    new BasicTag("class", callerStackTrace.getClassName()),
                    new BasicTag("method", callerStackTrace.getMethodName()),
                    new BasicTag("line", Integer.toString(callerStackTrace.getLineNumber()))
            );
            callerName = callerStackTrace.getClassName() + '.' + callerStackTrace.getMethodName() + '@' + callerStackTrace.getLineNumber();
        } else {
            callerName = UNKNOWN;
            commonTags = Arrays.asList(
                    new BasicTag("class", UNKNOWN),
                    new BasicTag("method", UNKNOWN),
                    new BasicTag("line", UNKNOWN)
            );
        }

        return source
                .compose(SpectatorExt.subscriptionMetrics(METRICS_PERSISTENT_STREAM, commonTags, registry))
                .retryWhen(RetryHandlerBuilder.retryHandler()
                        .withUnlimitedRetries()
                        .withDelay(INITIAL_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                        .withTitle("Auto-retry for " + callerName)
                        .buildExponentialBackoff()
                );
    }

    @Override
    public Clock getClock() {
        return clock;
    }

    @Override
    public CodePointTracker getCodePointTracker() {
        return codePointTracker;
    }

    @Override
    public CodeInvariants getCodeInvariants() {
        return codeInvariants;
    }

    @Override
    public SystemLogService getSystemLogService() {
        return systemLogService;
    }

    @Override
    public Registry getRegistry() {
        return registry;
    }

    @Override
    public FitFramework getFitFramework() {
        return fitFramework;
    }

    @Override
    public LocalScheduler getLocalScheduler() {
        return localScheduler;
    }

    @Override
    public boolean isSystemExitOnFailure() {
        return systemExitOnFailure;
    }

    @Override
    public void beforeAbort(SystemAbortEvent event) {
        logger.error("System abort requested: {}", event);
        try {
            systemAbortListener.onSystemAbortEvent(event);
        } catch (Exception e) {
            logger.error("Unexpected exception from the system abort listener", e);
        }
    }

    public static Builder newBuilder() {
        return new DefaultBuilder();
    }

    public static final class DefaultBuilder implements Builder {

        private MyEnvironment myEnvironment;
        private CodePointTracker codePointTracker;
        private CodeInvariants codeInvariants;
        private SystemLogService systemLogService;
        private boolean systemExitOnFailure;
        private SystemAbortListener systemAbortListener;
        private Registry registry;
        private Clock clock;
        private boolean fitEnabled;
        private Duration localSchedulerLoopInterval;

        private DefaultBuilder() {
        }

        public Builder withMyEnvironment(MyEnvironment myEnvironment) {
            this.myEnvironment = myEnvironment;
            return this;
        }

        public Builder withCodePointTracker(CodePointTracker codePointTracker) {
            this.codePointTracker = codePointTracker;
            return this;
        }

        public Builder withCodeInvariants(CodeInvariants codeInvariants) {
            this.codeInvariants = codeInvariants;
            return this;
        }

        public Builder withSystemLogService(SystemLogService systemLogService) {
            this.systemLogService = systemLogService;
            return this;
        }

        public Builder withSystemExitOnFailure(boolean systemExitOnFailure) {
            this.systemExitOnFailure = systemExitOnFailure;
            return this;
        }

        public Builder withSystemAbortListener(SystemAbortListener systemAbortListener) {
            this.systemAbortListener = systemAbortListener;
            return this;
        }

        public Builder withRegistry(Registry registry) {
            this.registry = registry;
            return this;
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder withFitFramework(boolean fitEnabled) {
            this.fitEnabled = fitEnabled;
            return this;
        }

        public Builder withLocalSchedulerLoopInterval(Duration localSchedulerLoopInterval) {
            this.localSchedulerLoopInterval = localSchedulerLoopInterval;
            return this;
        }

        public DefaultTitusRuntime build() {
            if (myEnvironment == null) {
                myEnvironment = MyEnvironments.empty();
            }
            if (codePointTracker == null) {
                codePointTracker = new LoggingCodePointTracker();
            }
            if (codeInvariants == null) {
                codeInvariants = LoggingCodeInvariants.getDefault();
            }
            if (systemLogService == null) {
                systemLogService = LoggingSystemLogService.getInstance();
            }
            if (systemAbortListener == null) {
                systemAbortListener = LoggingSystemAbortListener.getDefault();
            }
            if (localSchedulerLoopInterval == null) {
                localSchedulerLoopInterval = LOCAL_SCHEDULER_LOOP_INTERVAL_MS;
            }
            if (registry == null) {
                registry = new DefaultRegistry();
            }
            if (clock == null) {
                clock = Clocks.system();
            }
            return new DefaultTitusRuntime(
                    myEnvironment,
                    codePointTracker,
                    codeInvariants,
                    systemLogService,
                    systemExitOnFailure,
                    systemAbortListener,
                    localSchedulerLoopInterval,
                    registry,
                    clock,
                    fitEnabled
            );
        }
    }
}
