/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.ext.jooq.relocation;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Stopwatch;
import com.netflix.spectator.api.Counter;
import com.netflix.titus.api.common.LeaderActivationListener;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task relocation garbage collector. Removes entries from the relocation status table older than a configurable
 * amount of time.
 */
@Singleton
public class JooqTaskRelocationGC implements LeaderActivationListener {

    public static String METRIC_ROOT = "titus.relocation.gc.";

    private static final Logger logger = LoggerFactory.getLogger(JooqTaskRelocationGC.class);

    private final JooqRelocationConfiguration configuration;
    private final JooqTaskRelocationResultStore resultStore;
    private final TitusRuntime titusRuntime;

    private ScheduleReference scheduleRef;

    private final Counter gcCounter;

    @Inject
    public JooqTaskRelocationGC(JooqRelocationConfiguration configuration,
                                JooqTaskRelocationResultStore resultStore,
                                TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.resultStore = resultStore;
        this.titusRuntime = titusRuntime;
        this.gcCounter = titusRuntime.getRegistry().counter(METRIC_ROOT + "removedCount");
    }

    @Override
    public void activate() {
        this.scheduleRef = titusRuntime.getLocalScheduler().schedule(ScheduleDescriptor.newBuilder()
                        .withName(JooqTaskRelocationGC.class.getSimpleName())
                        .withDescription("Titus relocation data GC")
                        .withInitialDelay(Duration.ZERO)
                        .withInterval(configuration.getGcInterval())
                        .withTimeout(Duration.ofMinutes(30))
                        .build(),
                context -> doGC(),
                true
        );
    }

    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(scheduleRef, ScheduleReference::cancel);
    }

    private void doGC() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        logger.info("Starting new relocation status table GC cycle...");
        int count = resultStore.removeExpiredData(titusRuntime.getClock().wallTime() - configuration.getRetentionTime().toMillis());
        gcCounter.increment(count);
        logger.info("The relocation status table GC cycle finished: removedEntries={}, elapsedMs={}", count, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
}
