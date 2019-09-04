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

package com.netflix.titus.common.runtime;

import java.time.Duration;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import com.netflix.titus.common.runtime.internal.LoggingSystemAbortListener;
import com.netflix.titus.common.runtime.internal.LoggingSystemLogService;
import com.netflix.titus.common.util.code.LoggingCodeInvariants;
import com.netflix.titus.common.util.code.LoggingCodePointTracker;
import com.netflix.titus.common.util.code.RecordingCodeInvariants;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import rx.schedulers.TestScheduler;

public final class TitusRuntimes {

    private static final Duration LOCAL_SCHEDULER_LOOP_INTERVAL_MS = Duration.ofMillis(100);

    private TitusRuntimes() {
    }

    public static TitusRuntime internal(Duration localSchedulerLoopInterval) {
        return new DefaultTitusRuntime(
                new LoggingCodePointTracker(),
                LoggingCodeInvariants.getDefault(),
                LoggingSystemLogService.getInstance(),
                LoggingSystemAbortListener.getDefault(),
                localSchedulerLoopInterval,
                new DefaultRegistry(),
                Clocks.system(),
                false
        );
    }

    public static TitusRuntime internal() {
        return internal(LOCAL_SCHEDULER_LOOP_INTERVAL_MS);
    }

    public static TitusRuntime internal(boolean fitEnabled) {
        return new DefaultTitusRuntime(
                new LoggingCodePointTracker(),
                LoggingCodeInvariants.getDefault(),
                LoggingSystemLogService.getInstance(),
                LoggingSystemAbortListener.getDefault(),
                LOCAL_SCHEDULER_LOOP_INTERVAL_MS,
                new DefaultRegistry(),
                Clocks.system(),
                fitEnabled
        );
    }

    public static TitusRuntime test() {
        return new DefaultTitusRuntime(
                new LoggingCodePointTracker(),
                new RecordingCodeInvariants(),
                LoggingSystemLogService.getInstance(),
                LoggingSystemAbortListener.getDefault(),
                LOCAL_SCHEDULER_LOOP_INTERVAL_MS,
                new DefaultRegistry(),
                Clocks.test(),
                true
        );
    }

    public static TitusRuntime test(TestClock clock) {
        return new DefaultTitusRuntime(
                new LoggingCodePointTracker(),
                new RecordingCodeInvariants(),
                LoggingSystemLogService.getInstance(),
                LoggingSystemAbortListener.getDefault(),
                LOCAL_SCHEDULER_LOOP_INTERVAL_MS,
                new DefaultRegistry(),
                clock,
                true
        );
    }

    public static TitusRuntime test(TestScheduler testScheduler) {
        return new DefaultTitusRuntime(
                new LoggingCodePointTracker(),
                new RecordingCodeInvariants(),
                LoggingSystemLogService.getInstance(),
                LoggingSystemAbortListener.getDefault(),
                LOCAL_SCHEDULER_LOOP_INTERVAL_MS,
                new DefaultRegistry(),
                Clocks.testScheduler(testScheduler),
                true
        );
    }
}
