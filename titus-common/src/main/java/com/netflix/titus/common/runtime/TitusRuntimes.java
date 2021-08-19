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

import com.netflix.titus.common.environment.MyEnvironment;
import com.netflix.titus.common.environment.MyEnvironments;
import com.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import org.springframework.core.env.Environment;
import rx.schedulers.TestScheduler;

public final class TitusRuntimes {

    private TitusRuntimes() {
    }

    public static TitusRuntime internal(MyEnvironment environment) {
        return DefaultTitusRuntime.newBuilder()
                .withMyEnvironment(environment)
                .build();
    }

    public static TitusRuntime internal(Environment environment) {
        return DefaultTitusRuntime.newBuilder()
                .withMyEnvironment(MyEnvironments.newSpring(environment))
                .build();
    }

    public static TitusRuntime internal(Duration localSchedulerLoopInterval) {
        return DefaultTitusRuntime.newBuilder()
                .withLocalSchedulerLoopInterval(localSchedulerLoopInterval)
                .build();
    }

    public static TitusRuntime internal() {
        return DefaultTitusRuntime.newBuilder().build();
    }

    public static TitusRuntime internal(boolean fitEnabled) {
        return DefaultTitusRuntime.newBuilder().withFitFramework(true).build();
    }

    public static TitusRuntime test() {
        return DefaultTitusRuntime.newBuilder()
                .withClock(Clocks.test())
                .withFitFramework(true)
                .build();
    }

    public static TitusRuntime test(TestClock clock) {
        return DefaultTitusRuntime.newBuilder()
                .withClock(clock)
                .withFitFramework(true)
                .build();
    }

    public static TitusRuntime test(TestScheduler testScheduler) {
        return DefaultTitusRuntime.newBuilder()
                .withClock(Clocks.testScheduler(testScheduler))
                .withFitFramework(true)
                .build();
    }
}
