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

package com.netflix.titus.common.util.rx;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.titus.common.util.ExecutorsExt;
import rx.Scheduler;
import rx.schedulers.Schedulers;

public class SchedulerExt {

    /**
     * Create a {@link Scheduler} from a single (non-daemon) threaded {@link ScheduledExecutorService} with a name pattern.
     *
     * @param name the thread name pattern
     * @return the scheduler
     */
    public static Scheduler createSingleThreadScheduler(String name) {
        return Schedulers.from(ExecutorsExt.namedSingleThreadExecutor(name));
    }

}
