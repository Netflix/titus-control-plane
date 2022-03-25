/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.common.util.loadshedding.backoff;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.loadshedding.AdaptiveAdmissionController;
import com.netflix.titus.common.util.loadshedding.AdmissionBackoffStrategy;
import com.netflix.titus.common.util.time.Clock;

/**
 * A simple strategy that aims at achieving an effective admission rate close to the maximum that can be handled by
 * the request handler.
 */
public class SimpleAdmissionBackoffStrategy implements AdmissionBackoffStrategy {

    private static final String METRIC_ROOT = "titus.admissionController.simpleBackoff.";

    private final SimpleAdmissionBackoffStrategyConfiguration configuration;
    private final Clock clock;
    private final Registry registry;

    private volatile long beginningTimestamp;
    private volatile double throttleFactor;
    private final Lock lock = new ReentrantLock();

    private final AtomicLong successCount = new AtomicLong();
    private final AtomicLong unavailableCount = new AtomicLong();
    private final AtomicLong rateLimitedCount = new AtomicLong();

    private final Id metricIdThrottleFactor;

    public SimpleAdmissionBackoffStrategy(String id,
                                          SimpleAdmissionBackoffStrategyConfiguration configuration,
                                          TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.clock = titusRuntime.getClock();
        this.registry = titusRuntime.getRegistry();

        this.beginningTimestamp = clock.wallTime();
        this.throttleFactor = 1.0;
        this.metricIdThrottleFactor = registry.createId(METRIC_ROOT + "throttleFactor", "id", id);
        PolledMeter.using(registry).withId(metricIdThrottleFactor).monitorValue(this, self -> self.throttleFactor);
    }

    public void close() {
        PolledMeter.remove(registry, metricIdThrottleFactor);
    }

    @Override
    public double getThrottleFactor() {
        long now = clock.wallTime();
        if ((beginningTimestamp + configuration.getMonitoringIntervalMs()) > now) {
            return throttleFactor;
        }
        if (!lock.tryLock()) {
            return throttleFactor;
        }
        try {
            adjustThrottleFactor(now);
        } finally {
            lock.unlock();
        }
        return throttleFactor;
    }

    @Override
    public void onSuccess(long elapsedMs) {
        successCount.getAndIncrement();
    }

    @Override
    public void onError(long elapsedMs, AdaptiveAdmissionController.ErrorKind errorKind, Throwable cause) {
        switch (errorKind) {
            case RateLimited:
            default:
                rateLimitedCount.getAndIncrement();
                break;
            case Unavailable:
                unavailableCount.getAndIncrement();
                break;
        }
    }

    private void adjustThrottleFactor(long now) {
        // Order is important here. First check for unavailable errors. Next rate limited, and only as the last one success.
        if (unavailableCount.get() > 0) {
            this.throttleFactor = configuration.getUnavailableThrottleFactor();
        } else if (rateLimitedCount.get() > 0) {
            this.throttleFactor = Math.max(
                    configuration.getUnavailableThrottleFactor(),
                    throttleFactor - configuration.getRateLimitedAdjustmentFactor()
            );
        } else if (successCount.get() > 0) {
            this.throttleFactor = Math.min(
                    1.0,
                    throttleFactor + configuration.getRateLimitedAdjustmentFactor()
            );
        }
        successCount.set(0);
        rateLimitedCount.set(0);
        unavailableCount.set(0);
        beginningTimestamp = now;
    }
}
