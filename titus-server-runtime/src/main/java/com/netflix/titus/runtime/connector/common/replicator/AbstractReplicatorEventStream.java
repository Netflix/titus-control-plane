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

package com.netflix.titus.runtime.connector.common.replicator;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

public abstract class AbstractReplicatorEventStream<SNAPSHOT, TRIGGER> implements ReplicatorEventStream<SNAPSHOT, TRIGGER> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractReplicatorEventStream.class);

    protected final DataReplicatorMetrics metrics;
    protected final TitusRuntime titusRuntime;
    protected final Scheduler scheduler;

    protected AbstractReplicatorEventStream(DataReplicatorMetrics metrics, TitusRuntime titusRuntime, Scheduler scheduler) {
        this.metrics = metrics;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;
    }

    @Override
    public Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> connect() {

        return newConnection()
                .compose(ReactorExt.reEmitter(
                        // If there are no events in the stream, we will periodically emit the last cache instance
                        // with the updated cache update timestamp, so it does not look stale.
                        cacheEvent -> new ReplicatorEvent<>(cacheEvent.getSnapshot(), cacheEvent.getTrigger(), titusRuntime.getClock().wallTime()),
                        LATENCY_REPORT_INTERVAL,
                        scheduler
                ))
                .doOnNext(event -> {
                    metrics.connected();
                    metrics.event(event);
                })
                .doOnCancel(metrics::disconnected)
                .doOnError(error -> {
                    logger.warn("[{}] Connection to the event stream terminated with an error: {}", getClass().getSimpleName(), error.getMessage(), error);
                    metrics.disconnected(error);
                })
                .doOnComplete(metrics::disconnected);
    }

    protected abstract Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> newConnection();
}
