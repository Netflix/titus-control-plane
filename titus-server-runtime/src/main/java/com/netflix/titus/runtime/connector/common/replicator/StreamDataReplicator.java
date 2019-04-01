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

import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * {@link DataReplicator} implementation that wraps {@link ReplicatorEventStream}. The latter is provided
 * as a constructor argument by extensions of this class.
 */
public class StreamDataReplicator<SNAPSHOT extends ReplicatedSnapshot, TRIGGER> implements DataReplicator<SNAPSHOT, TRIGGER> {
    private static final Logger logger = LoggerFactory.getLogger(StreamDataReplicator.class);

    // Staleness threshold checked during the system initialization.
    private static final long STALENESS_THRESHOLD = 60_000;

    private final TitusRuntime titusRuntime;
    private final Disposable internalSubscription;

    private final Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> eventStream;

    private final AtomicReference<ReplicatorEvent<SNAPSHOT, TRIGGER>> lastReplicatorEventRef;

    public StreamDataReplicator(Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> eventStream,
                                Disposable internalSubscription,
                                AtomicReference<ReplicatorEvent<SNAPSHOT, TRIGGER>> lastReplicatorEventRef,
                                TitusRuntime titusRuntime) {
        this.eventStream = eventStream;
        this.internalSubscription = internalSubscription;
        this.lastReplicatorEventRef = lastReplicatorEventRef;
        this.titusRuntime = titusRuntime;
    }

    @Override
    public void close() {
        internalSubscription.dispose();
    }

    @Override
    public SNAPSHOT getCurrent() {
        return lastReplicatorEventRef.get().getSnapshot();
    }

    @Override
    public long getStalenessMs() {
        return titusRuntime.getClock().wallTime() - lastReplicatorEventRef.get().getLastUpdateTime();
    }

    @Override
    public Flux<Long> observeDataStalenessMs() {
        return eventStream.map(ReplicatorEvent::getLastUpdateTime);
    }

    @Override
    public Flux<Pair<SNAPSHOT, TRIGGER>> events() {
        return eventStream.map(event -> Pair.of(event.getSnapshot(), event.getTrigger()));
    }

    public static <SNAPSHOT extends ReplicatedSnapshot, TRIGGER> StreamDataReplicator<SNAPSHOT, TRIGGER>
    newStreamDataReplicator(ReplicatorEvent<SNAPSHOT, TRIGGER> initialEvent,
                            ReplicatorEventStream<SNAPSHOT, TRIGGER> replicatorEventStream,
                            DataReplicatorMetrics metrics,
                            TitusRuntime titusRuntime) {
        AtomicReference<ReplicatorEvent<SNAPSHOT, TRIGGER>> lastReplicatorEventRef = new AtomicReference<>(initialEvent);
        Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> eventStream = replicatorEventStream.connect().publish().autoConnect(1);
        Disposable internalSubscription = newMonitoringSubscription(metrics, lastReplicatorEventRef, eventStream);

        return new StreamDataReplicator<>(eventStream, internalSubscription, lastReplicatorEventRef, titusRuntime);
    }

    public static <SNAPSHOT extends ReplicatedSnapshot, TRIGGER> Flux<StreamDataReplicator<SNAPSHOT, TRIGGER>>
    newStreamDataReplicator(ReplicatorEventStream<SNAPSHOT, TRIGGER> replicatorEventStream,
                            DataReplicatorMetrics metrics,
                            TitusRuntime titusRuntime) {
        return Flux.defer(() -> {
            AtomicReference<ReplicatorEvent<SNAPSHOT, TRIGGER>> lastReplicatorEventRef = new AtomicReference<>();
            Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> eventStream = replicatorEventStream.connect().publish().autoConnect(2);
            Disposable internalSubscription = newMonitoringSubscription(metrics, lastReplicatorEventRef, eventStream);

            return eventStream.filter(e -> isFresh(e, titusRuntime)).take(1).map(e ->
                    new StreamDataReplicator<>(eventStream, internalSubscription, lastReplicatorEventRef, titusRuntime)
            );
        });
    }

    private static <SNAPSHOT extends ReplicatedSnapshot, TRIGGER> Disposable
    newMonitoringSubscription(DataReplicatorMetrics metrics,
                              AtomicReference<ReplicatorEvent<SNAPSHOT, TRIGGER>> lastReplicatorEventRef,
                              Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> eventStream) {
        return eventStream
                .doOnSubscribe(s -> metrics.connected())
                .doOnCancel(metrics::disconnected)
                .subscribe(
                        next -> {
                            logger.debug("Snapshot update: {}", next.getSnapshot().toSummaryString());
                            lastReplicatorEventRef.set(next);
                            metrics.event(next);
                        },
                        e -> {
                            logger.error("Unexpected error in the replicator event stream", e);
                            metrics.disconnected(e);
                        },
                        () -> {
                            logger.info("Replicator event stream completed");
                            metrics.disconnected();
                        }
                );
    }

    private static boolean isFresh(ReplicatorEvent event, TitusRuntime titusRuntime) {
        long now = titusRuntime.getClock().wallTime();
        return event.getLastUpdateTime() + STALENESS_THRESHOLD >= now;
    }
}
