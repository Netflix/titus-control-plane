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
import javax.annotation.PreDestroy;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEventStream.ReplicatorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * {@link DataReplicator} implementation that wraps {@link ReplicatorEventStream}. The latter is provided
 * as a constructor argument by extensions of this class.
 */
public class StreamDataReplicator<D> implements DataReplicator<D> {
    private static final Logger logger = LoggerFactory.getLogger(StreamDataReplicator.class);

    // Staleness threshold checked during the system initialization.
    private static final long STALENESS_THRESHOLD = 60_000;

    private final TitusRuntime titusRuntime;
    private final Disposable internalSubscription;

    private final Flux<ReplicatorEvent<D>> eventStream;

    private final AtomicReference<ReplicatorEvent<D>> lastReplicatorEventRef;

    public StreamDataReplicator(Flux<ReplicatorEvent<D>> eventStream, Disposable internalSubscription, AtomicReference<ReplicatorEvent<D>> lastReplicatorEventRef, TitusRuntime titusRuntime) {
        this.eventStream = eventStream;
        this.internalSubscription = internalSubscription;
        this.lastReplicatorEventRef = lastReplicatorEventRef;
        this.titusRuntime = titusRuntime;
    }

    @PreDestroy
    public void shutdown() {
        internalSubscription.dispose();
    }

    @Override
    public D getCurrent() {
        return lastReplicatorEventRef.get().getData();
    }

    @Override
    public long getStalenessMs() {
        return titusRuntime.getClock().wallTime() - lastReplicatorEventRef.get().getLastUpdateTime();
    }

    @Override
    public Flux<Long> observeDataStalenessMs() {
        return eventStream.map(ReplicatorEvent::getLastUpdateTime);
    }

    public static <D> Flux<StreamDataReplicator<D>> newStreamDataReplicator(ReplicatorEventStream<D> replicatorEventStream,
                                                                            DataReplicatorMetrics metrics,
                                                                            TitusRuntime titusRuntime) {
        return Flux.defer(() -> {
            AtomicReference<ReplicatorEvent<D>> lastReplicatorEventRef = new AtomicReference<>();

            Flux<ReplicatorEvent<D>> eventStream = replicatorEventStream.connect().publish().autoConnect(2);

            Disposable internalSubscription = eventStream
                    .doOnSubscribe(s -> metrics.connected())
                    .doOnCancel(metrics::disconnected)
                    .subscribe(
                            next -> {
                                lastReplicatorEventRef.set(next);
                                metrics.event(titusRuntime.getClock().wallTime() - next.getLastUpdateTime());
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

            return eventStream.filter(e -> isFresh(e, titusRuntime)).take(1).map(e ->
                    new StreamDataReplicator<>(eventStream, internalSubscription, lastReplicatorEventRef, titusRuntime)
            );
        });
    }

    private static boolean isFresh(ReplicatorEvent event, TitusRuntime titusRuntime) {
        long now = titusRuntime.getClock().wallTime();
        return event.getLastUpdateTime() + STALENESS_THRESHOLD >= now;
    }
}
