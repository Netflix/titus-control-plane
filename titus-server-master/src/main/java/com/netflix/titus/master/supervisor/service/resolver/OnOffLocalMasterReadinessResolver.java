/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.supervisor.service.resolver;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.netflix.titus.api.supervisor.model.MasterInstanceFunctions;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

/**
 * A wrapper to enforce lifecycle state of the wrapped delegate.
 */
public class OnOffLocalMasterReadinessResolver implements LocalMasterReadinessResolver {

    private static final Logger logger = LoggerFactory.getLogger(OnOffLocalMasterReadinessResolver.class);

    private final LocalMasterReadinessResolver delegate;
    private final Supplier<Boolean> isOn;
    private final Supplier<ReadinessStatus> enforcedStatusSupplier;
    private final Duration checkInterval;
    private final Scheduler scheduler;

    public OnOffLocalMasterReadinessResolver(LocalMasterReadinessResolver delegate,
                                             Supplier<Boolean> isOn,
                                             Supplier<ReadinessStatus> enforcedStatusSupplier,
                                             Duration checkInterval,
                                             Scheduler scheduler) {
        this.delegate = delegate;
        this.isOn = isOn;
        this.enforcedStatusSupplier = enforcedStatusSupplier;
        this.checkInterval = checkInterval;
        this.scheduler = scheduler;
    }

    @Override
    public Flux<ReadinessStatus> observeLocalMasterReadinessUpdates() {
        return Flux.defer(() -> {
            AtomicReference<ReadinessStatus> lastDelegateRef = new AtomicReference<>();
            AtomicReference<ReadinessStatus> lastEmittedRef = new AtomicReference<>();

            Flux<ReadinessStatus> onStream = delegate.observeLocalMasterReadinessUpdates()
                    .doOnNext(lastDelegateRef::set)
                    .filter(s -> isOn.get());

            Flux<ReadinessStatus> offStream = Flux.interval(checkInterval, scheduler)
                    .filter(s -> !isOn.get() || lastDelegateRef.get() != lastEmittedRef.get())
                    .map(tick -> isOn.get() ? lastDelegateRef.get() : enforcedStatusSupplier.get());

            return Flux.merge(onStream, offStream).doOnNext(update -> {
                if (MasterInstanceFunctions.areDifferent(lastEmittedRef.get(), update)) {
                    logger.info("Master readiness state change detected: {}", update);
                } else {
                    logger.debug("Master readiness status not changed: current={}", lastEmittedRef.get());
                }
                lastEmittedRef.set(update);
            });
        });
    }
}
