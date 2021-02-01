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

package com.netflix.titus.api.supervisor.service.resolver;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;

import com.netflix.titus.api.supervisor.model.MasterInstanceFunctions;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class PollingLocalMasterReadinessResolver implements LocalMasterReadinessResolver {

    private static final Logger logger = LoggerFactory.getLogger(PollingLocalMasterReadinessResolver.class);

    private final AtomicReference<ReadinessStatus> readinessStatusRef = new AtomicReference<>();
    private final DirectProcessor<ReadinessStatus> readinessUpdatesProcessor = DirectProcessor.create();

    private final ScheduleReference refreshReference;

    private PollingLocalMasterReadinessResolver(Mono<ReadinessStatus> readinessStatusMono,
                                                ScheduleDescriptor scheduleDescriptor,
                                                TitusRuntime titusRuntime,
                                                Scheduler scheduler) {
        readinessStatusRef.set(ReadinessStatus.notReadyNow(titusRuntime.getClock().wallTime()));
        refreshReference = titusRuntime.getLocalScheduler().scheduleMono(
                scheduleDescriptor,
                context ->
                        readinessStatusMono
                                .doOnNext(this::process)
                                .ignoreElement()
                                .cast(Void.class),
                scheduler
        );
    }

    @PreDestroy
    public void shutdown() {
        refreshReference.cancel();
        readinessUpdatesProcessor.onComplete();
    }

    @Override
    public Flux<ReadinessStatus> observeLocalMasterReadinessUpdates() {
        return readinessUpdatesProcessor
                .transformDeferred(ReactorExt.head(() -> Collections.singletonList(readinessStatusRef.get())))
                .transformDeferred(ReactorExt.badSubscriberHandler(logger));
    }

    private void process(ReadinessStatus newReadinessStatus) {
        if (newReadinessStatus == null) {
            return;
        }
        ReadinessStatus previousReadinessStatus = readinessStatusRef.get();
        if (MasterInstanceFunctions.areDifferent(newReadinessStatus, previousReadinessStatus)) {
            logger.info("Changing Master readiness status: previous={}, new={}", previousReadinessStatus, newReadinessStatus);
        } else {
            logger.debug("Master readiness status not changed: current={}", previousReadinessStatus);
        }

        readinessStatusRef.set(newReadinessStatus);
        readinessUpdatesProcessor.onNext(newReadinessStatus);
    }

    public static PollingLocalMasterReadinessResolver newPollingResolver(Mono<ReadinessStatus> readinessStatusMono,
                                                                         ScheduleDescriptor scheduleDescriptor,
                                                                         TitusRuntime titusRuntime,
                                                                         Scheduler scheduler) {
        return new PollingLocalMasterReadinessResolver(readinessStatusMono, scheduleDescriptor, titusRuntime, scheduler);
    }
}
