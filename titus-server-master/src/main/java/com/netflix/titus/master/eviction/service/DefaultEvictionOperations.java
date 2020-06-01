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

package com.netflix.titus.master.eviction.service;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.eviction.service.EvictionOperations;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.master.eviction.service.quota.QuotaEventEmitter;
import com.netflix.titus.master.eviction.service.quota.TitusQuotasManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Singleton
public class DefaultEvictionOperations implements EvictionOperations {

    private static final Logger logger = LoggerFactory.getLogger(DefaultEvictionOperations.class);

    private final V3JobOperations jobOperations;
    private final TitusQuotasManager quotaManager;
    private final TitusRuntime titusRuntime;
    private final Scheduler scheduler;
    private final EvictionServiceConfiguration configuration;

    private QuotaEventEmitter quotEventEmitter;
    private TaskTerminationExecutor taskTerminationExecutor;

    @Inject
    public DefaultEvictionOperations(EvictionServiceConfiguration configuration,
                                     V3JobOperations jobOperations,
                                     TitusQuotasManager quotaManager,
                                     TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.jobOperations = jobOperations;
        this.quotaManager = quotaManager;
        this.titusRuntime = titusRuntime;

        Executor executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, DefaultEvictionOperations.class.getSimpleName());
            thread.setDaemon(true);
            return thread;
        });
        this.scheduler = Schedulers.fromExecutor(executor);
    }

    @Activator
    public void enterActiveMode() {
        this.quotEventEmitter = new QuotaEventEmitter(configuration, jobOperations, quotaManager, titusRuntime);
        this.taskTerminationExecutor = new TaskTerminationExecutor(configuration, jobOperations, quotaManager, titusRuntime, scheduler);
    }

    @PreDestroy
    public void shutdown() {
        if (quotaManager != null) {
            quotaManager.shutdown();
        }
        if (quotEventEmitter != null) {
            quotEventEmitter.shutdown();
        }
        if (taskTerminationExecutor != null) {
            taskTerminationExecutor.shutdown();
        }
        scheduler.dispose();
    }

    @Override
    public EvictionQuota getEvictionQuota(Reference reference) {
        return quotaManager.findEvictionQuota(reference).orElseThrow(() -> EvictionException.noQuotaFound(reference));
    }

    @Override
    public Optional<EvictionQuota> findEvictionQuota(Reference reference) {
        return quotaManager.findEvictionQuota(reference);
    }

    @Override
    public Mono<Void> terminateTask(String taskId, String reason, String callerId) {
        return taskTerminationExecutor.terminateTask(taskId, reason, callerId);
    }

    @Override
    public Flux<EvictionEvent> events(boolean includeSnapshot) {
        return ReactorExt.protectFromMissingExceptionHandlers(
                Flux.merge(quotEventEmitter.events(includeSnapshot), taskTerminationExecutor.events()),
                logger
        );
    }

    private EvictionQuota toVeryHighQuota(Reference reference) {
        return EvictionQuota.newBuilder()
                .withReference(reference)
                .withQuota(VERY_HIGH_QUOTA)
                .build();
    }
}
