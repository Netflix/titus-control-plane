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

package com.netflix.titus.master.eviction.service.quota;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.model.event.EvictionQuotaEvent;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.master.eviction.service.EvictionServiceConfiguration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * {@link QuotaEventEmitter} emits quota updates at a regular interval. It is accomplished by polling periodically
 * all quota providers and computing the difference between the last and current state.
 */
@Singleton
public class QuotaEventEmitter {

    private static final ScheduleDescriptor SCHEDULE_DESCRIPTOR = ScheduleDescriptor.newBuilder()
            .withName(QuotaEventEmitter.class.getSimpleName())
            .withDescription("Quota update events")
            .withInterval(Duration.ofSeconds(1))
            .withRetryerSupplier(Retryers::never)
            .withTimeout(Duration.ofSeconds(5))
            .build();

    private final V3JobOperations jobOperations;
    private final TitusQuotasManager quotasManager;
    private final ScheduleReference scheduleReference;

    @VisibleForTesting
    final Set<SinkHolder> eventSubscriberSinks = Sets.newConcurrentHashSet();

    @Inject
    public QuotaEventEmitter(EvictionServiceConfiguration configuration,
                             V3JobOperations jobOperations,
                             TitusQuotasManager quotasManager,
                             TitusRuntime titusRuntime) {
        this.jobOperations = jobOperations;
        this.quotasManager = quotasManager;
        this.scheduleReference = titusRuntime.getLocalScheduler().schedule(
                SCHEDULE_DESCRIPTOR.toBuilder()
                        .withInterval(Duration.ofMillis(configuration.getEventStreamQuotaUpdateIntervalMs()))
                        .build(),
                this::refresh,
                true
        );
    }

    @PreDestroy
    public void shutdown() {
        scheduleReference.close();
    }

    public Flux<EvictionEvent> events(boolean includeSnapshot) {
        return Flux.create(sink -> {
            Preconditions.checkState(scheduleReference != null && !scheduleReference.isClosed());
            eventSubscriberSinks.add(new SinkHolder(sink, includeSnapshot));
            sink.onDispose(() -> eventSubscriberSinks.remove(sink));
        });
    }

    private void refresh(ExecutionContext context) {
        eventSubscriberSinks.forEach(sinkHolder -> {
            if (sinkHolder.getSink().isCancelled()) {
                eventSubscriberSinks.remove(sinkHolder);
            } else {
                sinkHolder.refresh();
            }
        });
    }

    private class SinkHolder {

        private final FluxSink<EvictionEvent> sink;
        private Map<Reference, EvictionQuota> emittedQuotas = Collections.emptyMap();
        private boolean includeSnapshot;

        private SinkHolder(FluxSink<EvictionEvent> sink, boolean includeSnapshot) {
            this.sink = sink;
            this.includeSnapshot = includeSnapshot;
        }

        private FluxSink<EvictionEvent> getSink() {
            return sink;
        }

        public void refresh() {
            try {
                if (includeSnapshot) {
                    firstRefreshWithSnapshot();
                } else if (emittedQuotas.isEmpty()) {
                    refreshIfNoPreviousEmits();
                } else {
                    refreshWithPreviousEmits();
                }
            } catch (Exception e) {
                sink.error(e);
                eventSubscriberSinks.remove(this);
            }
        }

        private void firstRefreshWithSnapshot() {
            HashMap<Reference, EvictionQuota> newlyEmittedQuotas = new HashMap<>();
            buildSnapshot().forEach(event -> {
                sink.next(event);
                newlyEmittedQuotas.put(event.getQuota().getReference(), event.getQuota());
            });
            sink.next(EvictionEvent.newSnapshotEndEvent());
            this.includeSnapshot = false;
            this.emittedQuotas = newlyEmittedQuotas;
        }

        private void refreshIfNoPreviousEmits() {
            HashMap<Reference, EvictionQuota> newlyEmittedQuotas = new HashMap<>();
            buildSnapshot().forEach(event -> {
                sink.next(event);
                newlyEmittedQuotas.put(event.getQuota().getReference(), event.getQuota());
            });
            this.emittedQuotas = newlyEmittedQuotas;
        }

        private void refreshWithPreviousEmits() {
            HashMap<Reference, EvictionQuota> newlyEmittedQuotas = new HashMap<>();
            buildSnapshot().forEach(event -> {
                EvictionQuota quota = event.getQuota();
                EvictionQuota previous = emittedQuotas.get(quota.getReference());
                if (previous == null || quota.getQuota() != previous.getQuota()) {
                    sink.next(event);
                }
                newlyEmittedQuotas.put(quota.getReference(), quota);
            });
            this.emittedQuotas = newlyEmittedQuotas;
        }

        private List<EvictionQuotaEvent> buildSnapshot() {
            List<EvictionQuotaEvent> snapshot = new ArrayList<>();

            snapshot.add(EvictionEvent.newQuotaEvent(quotasManager.findEvictionQuota(Reference.system()).get()));
            jobOperations.getJobs()
                    .forEach(job -> quotasManager
                            .findEvictionQuota(Reference.job(job.getId()))
                            .ifPresent(quota -> snapshot.add(EvictionEvent.newQuotaEvent(quota)))
                    );

            return snapshot;
        }
    }
}
