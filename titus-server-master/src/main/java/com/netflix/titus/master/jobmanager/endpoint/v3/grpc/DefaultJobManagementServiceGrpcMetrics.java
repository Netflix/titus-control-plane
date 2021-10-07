/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.endpoint.v3.grpc;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.master.MetricConstants;

/**
 * Metrics for DefaultJobManagementServiceGrpc. They are supplementary to the GRPC metrics interceptor to provide
 * more insight into the internal event stream processing.
 */
class DefaultJobManagementServiceGrpcMetrics {

    private static final String ROOT = MetricConstants.METRIC_JOB_MANAGER + "grpcServer.";

    private static final ScheduleDescriptor SCHEDULE_DESCRIPTOR = ScheduleDescriptor.newBuilder()
            .withName(DefaultJobManagementServiceGrpcMetrics.class.getSimpleName())
            .withDescription("GRPC job service metrics updater")
            .withInitialDelay(Duration.ZERO)
            .withInterval(Duration.ofSeconds(1))
            .withTimeout(Duration.ofSeconds(5))
            .withRetryerSupplier(Retryers::never)
            .build();

    private final Registry registry;

    private final ConcurrentMap<String, StreamHolder> streamHolders = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Gauge> pendingSubscriptionsByCallerId = new ConcurrentHashMap<>();
    private final ScheduleReference scheduleRef;

    DefaultJobManagementServiceGrpcMetrics(TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();
        this.scheduleRef = titusRuntime.getLocalScheduler().schedule(SCHEDULE_DESCRIPTOR, context -> updatePendingSubscriptionsGauges(), true);
    }

    void shutdown() {
        scheduleRef.cancel();
    }

    void observeJobsStarted(String trxId, CallMetadata callMetadata) {
        streamHolders.put(trxId, new StreamHolder(callMetadata));
    }

    void observeJobsUnsubscribed(String trxId, long elapsed) {
        StreamHolder holder = streamHolders.remove(trxId);
        if (holder != null) {
            holder.unsubscribed(elapsed);
        }
    }

    void observeJobsError(String trxId, long elapsed, Throwable e) {
        StreamHolder holder = streamHolders.remove(trxId);
        if (holder != null) {
            holder.error(e.getClass().getSimpleName(), elapsed);
        }
    }

    void observeJobsCompleted(String trxId, long elapsed) {
        StreamHolder holder = streamHolders.remove(trxId);
        if (holder != null) {
            holder.completed(elapsed);
        }
    }

    void observeJobsEventEmitted(String trxId) {
        StreamHolder holder = streamHolders.get(trxId);
        if (holder != null) {
            holder.eventEmitted();
        }
    }

    void updatePendingSubscriptionsGauges() {
        Map<String, Integer> counters = new HashMap<>();
        for (StreamHolder holder : streamHolders.values()) {
            counters.put(holder.getCallerId(), counters.getOrDefault(holder.getCallerId(), 0) + 1);
        }
        Set<String> lost = CollectionsExt.copyAndRemove(pendingSubscriptionsByCallerId.keySet(), counters.keySet());
        for (String id : lost) {
            Gauge gauge = pendingSubscriptionsByCallerId.remove(id);
            if (gauge != null) {
                gauge.set(0);
            }
        }
        counters.forEach((callerId, count) -> {
            Gauge gauge = pendingSubscriptionsByCallerId.computeIfAbsent(callerId, c ->
                    registry.gauge(ROOT + "observeJobsSubscriptions", "callerId", callerId)
            );
            gauge.set(count);
        });
    }

    private class StreamHolder {

        private final String callerId;
        private final Counter eventsCounter;

        private StreamHolder(CallMetadata callMetadata) {
            this.callerId = CollectionsExt.isNullOrEmpty(callMetadata.getCallers()) ? "unknown" : callMetadata.getCallers().get(0).getId();
            this.eventsCounter = registry.counter(ROOT + "observeJobsSubscriptionEvents",
                    "callerId", callerId);
        }

        private String getCallerId() {
            return callerId;
        }

        private void unsubscribed(long elapsed) {
            registry.timer(ROOT + "observeJobsSubscriptionExecutionTime",
                    "callerId", callerId,
                    "status", "unsubscribed"
            ).record(elapsed, TimeUnit.MILLISECONDS);
        }

        private void error(String error, long elapsed) {
            registry.timer(ROOT + "observeJobsSubscriptionExecutionTime",
                    "callerId", callerId,
                    "status", "error",
                    "error", error
            ).record(elapsed, TimeUnit.MILLISECONDS);
        }

        private void completed(long elapsed) {
            registry.timer(ROOT + "observeJobsSubscriptionExecutionTime",
                    "callerId", callerId,
                    "status", "completed"
            ).record(elapsed, TimeUnit.MILLISECONDS);
        }

        private void eventEmitted() {
            eventsCounter.increment();
        }
    }
}
