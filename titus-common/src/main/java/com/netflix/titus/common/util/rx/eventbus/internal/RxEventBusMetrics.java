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

package com.netflix.titus.common.util.rx.eventbus.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;

class RxEventBusMetrics {

    private final Id eventCounterId;
    private final Id subscriberMetricsId;
    private final Registry registry;

    private final ConcurrentMap<String, SubscriberMetrics> subscriberMetrics = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<?>, Counter> eventCounters = new ConcurrentHashMap<>();

    RxEventBusMetrics(Id rootId, Registry registry) {
        this.eventCounterId = registry.createId(rootId.name() + "input", rootId.tags());
        this.subscriberMetricsId = registry.createId(rootId.name() + "subscribers", rootId.tags());
        this.registry = registry;
    }

    void subscriberAdded(String subscriberId) {
        subscriberMetrics.put(subscriberId, new SubscriberMetrics(subscriberId));
    }

    void subscriberRemoved(String subscriberId) {
        subscriberMetrics.remove(subscriberId).close();
    }

    void published(Object event) {
        Counter counter = eventCounters.computeIfAbsent(
                event.getClass(),
                c -> registry.counter(eventCounterId.withTag("class", c.getName()))
        );
        counter.increment();
    }

    void delivered(String subscriberId, long queueSize, Object event, long latency) {
        SubscriberMetrics metrics = this.subscriberMetrics.get(subscriberId);
        if (metrics != null) { // Should always be non-null
            metrics.delivered(queueSize, event, latency);
        }
    }

    void overflowed(String subscriberId) {
        SubscriberMetrics metrics = this.subscriberMetrics.get(subscriberId);
        if (metrics != null) { // Should always be non-null
            metrics.overflowed();
        }
    }

    private class SubscriberMetrics {
        private final Id eventCounterId;

        private final AtomicLong queueSizeGauge;
        private final AtomicLong latencyGauge;
        private final AtomicLong overflowGauge;
        private final ConcurrentMap<Class<?>, Counter> eventCounters = new ConcurrentHashMap<>();

        SubscriberMetrics(String subscriberId) {
            Id myId = subscriberMetricsId.withTags("subscriber", subscriberId);

            this.eventCounterId = idFor(myId, "output");
            this.queueSizeGauge = registry.gauge(idFor(myId, "queueSize"), new AtomicLong());
            this.latencyGauge = registry.gauge(idFor(myId, "latency"), new AtomicLong());
            this.overflowGauge = registry.gauge(idFor(myId, "overflow"), new AtomicLong());
        }

        private Id idFor(Id myId, String suffix) {
            return registry.createId(myId + "." + suffix, myId.tags());
        }

        // We do not reset overflow gauge, as it would disappear too quickly
        void close() {
            queueSizeGauge.set(0);
            latencyGauge.set(0);
        }

        void delivered(long queueSize, Object event, long latency) {
            queueSizeGauge.set(queueSize);
            latencyGauge.set(latency);
            Counter counter = eventCounters.computeIfAbsent(
                    event.getClass(),
                    c -> registry.counter(eventCounterId.withTag("class", c.getName()))
            );
            counter.increment();
        }

        void overflowed() {
            overflowGauge.set(1);
        }
    }
}
