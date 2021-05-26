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

package com.netflix.titus.common.util.spectator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import rx.Observable;

/**
 * Collection of generic higher level metrics built on top of Spectator.
 */
public final class SpectatorExt {

    public static final int MAX_TAG_VALUE_LENGTH = 120;

    private static final int UUID_LENGTH = UUID.randomUUID().toString().length();

    public interface FsmMetrics<S> {
        void transition(S nextState);

        void transition(S nextState, String reason);
    }

    private SpectatorExt() {
    }

    /**
     * Spectator tag value size is limited to {@link #MAX_TAG_VALUE_LENGTH}. Tags with values larger than this are
     * dropped from metrics. This function trims long values to fit into the Spectator constraints.
     */
    public static String trimTagValue(String value) {
        if (value == null || value.isEmpty() || value.length() <= MAX_TAG_VALUE_LENGTH) {
            return value;
        }
        return value.substring(0, MAX_TAG_VALUE_LENGTH);
    }

    /**
     * Return a unique tag value with the provided base (value prefix), and a collision predicate.
     * The returned value is guaranteed to be unique and within the Spectator tag value limits.
     */
    public static String uniqueTagValue(String base, Predicate<String> collisionCheck) {
        String trimmed = trimTagValue(base);
        if (!collisionCheck.test(trimmed)) {
            return trimmed;
        }
        String trimmedBase = (trimmed.length() + 1 + UUID_LENGTH) <= MAX_TAG_VALUE_LENGTH
                ? trimmed + '#'
                : trimmed.substring(0, MAX_TAG_VALUE_LENGTH - UUID_LENGTH - 1) + '#';
        String value;
        do {
            value = trimmedBase + UUID.randomUUID();
        } while (collisionCheck.test(value));
        return value;
    }

    /**
     * Metric collector for a Finite State Machine (FSM). It reports a current state of an FSM, and the state transitions.
     * If FSM reaches the terminal state, the current state indicators are cleared, to prevent infinite accumulation.
     */
    public static <E extends Enum<E>> FsmMetrics<E> fsmMetrics(Id rootId,
                                                               Function<E, Boolean> finalStateEval,
                                                               E initialState,
                                                               Registry registry) {
        return new FsmMetricsImpl<>(rootId, Enum::name, finalStateEval, initialState, registry);
    }

    /**
     * Monitors a value, and a collection of buckets, each denoting a value range. If a current value is within a
     * particular bucket/range, its gauge is set to 1. All the other buckets are set to 0.
     * If a current value is less than zero, all gauge values are set to 0.
     */
    public static <SOURCE> ValueRangeMetrics<SOURCE> valueRangeMetrics(Id rootId,
                                                                       long[] levels,
                                                                       SOURCE source,
                                                                       Function<SOURCE, Long> valueSupplier,
                                                                       Registry registry) {
        return ValueRangeMetrics.metricsOf(rootId, levels, source, valueSupplier, registry);
    }

    /**
     * Creates a composite gauge consisting of a set of basic gauges each with a unique set of discerning tags.
     */
    public static MultiDimensionalGauge multiDimensionalGauge(Id rootId, Collection<String> discerningTagNames, Registry registry) {
        return new MultiDimensionalGauge(rootId, discerningTagNames, registry);
    }

    /**
     * RxJava subscription metrics.
     */
    public static <T> Observable.Transformer<T, T> subscriptionMetrics(String rootName, List<Tag> commonTags, Registry registry) {
        return new SubscriptionMetrics<>(rootName, commonTags, registry);
    }

    /**
     * RxJava subscription metrics.
     */
    public static <T> Observable.Transformer<T, T> subscriptionMetrics(String rootName, Class<?> type, Registry registry) {
        return new SubscriptionMetrics<>(rootName, type, registry);
    }

    /**
     * RxJava subscription metrics.
     */
    public static <T> Observable.Transformer<T, T> subscriptionMetrics(String rootName, Registry registry) {
        return new SubscriptionMetrics<>(rootName, Collections.emptyList(), registry);
    }

    /**
     * RxJava metrics transformer for {@link Observable observables}, {@link rx.Completable completables}, or
     * {@link rx.Single singles} that are continuously scheduled.
     */
    public static ContinuousSubscriptionMetrics continuousSubscriptionMetrics(String rootName, List<Tag> tags, Registry registry) {
        return new ContinuousSubscriptionMetrics(rootName, tags, registry);
    }

    /**
     * Collection of metrics for tracking a status of an action run periodically.
     */
    public static ActionMetrics actionMetrics(String rootName, List<Tag> tags, Registry registry) {
        return new ActionMetrics(registry.createId(rootName, tags), registry);
    }

    /**
     * Collection of metrics for tracking a status of an action run periodically.
     */
    public static ActionMetrics actionMetrics(Id rootId, Registry registry) {
        return new ActionMetrics(rootId, registry);
    }
}
