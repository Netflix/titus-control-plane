/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.util.spectator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import rx.Observable;

import static java.util.Arrays.asList;

/**
 * Collection of generic higher level metrics built on top of Spectator.
 */
public final class SpectatorExt {

    public interface FsmMetrics<S> {
        void transition(S nextState);
    }

    private SpectatorExt() {
    }

    /**
     * Metric collector for a Finite State Machine (FSM). It reports a current state of an FSM, and the state transitions.
     * If FSM reaches the terminal state, the current state indicators are cleared, to prevent infinite accumulation.
     */
    public static <E extends Enum<E>> FsmMetrics<E> fsmMetrics(Collection<E> trackedStates,
                                                               Id rootId,
                                                               Function<E, Boolean> finalStateEval,
                                                               Registry registry) {
        return new FsmMetricsImpl<>(rootId, new ArrayList<>(trackedStates), Enum::name, finalStateEval, registry);
    }

    /**
     * FSM metrics for an enum type (see {@link #fsmMetrics(Collection, Id, Function, Registry)}).
     */
    public static <E extends Enum<E>> FsmMetrics<E> fsmMetrics(Class<E> fsmType,
                                                               Id rootId,
                                                               Function<E, Boolean> finalStateEval,
                                                               Registry registry) {
        return new FsmMetricsImpl<>(rootId, asList(fsmType.getEnumConstants()), Enum::name, finalStateEval, registry);
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
     * RxJava long running completable metrics.
     */
    public static ActionMetrics actionMetrics(String rootName, List<Tag> tags, Registry registry) {
        return new ActionMetrics(rootName, tags, registry);
    }
}
