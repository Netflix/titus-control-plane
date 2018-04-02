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

package com.netflix.titus.common.framework.fit;

import java.util.function.Supplier;

import com.netflix.titus.common.framework.fit.internal.ActiveFitFramework;
import com.netflix.titus.common.framework.fit.internal.DefaultFitRegistry;
import com.netflix.titus.common.framework.fit.internal.InactiveFitFramework;
import com.netflix.titus.common.framework.fit.internal.action.FitErrorAction;
import com.netflix.titus.common.framework.fit.internal.action.FitLatencyAction;
import com.netflix.titus.common.util.tuple.Pair;

import static java.util.Arrays.asList;

/**
 * Factory methods for the FIT (Failure Injection Testing) framework.
 */
public abstract class FitFramework {

    /**
     * Returns true if FIT framework is activated.
     */
    public abstract boolean isActive();

    /**
     * Returns the default FIT action registry, with the predefined/standard action set.
     */
    public abstract FitRegistry getFitRegistry();

    /**
     * Top level FIT component.
     */
    public abstract FitComponent getRootComponent();

    /**
     * Creates a new, empty FIT component.
     */
    public abstract FitInjection.Builder newFitInjectionBuilder(String id);

    /**
     * Creates an interface proxy with methods instrumented using the provided {@link FitInjection} instance:
     * <ul>
     * <li>
     * All methods returning {@link java.util.concurrent.CompletableFuture} are wrapped with
     * {@link FitInjection#aroundCompletableFuture(String, Supplier)} operator.
     * </li>
     * <li>
     * All methods returning {@link com.google.common.util.concurrent.ListenableFuture} are wrapped with
     * {@link FitInjection#aroundListenableFuture(String, Supplier)} operator.
     * </li>
     * <li>
     * All methods returning {@link rx.Observable} are wrapped with
     * {@link FitInjection#aroundObservable(String, Supplier)} operator.
     * </li>
     * <li>
     * All the remaining interface methods are assumed to be blocking, and are wrapped with
     * {@link FitInjection#beforeImmediate(String)}, and {@link FitInjection#afterImmediate(String)} handlers.
     * </li>
     * </ul>
     */
    public abstract <I> I newFitProxy(I interf, FitInjection injection);

    /**
     * New active FIT framework.
     */
    public static FitFramework newFitFramework() {
        return new ActiveFitFramework(newDefaultRegistry());
    }

    /**
     * New inactive FIT framework. All {@link FitFramework} API methods throw an exception if called, except {@link #isActive()}
     * method, which can be called to check the activation status.
     */
    public static FitFramework inactiveFitFramework() {
        return new InactiveFitFramework();
    }

    private static DefaultFitRegistry newDefaultRegistry() {
        return new DefaultFitRegistry(
                asList(
                        Pair.of(FitErrorAction.DESCRIPTOR, (id, properties) -> injection -> new FitErrorAction(id, properties, injection)),
                        Pair.of(FitLatencyAction.DESCRIPTOR, (id, properties) -> injection -> new FitLatencyAction(id, properties, injection))
                )
        );
    }
}
