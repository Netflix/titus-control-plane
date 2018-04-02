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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.google.common.util.concurrent.ListenableFuture;
import rx.Observable;

public abstract class AbstractFitAction implements FitAction {

    private final String id;
    private final FitActionDescriptor descriptor;
    private final Map<String, String> properties;
    private final FitInjection injection;
    protected final boolean runBefore;

    protected AbstractFitAction(String id, FitActionDescriptor descriptor, Map<String, String> properties, FitInjection injection) {
        this.id = id;
        this.descriptor = descriptor;
        this.properties = properties;
        this.injection = injection;
        this.runBefore = Boolean.parseBoolean(properties.getOrDefault("before", "true"));
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public FitActionDescriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    public FitInjection getInjection() {
        return injection;
    }

    @Override
    public void beforeImmediate(String injectionPoint) {
    }

    @Override
    public void afterImmediate(String injectionPoint) {
    }

    @Override
    public <T> T afterImmediate(String injectionPoint, T result) {
        return result;
    }

    @Override
    public <T> Supplier<Observable<T>> aroundObservable(String injectionPoint, Supplier<Observable<T>> source) {
        return source;
    }

    @Override
    public <T> Supplier<CompletableFuture<T>> aroundCompletableFuture(String injectionPoint, Supplier<CompletableFuture<T>> source) {
        return source;
    }

    @Override
    public <T> Supplier<ListenableFuture<T>> aroundListenableFuture(String injectionPoint, Supplier<ListenableFuture<T>> source) {
        return source;
    }
}
