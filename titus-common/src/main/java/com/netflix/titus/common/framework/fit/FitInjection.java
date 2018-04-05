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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.google.common.util.concurrent.ListenableFuture;
import rx.Observable;

/**
 * {@link FitInjection} represents a particular point in code or several codes for which the same FIT action or action set
 * should be triggered. For example a database driver API may be associated with one {@link FitInjection} instance to
 * generate random errors, irrespective of which operation is called. If different errors should be generated for
 * read and write operations, two {@link FitInjection} instances would be needed to handle each case separately.
 */
public interface FitInjection {

    /**
     * A unique id within a scope of {@link FitComponent} which owns this instance.
     */
    String getId();

    /**
     * Human readable text describing the purpose of this injection point.
     */
    String getDescription();

    /**
     * Exception type to throw when creating a syntactic error.
     */
    Class<? extends Throwable> getExceptionType();

    /**
     * Adds new FIT action.
     */
    void addAction(FitAction action);

    /**
     * Returns all FIT actions associated with this injection point.
     */
    List<FitAction> getActions();

    /**
     * Gets FIT action by id.
     *
     * @throws IllegalArgumentException if action with the provided id is not found
     */
    FitAction getAction(String actionId);

    /**
     * Returns an action with the given id if found or {@link Optional#empty()} otherwise.
     */
    Optional<FitAction> findAction(String actionId);

    /**
     * Removes an action with the given id.
     *
     * @return true if the action was found, and removed
     */
    boolean removeAction(String actionId);

    /**
     * Method to be called in the beginning of the client's code section.
     */
    void beforeImmediate(String injectionPoint);

    /**
     * Method to be called at the end of the client's code section.
     */
    void afterImmediate(String injectionPoint);

    /**
     * Method to be called at the end of the client's code section.
     *
     * @param result value returned by the client, which can be intercepted and modified by the FIT action
     */
    <T> T afterImmediate(String injectionPoint, T result);

    /**
     * Executes before/after FIT handlers around the provided action.
     */
    default void aroundImmediate(String injectionPoint, Runnable action) {
        beforeImmediate(injectionPoint);
        action.run();
        afterImmediate(injectionPoint);
    }

    /**
     * Wraps an observable to inject on subscribe or after completion errors.
     */
    <T> Observable<T> aroundObservable(String injectionPoint, Supplier<Observable<T>> source);

    /**
     * Wraps a Java future to inject an error before the future creation or after it completes.
     */
    <T> CompletableFuture<T> aroundCompletableFuture(String injectionPoint, Supplier<CompletableFuture<T>> source);

    /**
     * Wraps a Guava future to inject an error before the future creation or after it completes.
     */
    <T> ListenableFuture<T> aroundListenableFuture(String injectionPoint, Supplier<ListenableFuture<T>> source);

    interface Builder {

        Builder withDescription(String description);

        Builder withExceptionType(Class<? extends Throwable> exceptionType);

        FitInjection build();
    }
}
