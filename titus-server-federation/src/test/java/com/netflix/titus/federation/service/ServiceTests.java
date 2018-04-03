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

package com.netflix.titus.federation.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import rx.Observable;
import rx.observers.AssertableSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Common test utilities (functions).
 */
final class ServiceTests {
    private ServiceTests() {
    }

    /**
     * @param <Q> query type
     * @param <R> result type
     * @param <T> type of items in the result
     */
    static <Q, R, T> List<T> walkAllPages(int pageWalkSize,
                                          Function<Q, Observable<R>> pageFetcher,
                                          Function<com.netflix.titus.grpc.protogen.Page, Q> queryFactory,
                                          Function<R, com.netflix.titus.grpc.protogen.Pagination> paginationGetter,
                                          Function<R, List<T>> itemsGetter) {
        List<T> allItems = new ArrayList<>();
        Optional<R> lastResult = Optional.empty();
        int currentCursorPosition = -1;
        int currentPageNumber = 0;

        while (lastResult.map(r -> paginationGetter.apply(r).getHasMore()).orElse(true)) {
            com.netflix.titus.grpc.protogen.Page.Builder builder = com.netflix.titus.grpc.protogen.Page.newBuilder().setPageSize(pageWalkSize);
            if (lastResult.isPresent()) {
                builder.setCursor(paginationGetter.apply(lastResult.get()).getCursor());
            } else {
                builder.setPageNumber(0);
            }

            Q query = queryFactory.apply(builder.build());
            AssertableSubscriber<R> testSubscriber = pageFetcher.apply(query).test();
            testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
            testSubscriber.assertNoErrors().assertCompleted();
            testSubscriber.assertValueCount(1);

            final List<R> results = testSubscriber.getOnNextEvents();
            assertThat(results).hasSize(1);
            R result = results.get(0);
            List<T> items = itemsGetter.apply(result);
            com.netflix.titus.grpc.protogen.Pagination pagination = paginationGetter.apply(result);
            if (pagination.getHasMore()) {
                assertThat(items).hasSize(pageWalkSize);
            }
            currentCursorPosition += items.size();
            if (pagination.getTotalItems() > 0) {
                assertThat(pagination.getCursorPosition()).isEqualTo(currentCursorPosition);
            } else {
                assertThat(pagination.getCursorPosition()).isEqualTo(0);
            }
            assertThat(pagination.getCurrentPage().getPageNumber()).isEqualTo(currentPageNumber++);
            allItems.addAll(items);
            lastResult = Optional.of(result);
            testSubscriber.unsubscribe();
        }

        return allItems;
    }
}

