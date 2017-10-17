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

package io.netflix.titus.ext.cassandra.executor;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import rx.Observable;
import rx.Subscriber;
import rx.subscriptions.Subscriptions;

public class FuturePaginatedQuery<PAGE, T> {

    private final Subscriber<? super T> subscriber;
    private final Function<PAGE, ListenableFuture<PAGE>> next;
    private final BiFunction<PAGE, Integer, List<T>> itemsFetcher;
    private final Function<PAGE, Boolean> endStatusFetcher;

    private volatile boolean cancelled;
    private volatile ListenableFuture<PAGE> currentFuture;
    private volatile int fetchedItemsCount;

    public FuturePaginatedQuery(Subscriber<? super T> subscriber,
                                Supplier<ListenableFuture<PAGE>> initial,
                                Function<PAGE, ListenableFuture<PAGE>> next,
                                BiFunction<PAGE, Integer, List<T>> itemsFetcher,
                                Function<PAGE, Boolean> endStatusFetcher) {
        this.subscriber = subscriber;
        this.next = next;
        this.itemsFetcher = itemsFetcher;
        this.endStatusFetcher = endStatusFetcher;

        subscriber.add(Subscriptions.create(this::cancel));
        handleReply(initial.get());
    }

    private void cancel() {
        cancelled = true;
        if (currentFuture != null) {
            currentFuture.cancel(true);
        }
    }

    private void handleReply(ListenableFuture<PAGE> pageFuture) {
        currentFuture = pageFuture;
        Futures.addCallback(pageFuture, new FutureCallback<PAGE>() {
            @Override
            public void onSuccess(PAGE page) {
                if (!cancelled) {
                    List<T> pageItems = itemsFetcher.apply(page, fetchedItemsCount);
                    fetchedItemsCount += pageItems.size();
                    pageItems.forEach(subscriber::onNext);
                    if (endStatusFetcher.apply(page)) {
                        subscriber.onCompleted();
                    } else {
                        if (!cancelled) {
                            handleReply(next.apply(page));
                        }
                    }
                }
            }

            @Override
            public void onFailure(Throwable error) {
                if (!cancelled) {
                    subscriber.onError(error);
                }
            }
        });
    }

    /**
     * Converts futures based paginated query into an observable.
     */
    public static <PAGE, T> Observable<T> paginatedQuery(Supplier<ListenableFuture<PAGE>> initial,
                                                         Function<PAGE, ListenableFuture<PAGE>> next,
                                                         BiFunction<PAGE, Integer, List<T>> itemsFetcher,
                                                         Function<PAGE, Boolean> endStatusFetcher) {
        return Observable.create(subscriber -> new FuturePaginatedQuery<>(subscriber, initial, next, itemsFetcher, endStatusFetcher));
    }
}
