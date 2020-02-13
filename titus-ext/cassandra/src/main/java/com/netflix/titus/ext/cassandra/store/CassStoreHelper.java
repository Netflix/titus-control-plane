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

package com.netflix.titus.ext.cassandra.store;

import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.netflix.titus.api.jobmanager.store.JobStoreException;
import rx.Emitter;
import rx.Observable;
import rx.Scheduler;


public class CassStoreHelper {
    private final Session session;
    private final Scheduler scheduler;

    /**
     * Results from queries will have pages fetched on demand, which can block code iterating on
     * {@link ResultSet result sets}. For that reason, it is recommended a Scheduler suitable for (slow) blocking
     * operations is used to process results.
     *
     * @param session   a C* session where queries will be executed
     * @param scheduler where results (callbacks) will be processed so C* driver threads are not blocked
     */
    public CassStoreHelper(Session session, Scheduler scheduler) {
        this.session = session;
        this.scheduler = scheduler;
    }

    public Observable<ResultSet> execute(Statement statement) {
        return buildResultSetObservable(() -> session.executeAsync(statement));
    }

    private Observable<ResultSet> buildResultSetObservable(Supplier<ResultSetFuture> resultSetFutureSupplier) {
        return Observable.<ResultSet>create(emitter -> {
                    ResultSetFuture resultSetFuture = resultSetFutureSupplier.get();
                    Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(@Nullable ResultSet result) {
                            emitter.onNext(result);
                            emitter.onCompleted();
                        }

                        @Override
                        public void onFailure(@Nonnull Throwable e) {
                            emitter.onError(JobStoreException.cassandraDriverError(e));
                        }
                    });
                },
                Emitter.BackpressureMode.NONE
        ).observeOn(scheduler);
    }
}
