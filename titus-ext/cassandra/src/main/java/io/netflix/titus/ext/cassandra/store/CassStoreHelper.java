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

package io.netflix.titus.ext.cassandra.store;

import io.netflix.titus.api.jobmanager.store.JobStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;
import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.exceptions.Exceptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;


public class CassStoreHelper {
    private static Logger log = LoggerFactory.getLogger(CassStoreHelper.class);
    private Session session;

    public CassStoreHelper(Session session) {
        this.session = session;
    }


    public Observable<ResultSet> executeQuery(String query) {
        return buildResultSetObservable(() -> session.executeAsync(query));
    }

    public Observable<ResultSet> execute(Statement statement) {
        return buildResultSetObservable(() -> session.executeAsync(statement));
    }


    private Observable<ResultSet> buildResultSetObservable(Supplier<ResultSetFuture> resultSetFutureSupplier) {
        return Observable.create(emitter -> {
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
        }, Emitter.BackpressureMode.NONE);

    }
}
