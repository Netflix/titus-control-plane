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

package com.netflix.titus.ext.cassandra.executor;

import java.io.IOException;
import javax.annotation.Nullable;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import rx.Observable;
import rx.subscriptions.Subscriptions;

public class UpdateOperations {

    private final Session session;

    public UpdateOperations(ExecutionContext context) {
        this.session = context.getSession();
    }

    Observable<Void> executeUpdate(BoundStatement statement) {
        return Observable.create(subscriber -> {
            ResultSetFuture resultSetFuture;
            try {
                resultSetFuture = session.executeAsync(statement);
            } catch (Exception e) {
                subscriber.onError(e);
                return;
            }

            Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(@Nullable ResultSet rows) {
                    if (rows != null && !rows.isExhausted()) {
                        subscriber.onError(new IOException("Update operation should return empty result set"));
                    } else {
                        subscriber.onCompleted();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    subscriber.onError(e);
                }
            }, MoreExecutors.directExecutor());
            subscriber.add(Subscriptions.create(() -> resultSetFuture.cancel(false)));
        });
    }

    void executeBlockingUpdate(BoundStatement statement) {
        executeUpdate(statement).toBlocking().firstOrDefault(null);
    }
}
