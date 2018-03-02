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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.ConstraintViolation;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.netflix.titus.api.agent.store.AgentStoreException;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Emitter;
import rx.Observable;

/**
 * TODO to share common store related code, we need to have a common base class for store exceptions
 */
final class StoreUtils {

    private static final Logger logger = LoggerFactory.getLogger(StoreUtils.class);

    private StoreUtils() {
    }

    /**
     * Execute single CASS statement, wrapping it into observable.
     */
    static Observable<ResultSet> execute(Session session, Statement statement) {
        return Observable.create(emitter -> {
            ResultSetFuture resultSetFuture = session.executeAsync(statement);
            Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(@Nullable ResultSet result) {
                    emitter.onNext(result);
                    emitter.onCompleted();
                }

                @Override
                public void onFailure(@Nonnull Throwable e) {
                    emitter.onError(AgentStoreException.cassandraDriverError(e));
                }
            });
        }, Emitter.BackpressureMode.NONE);
    }

    static <T> Observable<T> retrieve(Session session,
                                      ObjectMapper mapper,
                                      Statement fetchStatement,
                                      Class<T> type,
                                      EntitySanitizer entitySanitizer,
                                      boolean failOnError) {
        fetchStatement.setFetchSize(Integer.MAX_VALUE);
        return execute(session, fetchStatement)
                .flatMapIterable(resultSet -> {
                            List<Row> all = resultSet.all();
                            List<T> converted = new ArrayList<>();
                            all.forEach(row -> {
                                String text = row.getString(0);
                                try {
                                    T value = ObjectMappers.readValue(mapper, text, type);
                                    Set<ConstraintViolation<T>> violations = entitySanitizer.validate(value);
                                    if(violations.isEmpty()) {
                                        converted.add(value);
                                    } else {
                                        if(failOnError) {
                                            throw AgentStoreException.badData(value, violations);
                                        }
                                        logger.warn("Ignoring bad record of type {} due to validation constraint violations: record={}, violations={}", type, value, violations);
                                    }
                                } catch (Exception e) {
                                    if (failOnError) {
                                        throw e;
                                    }
                                    logger.warn("Ignoring bad record of type {}: {}", type, text, e);
                                }
                            });
                            return converted;
                        }
                );
    }

    static <T> Completable store(Session session,
                                 ObjectMapper mapper,
                                 PreparedStatement insertEntityStatement,
                                 String id,
                                 T entity) {
        return Observable.fromCallable((Callable<Statement>) () -> {
            String entityJsonString = ObjectMappers.writeValueAsString(mapper, entity);
            return insertEntityStatement.bind(id, entityJsonString);
        }).flatMap(
                statement -> StoreUtils.execute(session, statement)
        ).toCompletable();
    }

    static Completable remove(Session session, PreparedStatement deleteStatment, List<String> ids) {
        return execute(session, deleteStatment.bind(ids)).toCompletable();
    }

    static Completable remove(Session session, PreparedStatement deleteStatment, String id) {
        return execute(session, deleteStatment.bind(id)).toCompletable();
    }
}
