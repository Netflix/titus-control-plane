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

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import io.netflix.titus.common.util.tuple.Pair;
import rx.Observable;

/**
 * RxJava enabled Cassandra statement executor for dynamic column family definitions (key/column1/value).
 */
public class AsyncCassandraExecutor {

    private final ExecutionContext context;

    private final UpdateOperations updateOperations;
    private final QueryOperations queryOperations;

    public AsyncCassandraExecutor(Session session, int pageSize, int split) {
        this.context = new ExecutionContext(session, pageSize, split);

        this.updateOperations = new UpdateOperations(context);
        this.queryOperations = new QueryOperations(context);
    }

    public Session getSession() {
        return context.getSession();
    }

    public Observable<Void> executeUpdate(BoundStatement statement) {
        return updateOperations.executeUpdate(statement);
    }

    /**
     * Returns map of { rowId -> { columnId -> entity }}. Single row may contain many columns, and each column value is
     * a JSON document, that is mapped to its corresponding type.
     */
    public Observable<Map<String, Map<String, Object>>> query(BoundStatement statement, Function<String, Class<?>> entityTypeResolver) {
        return queryOperations.executeQuery(statement, entityTypeResolver);
    }

    /**
     * Returns map of { rowId -> { columnId -> entity }}. Single row may contain many columns, and each column value is
     * a JSON document, that is mapped to its corresponding type.
     */
    public Observable<Map<String, Map<String, Object>>> rangeQuery(PreparedStatement statement) {
        return queryOperations.executeRangeQuery(statement, null);
    }

    /**
     * Returns map of { rowId -> { columnId -> entity }}. Single row may contain many columns, and each column value is
     * a JSON document, that is mapped to its corresponding type.
     */
    public Observable<Map<String, Map<String, Object>>> rangeQuery(PreparedStatement statement, Function<String, Class<?>> entityTypeResolver) {
        return queryOperations.executeRangeQuery(statement, entityTypeResolver);
    }

    /**
     * Reads { rowId, columnId, value } entries from Cassandra. The value is not deserialized.
     */
    @Deprecated
    public Observable<Cell> rawRangeQuery(PreparedStatement statement) {
        return queryOperations.executeRawRangeQuery(statement, null);
    }

    /**
     * Reads { primary_key, value } entries from Cassandra. The value is not deserialized.
     */
    public Observable<Pair<Object, Object>> rawRangeQuery2(String keyName, String valueName, PreparedStatement statement) {
        return queryOperations.executeRawRangeQuery2(keyName, valueName, statement, Optional.empty());
    }

    /**
     * Reads { rowId, columnId, value } entries from Cassandra.
     */
    public Observable<Cell> rawRangeQuery(PreparedStatement statement, Function<String, Class<?>> entityTypeResolver) {
        return queryOperations.executeRawRangeQuery(statement, entityTypeResolver);
    }

    /**
     * {@see #query}
     */
    public Map<String, Map<String, Object>> queryBlocking(BoundStatement statement, Function<String, Class<?>> entityTypeResolver) {
        return query(statement, entityTypeResolver).toBlocking().first();
    }

    public Observable<Map<String, Object>> rowQuery(BoundStatement statement, String rowId, Function<String, Class<?>> entityTypeResolver) {
        return queryOperations.executeRowQuery(statement, rowId, entityTypeResolver);
    }

    public Optional<Map<String, Object>> rowQueryBlocking(BoundStatement statement, String rowId, Function<String, Class<?>> entityTypeResolver) {
        return Optional.ofNullable(
                rowQuery(statement, rowId, entityTypeResolver).toBlocking().firstOrDefault(null)
        );
    }
}
