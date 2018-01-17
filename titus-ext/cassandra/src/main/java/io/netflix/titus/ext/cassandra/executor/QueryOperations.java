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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PagingIterable;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;
import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

class QueryOperations {

    private static final Logger logger = LoggerFactory.getLogger(QueryOperations.class);

    private final Session session;
    private final List<TokenRange> tokenRanges;
    private final int pageSize;

    public QueryOperations(ExecutionContext context) {
        this.session = context.getSession();
        this.tokenRanges = context.getTokenRanges();
        this.pageSize = context.getPageSize();
    }

    Observable<Map<String, Map<String, Object>>> executeQuery(BoundStatement statement, Function<String, Class<?>> entityTypeResolver) {
        return executeQueryInternal(statement.bind(), entityTypeResolver)
                .toList()
                .map(all -> {
                    Map<String, Map<String, Object>> result = new HashMap<>();
                    all.forEach(cell -> {
                        Map<String, Object> rowResult = result.computeIfAbsent(cell.getRowId(), k -> new HashMap<>());
                        rowResult.put(cell.getColumn(), cell.getValue());
                    });
                    return result;
                });
    }

    Map<String, Map<String, Object>> executeQueryBlocking(BoundStatement statement, Function<String, Class<?>> entityTypeResolver) {
        return executeQuery(statement, entityTypeResolver).toBlocking().first();
    }

    Observable<Map<String, Object>> executeRowQuery(BoundStatement statement, String rowId, Function<String, Class<?>> entityTypeResolver) {
        return executeQueryInternal(statement, entityTypeResolver)
                .toList()
                .map(all -> {
                    Map<String, Object> result = new HashMap<>();
                    all.forEach(cell -> {
                        if (!cell.getRowId().equals(rowId)) {
                            logger.warn("Read inconsistency requested - expected all columns for row {}, but found result for row {}", rowId, cell.getRowId());
                        } else {
                            result.put(cell.getColumn(), cell.getValue());
                        }
                    });
                    return result;
                });
    }

    Optional<Map<String, Object>> executeRowQueryBlocking(BoundStatement statement, String rowId, Function<String, Class<?>> entityTypeResolver) {
        return Optional.ofNullable(
                executeRowQuery(statement, rowId, entityTypeResolver).toBlocking().firstOrDefault(null)
        );
    }

    @Deprecated
    public Observable<Cell> executeRawRangeQuery(PreparedStatement statement, Function<String, Class<?>> entityTypeResolver) {
        List<Observable<Cell>> allQueries = tokenRanges.stream()
                .map(range -> statement.bind().setToken("min", range.getStart()).setToken("max", range.getEnd()))
                .map(boundStatement -> executeQueryInternal(boundStatement, entityTypeResolver).onBackpressureBuffer())
                .collect(Collectors.toList());
        return Observable.merge(allQueries);
    }

    public Observable<Pair<Object, Object>> executeRawRangeQuery2(String keyName, String valueName, PreparedStatement statement, Optional<Class<?>> entityType) {
        List<Observable<Pair<Object, Object>>> allQueries = tokenRanges.stream()
                .map(range -> statement.bind().setToken("min", range.getStart()).setToken("max", range.getEnd()))
                .map(boundStatement -> executeQueryInternal2(keyName, valueName, boundStatement, entityType).onBackpressureBuffer())
                .collect(Collectors.toList());
        return Observable.merge(allQueries);
    }

    public Observable<Map<String, Map<String, Object>>> executeRangeQuery(PreparedStatement statement, Function<String, Class<?>> entityTypeResolver) {
        return executeRawRangeQuery(statement, entityTypeResolver).toList()
                .map(all -> {
                    Map<String, Map<String, Object>> result = new HashMap<>();
                    all.forEach(cell -> {
                        Map<String, Object> rowResult = result.computeIfAbsent(cell.getRowId(), k -> new HashMap<>());
                        rowResult.put(cell.getColumn(), cell.getValue());
                    });
                    return result;
                });
    }

    private Observable<Cell> executeQueryInternal(BoundStatement boundStatement, Function<String, Class<?>> entityTypeResolver) {
        boundStatement.setFetchSize(pageSize);
        return FuturePaginatedQuery.paginatedQuery(
                () -> session.executeAsync(boundStatement),
                ResultSet::fetchMoreResults,
                (rs, total) -> {
                    int remaining = rs.getAvailableWithoutFetching();
                    List<Cell> pageItems = new ArrayList<>(remaining);
                    for (Row row : rs) {
                        String key = row.getString("key");
                        String column = row.getString("column1");
                        if (entityTypeResolver == null) {
                            String value = row.getString("value");
                            pageItems.add(new Cell(key, column, value));
                        } else {
                            Class<?> entityType = entityTypeResolver.apply(column);
                            if (entityType == null) {
                                logger.debug("Ignoring column {}", column);
                            } else {
                                Object value = row.get("value", entityType);
                                pageItems.add(new Cell(key, column, value));
                            }
                        }
                        if (--remaining == 0) {
                            break;
                        }
                    }

                    logger.debug("Got page with {} items (total {})", pageItems.size(), total + pageItems.size());
                    return pageItems;
                },
                PagingIterable::isFullyFetched
        );
    }

    private Observable<Pair<Object, Object>> executeQueryInternal2(String keyName, String valueName, BoundStatement boundStatement, Optional<Class<?>> type) {
        boundStatement.setFetchSize(pageSize);
        return FuturePaginatedQuery.paginatedQuery(
                () -> session.executeAsync(boundStatement),
                ResultSet::fetchMoreResults,
                (rs, total) -> {
                    int remaining = rs.getAvailableWithoutFetching();
                    List<Pair<Object, Object>> pageItems = new ArrayList<>(remaining);
                    for (Row row : rs) {
                        Object key = row.getObject(keyName);
                        if (!type.isPresent()) {
                            String value = row.getString(valueName);
                            pageItems.add(Pair.of(key, value));
                        } else {
                            Class<?> entityType = type.get();
                            Object value = row.get(valueName, entityType);
                            pageItems.add(Pair.of(key, value));
                        }
                        if (--remaining == 0) {
                            break;
                        }
                    }

                    logger.debug("Got page with {} items (total {})", pageItems.size(), total + pageItems.size());
                    return pageItems;
                },
                PagingIterable::isFullyFetched
        );
    }
}
