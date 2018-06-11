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

package com.netflix.titus.ext.cassandra.tool;

import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.TruncateException;
import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.ext.cassandra.executor.AsyncCassandraExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class CassandraUtils {

    private static final Logger logger = LoggerFactory.getLogger(CassandraUtils.class);

    private static final int MAX_CONCURRENCY = 1000;

    public static final int PAGE_SIZE = 1000;
    public static final int SPLIT = 2;

    public static void truncateTable(CommandContext context, String table) {
        PreparedStatement truncateStatement = truncateTableInternal(context, table, 1);
        logger.info("Truncated table {}.{}", truncateStatement.getQueryKeyspace(), table);
    }

    private static PreparedStatement truncateTableInternal(CommandContext context, String table, int retryCount) {
        PreparedStatement truncateStatement = context.getTargetSession().prepare(
                "TRUNCATE \"" + table + "\""
        );
        try {
            context.getTargetCassandraExecutor().executeUpdate(truncateStatement.bind()).toBlocking().firstOrDefault(null);
        } catch (TruncateException e) {
            if (retryCount > 0 && e.getMessage().contains("Cannot achieve consistency level ALL")) {
                logger.warn("Retrying failed truncate operations for table {}. Cause: {}", table, e.getMessage());
                return truncateTableInternal(context, table, retryCount - 1);
            }

            // Check if table is empty
            logger.info("Cannot complete truncate operation. Checking if table is empty: {}", table);
            Pair<Object, Object> value = readTwoColumnTable(context.getTargetSession(), table).take(1).toBlocking().firstOrDefault(null);
            if (value != null) {
                throw new IllegalStateException("Could not truncate table: " + table);
            }
        }
        return truncateStatement;
    }

    public static Pair<String, String> resolveColumnNamesInTwoColumnTable(Session sourceSession, String table) {
        TableMetadata tableMetadata = sourceSession.getCluster().getMetadata()
                .getKeyspace(sourceSession.getLoggedKeyspace())
                .getTable(table);
        String primaryKey = tableMetadata.getPartitionKey().get(0).getName();
        List<String> valueColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .filter(c -> !c.equals(primaryKey))
                .collect(Collectors.toList());
        Preconditions.checkState(valueColumns.size() == 1, "Expected one non primary key column, and is: %s", valueColumns);
        String valueColumn = valueColumns.get(0);

        return Pair.of(primaryKey, valueColumn);
    }

    public static Observable<Pair<Object, Object>> readTwoColumnTable(Session sourceSession, String table) {
        Pair<String, String> columnNames = resolveColumnNamesInTwoColumnTable(sourceSession, table);
        String primaryKey = columnNames.getLeft();
        String valueColumn = columnNames.getRight();

        PreparedStatement queryAllStatement = sourceSession.prepare(
                String.format("SELECT * FROM %s WHERE token(%s) > :min AND token(%s) <= :max", table, primaryKey, primaryKey)
        );
        AsyncCassandraExecutor executor = new AsyncCassandraExecutor(sourceSession, PAGE_SIZE, SPLIT);
        return executor.rawRangeQuery2(primaryKey, valueColumn, queryAllStatement);
    }

    public static long writeIntoTwoColumnTable(Session targetSession, String table, Observable<Pair<Object, Object>> sourceData) {
        Pair<String, String> columnNames = resolveColumnNamesInTwoColumnTable(targetSession, table);
        String primaryKey = columnNames.getLeft();
        String valueColumn = columnNames.getRight();

        PreparedStatement insertStatement = targetSession.prepare(
                String.format("INSERT INTO %s (%s, %s) VALUES (?, ?)", table, primaryKey, valueColumn)
        );

        AsyncCassandraExecutor executor = new AsyncCassandraExecutor(targetSession, PAGE_SIZE, SPLIT);

        long recordCount = sourceData
                .flatMap(pair -> {
                    BoundStatement boundStatement = insertStatement.bind(pair.getLeft(), pair.getRight());
                    return executor
                            .executeUpdate(boundStatement)
                            .cast(Long.class)
                            .concatWith(Observable.just(1L));
                }, MAX_CONCURRENCY)
                .reduce(0L, (acc, v) -> acc + v)
                .toBlocking().firstOrDefault(null);

        return recordCount;
    }

    public static void copyTable(CommandContext context, String table) {
        long recordCount = writeIntoTwoColumnTable(
                context.getTargetSession(),
                table,
                readTwoColumnTable(context.getSourceSession(), table)
        );
        logger.info("Copied {} records from table {}.{} to {}.{}", recordCount,
                context.getSourceKeySpace(), table,
                context.getTargetKeySpace(), table
        );
    }
}
