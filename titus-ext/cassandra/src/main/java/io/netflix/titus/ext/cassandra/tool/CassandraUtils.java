package io.netflix.titus.ext.cassandra.tool;

import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Preconditions;
import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class CassandraUtils {

    private static final Logger logger = LoggerFactory.getLogger(CassandraUtils.class);

    private static final int MAX_CONCURRENCY = 1000;

    public static void truncateTable(CommandContext context, String table) {
        PreparedStatement truncateStatement = context.getTargetSession().prepare(
                "TRUNCATE \"" + table + "\""
        );
        context.getTargetCassandraExecutor().executeUpdate(truncateStatement.bind()).toBlocking().firstOrDefault(null);
        logger.info("Truncated table {}.{}", truncateStatement.getQueryKeyspace(), table);
    }

    public static void copyTable(CommandContext context, String table) {
        TableMetadata tableMetadata = context.getSourceSession().getCluster().getMetadata()
                .getKeyspace(context.getSourceSession().getLoggedKeyspace())
                .getTable(table);
        String primaryKey = tableMetadata.getPartitionKey().get(0).getName();
        List<String> valueColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .filter(c -> !c.equals(primaryKey))
                .collect(Collectors.toList());
        Preconditions.checkState(valueColumns.size() == 1, "Expected one non primary key column, and is: %s", valueColumns);
        String valueColumn = valueColumns.get(0);

        PreparedStatement queryAllStatement = context.getSourceSession().prepare(
                String.format("SELECT * FROM %s WHERE token(%s) > :min AND token(%s) <= :max", table, primaryKey, primaryKey)
        );
        PreparedStatement insertStatement = context.getTargetSession().prepare(
                String.format("INSERT INTO %s (%s, %s) VALUES (?, ?)", table, primaryKey, valueColumn)
        );

        Observable<Pair<Object, Object>> sourceData = context.getSourceCassandraExecutor().rawRangeQuery2(primaryKey, valueColumn, queryAllStatement);
        long recordCount = sourceData
                .flatMap(pair -> {
                    BoundStatement boundStatement = insertStatement.bind(pair.getLeft(), pair.getRight());
                    return context.getTargetCassandraExecutor()
                            .executeUpdate(boundStatement)
                            .cast(Long.class)
                            .concatWith(Observable.just(1L));
                }, MAX_CONCURRENCY)
                .reduce(0L, (acc, v) -> acc + v)
                .toBlocking().firstOrDefault(null);
        logger.info("Copied {} records from table {}.{} to {}.{}", recordCount,
                queryAllStatement.getQueryKeyspace(), table,
                insertStatement.getQueryKeyspace(), table
        );
    }
}
