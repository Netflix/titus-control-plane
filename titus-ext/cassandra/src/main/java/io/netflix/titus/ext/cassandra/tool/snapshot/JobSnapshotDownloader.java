package io.netflix.titus.ext.cassandra.tool.snapshot;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.ext.cassandra.tool.CassandraSchemas;
import io.netflix.titus.ext.cassandra.tool.CassandraUtils;
import rx.Observable;

/**
 * Downloads jobs active data from Cassandra database into set of files. A snapshot can be loaded back into
 * Cassandra using {@link JobSnapshotLoader}.
 */
public class JobSnapshotDownloader {

    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
    }

    private final Session session;
    private final boolean includeArchived;
    private final File outputFolder;

    public JobSnapshotDownloader(Session session, boolean includeArchived, File output) {
        this.session = session;
        this.includeArchived = includeArchived;
        Preconditions.checkArgument(!output.exists() || output.isDirectory(), "% exists and is not a directory", output);
        this.outputFolder = output;
    }

    public void download() {
        if (!outputFolder.exists()) {
            Preconditions.checkState(outputFolder.mkdirs(), "Cannot create output folder: %s", outputFolder.getAbsolutePath());
        }
        writeIdBuckets(CassandraSchemas.ACTIVE_JOB_IDS_TABLE);
        writeDataTable(CassandraSchemas.ACTIVE_JOBS_TABLE);
        writeIdMappingTable(CassandraSchemas.ACTIVE_TASK_IDS_TABLE);
        writeDataTable(CassandraSchemas.ACTIVE_TASKS_TABLE);

        if (includeArchived) {
            writeDataTable(CassandraSchemas.ARCHIVED_JOBS_TABLE);
            writeIdMappingTable(CassandraSchemas.ARCHIVED_TASK_IDS_TABLE);
            writeDataTable(CassandraSchemas.ARCHIVED_TASKS_TABLE);
        }
    }

    private void writeDataTable(String table) {
        File output = new File(outputFolder, table + ".json");

        List<JsonNode> allItems = CassandraUtils.readTwoColumnTable(session, table)
                .flatMap(p -> {
                    try {
                        return Observable.just(MAPPER.readTree((String) p.getRight()));
                    } catch (IOException e) {
                        return Observable.error(e);
                    }
                })
                .toList()
                .toBlocking().first();

        System.out.println(String.format("Writing %s rows from table %s to file: %s...", allItems.size(), table, output));
        try {
            MAPPER.writeValue(output, allItems);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A table holding buckets, where a key is bucket id of type int, and the value is a string.
     */
    private void writeIdBuckets(String table) {
        writeMapping(table, allItems -> {
            Map<Integer, List<String>> buckets = new HashMap<>();
            allItems.forEach(pair ->
                    buckets.computeIfAbsent((Integer) pair.getLeft(), b -> new ArrayList<>()).add((String) pair.getRight())
            );
            return buckets;
        });
    }

    /**
     * A table holding ids has two columns of string type. We encode these values as map entries in output JSON document.
     */
    private void writeIdMappingTable(String table) {
        writeMapping(table, allItems -> {
            Map<String, List<String>> jobTaskMap = new HashMap<>();
            allItems.forEach(pair ->
                    jobTaskMap.computeIfAbsent((String) pair.getLeft(), b -> new ArrayList<>()).add((String) pair.getRight())
            );
            return jobTaskMap;
        });
    }

    private void writeMapping(String table, Function<List<Pair<Object, Object>>, Map<?, ?>> mapper) {
        File output = new File(outputFolder, table + ".json");

        List<Pair<Object, Object>> allItems = CassandraUtils.readTwoColumnTable(session, table)
                .toList()
                .toBlocking().first();

        System.out.println(String.format("Writing %s rows from table %s to file: %s...", allItems.size(), table, output));

        try {
            MAPPER.writeValue(output, mapper.apply(allItems));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
