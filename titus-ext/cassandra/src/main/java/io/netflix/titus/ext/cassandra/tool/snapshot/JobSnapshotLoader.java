package io.netflix.titus.ext.cassandra.tool.snapshot;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.ext.cassandra.tool.CassandraSchemas;
import io.netflix.titus.ext.cassandra.tool.CassandraUtils;
import rx.Observable;

/**
 * Loads jobs active data from files into Cassandra database. A snapshot can be created using {@link JobSnapshotDownloader}.
 */
public class JobSnapshotLoader {

    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
    }

    private final Session session;
    private final File inputFolder;

    public JobSnapshotLoader(Session session, File inputFolder) {
        checkAllFilesExist(inputFolder);
        this.session = session;
        this.inputFolder = inputFolder;
    }

    private void checkAllFilesExist(File inputFolder) {
        CassandraSchemas.JOB_ACTIVE_TABLES.forEach(table -> {
            File input = new File(inputFolder, table + ".json");
            Preconditions.checkArgument(input.isFile(), "File not found: %s", input);
        });
    }

    public void load() {
        readIdBuckets(CassandraSchemas.ACTIVE_JOB_IDS_TABLE);
        readDataTable(CassandraSchemas.ACTIVE_JOBS_TABLE);
        readIdMappingTable(CassandraSchemas.ACTIVE_TASK_IDS_TABLE);
        readDataTable(CassandraSchemas.ACTIVE_TASKS_TABLE);
    }

    private void readDataTable(String table) {
        ArrayNode jsonTree = (ArrayNode) readJsonTree(table);
        List<Pair<Object, Object>> items = new ArrayList<>();
        jsonTree.forEach(item -> {
            try {
                items.add(Pair.of(item.get("id").textValue(), MAPPER.writeValueAsString(item)));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        });
        long written = CassandraUtils.writeIntoTwoColumnTable(session, table, Observable.from(items));
        System.out.println(String.format("Successfully writen %s entries into table %s", written, table));
    }

    /**
     * A table holding buckets, where a key is bucket id of type int, and the value is a string.
     */
    private void readIdBuckets(String table) {
        ObjectNode jsonTree = (ObjectNode) readJsonTree(table);
        List<Pair<Object, Object>> items = new ArrayList<>();
        jsonTree.fieldNames().forEachRemaining(key -> {
            ArrayNode values = (ArrayNode) jsonTree.get(key);
            int bucketId = Integer.parseInt(key);
            values.forEach(value -> {
                items.add(Pair.of(bucketId, value.asText()));
            });
        });
        long written = CassandraUtils.writeIntoTwoColumnTable(session, table, Observable.from(items));
        System.out.println(String.format("Successfully writen %s entries into table %s", written, table));
    }

    /**
     * A table holding ids has two columns of string type. We encode these values as map entries in output JSON document.
     */
    private void readIdMappingTable(String table) {
        ObjectNode jsonTree = (ObjectNode) readJsonTree(table);
        List<Pair<Object, Object>> items = new ArrayList<>();
        jsonTree.fieldNames().forEachRemaining(key -> {
            ArrayNode values = (ArrayNode) jsonTree.get(key);
            values.forEach(value -> items.add(Pair.of(key, value.asText())));
        });
        long written = CassandraUtils.writeIntoTwoColumnTable(session, table, Observable.from(items));
        System.out.println(String.format("Successfully writen %s entries into table %s", written, table));
    }

    private JsonNode readJsonTree(String table) {
        File input = new File(inputFolder, table + ".json");
        try {
            JsonNode jsonTree = MAPPER.readTree(input);
            System.out.println(String.format("Loading %s rows into table %s to file: %s...", jsonTree.size(), table, input));
            return jsonTree;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
