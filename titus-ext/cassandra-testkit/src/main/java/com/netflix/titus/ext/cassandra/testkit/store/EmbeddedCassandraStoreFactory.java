package com.netflix.titus.ext.cassandra.testkit.store;

import java.io.File;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.runtime.TitusRuntimes;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.ext.cassandra.store.CassandraJobStore;
import io.netflix.titus.ext.cassandra.store.CassandraStoreConfiguration;
import io.netflix.titus.ext.cassandra.tool.snapshot.JobSnapshotLoader;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import static io.netflix.titus.ext.cassandra.tool.CassandraSchemas.JOB_ACTIVE_TABLES;

public class EmbeddedCassandraStoreFactory {

    private static final long STARTUP_TIMEOUT = 30_000;

    private static final String CASSANDRA_CONFIG = "embedded-cassandra.yaml";

    private static final String CASSANDRA_SCHEMA = "tables.cql";
    private static final String CASSANDRA_KEYSPACE = "titus_embedded";

    private static final CassandraStoreConfiguration CONFIGURATION = new CassandraStoreConfiguration() {
        @Override
        public boolean isFailOnInconsistentAgentData() {
            return true;
        }

        @Override
        public boolean isFailOnInconsistentLoadBalancerData() {
            return false;
        }

        @Override
        public boolean isFailOnInconsistentSchedulerData() {
            return false;
        }

        @Override
        public int getConcurrencyLimit() {
            return 10;
        }
    };

    private final Session session;
    private final TitusRuntime titusRuntime;

    public EmbeddedCassandraStoreFactory(Session session, TitusRuntime titusRuntime) {
        this.session = session;
        this.titusRuntime = titusRuntime;
    }

    public void shutdown() {
        session.close();
    }

    public JobStore getJobStore() {
        return new CassandraJobStore(CONFIGURATION, session, titusRuntime);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private File jobInputFolder;
        private TitusRuntime titusRuntime;

        public Builder withTitusRuntime(TitusRuntime titusRuntime) {
            this.titusRuntime = titusRuntime;
            return this;
        }

        public Builder withJobStoreFiles(File folder) {
            Preconditions.checkArgument(folder.exists(), "%s not found", folder);
            Preconditions.checkArgument(folder.isDirectory(), "%s is not directory", folder);

            Set<String> expectedFiles = JOB_ACTIVE_TABLES.stream().map(t -> t + ".json").collect(Collectors.toSet());
            Set<String> foundFiles = expectedFiles.stream().filter(f -> new File(folder, f).exists()).collect(Collectors.toSet());
            if (foundFiles.size() != JOB_ACTIVE_TABLES.size()) {
                Set<String> missingFiles = CollectionsExt.copyAndRemove(expectedFiles, foundFiles);
                throw new IllegalArgumentException("Incomplete set of job files. Missing files: " + missingFiles);
            }

            this.jobInputFolder = folder;

            return this;
        }

        public EmbeddedCassandraStoreFactory build() {
            Session session = createEmbeddedCassandra();
            if (jobInputFolder != null) {
                loadJobStore(session);
            }
            if (titusRuntime == null) {
                titusRuntime = TitusRuntimes.internal();
            }
            return new EmbeddedCassandraStoreFactory(session, titusRuntime);
        }

        private void loadJobStore(Session session) {
            new JobSnapshotLoader(session, jobInputFolder).load();
        }

        private Session createEmbeddedCassandra() {
            // Disable fsync for a massive speedup on old platters. This improves boot time by a few seconds.
            System.setProperty("cassandra.unsafesystem", "true");

            try {
                File cassandraTmpDir = Files.createTempDir();
                EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_CONFIG, cassandraTmpDir.getAbsolutePath(), STARTUP_TIMEOUT);
            } catch (Exception e) {
                throw new IllegalStateException("Cannot initialize the embedded Cassandra", e);
            }

            Session session = EmbeddedCassandraServerHelper.getSession();
            session.execute("CREATE KEYSPACE " + CASSANDRA_KEYSPACE + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
            session.execute("USE " + CASSANDRA_KEYSPACE);

            CQLDataLoader dataLoader = new CQLDataLoader(session);
            dataLoader.load(new ClassPathCQLDataSet(CASSANDRA_SCHEMA, "Titus"));

            return session;
        }
    }
}
