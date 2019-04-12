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

package com.netflix.titus.ext.cassandra.testkit.store;

import java.io.File;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.ext.cassandra.store.CassandraJobStore;
import com.netflix.titus.ext.cassandra.store.CassandraStoreConfiguration;
import com.netflix.titus.ext.cassandra.tool.snapshot.JobSnapshotLoader;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import static com.netflix.titus.ext.cassandra.tool.CassandraSchemas.JOB_ACTIVE_TABLES;

public class EmbeddedCassandraStoreFactory {

    private static final long STARTUP_TIMEOUT = 30_000;

    private static final String CASSANDRA_CONFIG = "embedded-cassandra.yaml";

    private static final String CASSANDRA_SCHEMA = "tables.cql";
    private static final String CASSANDRA_KEYSPACE = "titus_embedded";

    private static final CassandraStoreConfiguration CONFIGURATION = new CassandraStoreConfiguration() {
        @Override
        public String getV2KeySpace() {
            return "dev";
        }

        @Override
        public boolean isFailOnInconsistentCapacityGroupData() {
            return true;
        }

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

        @Override
        public boolean isTracingEnabled() {
            return false;
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
        return new CassandraJobStore(CONFIGURATION, job -> true, session, titusRuntime);
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
