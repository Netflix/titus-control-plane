/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.ext.jooq.jobactivity;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherStore;
import com.netflix.titus.api.jobactivity.store.JobActivityStoreException;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.spectator.DatabaseMetrics;
import com.netflix.titus.ext.jooq.JooqConfiguration;
import com.netflix.titus.ext.jooq.JooqContext;
import com.netflix.titus.ext.jooq.JooqUtils;
import com.netflix.titus.ext.jooq.activity.Activity;
import com.netflix.titus.runtime.jobactivity.JobActivityPublisherRecordUtils;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.jooq.impl.DSL.max;

/**
 * Implementation of a {@link JobActivityPublisherStore} that persists records in a jOOQ SQL database.
 */
@Singleton
public class JooqJobActivityPublisherStore implements JobActivityPublisherStore {
    private static final Logger logger = LoggerFactory.getLogger(JooqJobActivityPublisherStore.class);

    private static final String JOOQ_METRICS_DATABASE_NAME = "JobActivityPublisher";

    private final JooqContext jooqContext;
    private final LogStorageInfo<Task> logStorageInfo;
    private final DSLContext dslContext;
    private final DatabaseMetrics databaseMetrics;

    /**
     * Tracks the current queue index (e.g., tail of the queue is the highest index value). This approach has
     * the following caveats:
     * 1) The index value keyspace may be sparse if some insert operations fail.
     * 2) We expect only a single producer/writer to the table.
     * 3) The index value can decrease between restarts (e.g., queue gets drained). This is OK as index
     * values can be reused and will reset to the highest value after restart.
     * 4) The value is interpreted as an unsigned long. Realistically it is not going to wrap. However, if we
     * wish to handle wrapping we could create a new column and begin inserting into the new column with index 0 again
     * so the prior and wrapped values never exist in the same space.
     */
    private AtomicLong queueIndex;

    @Inject
    public JooqJobActivityPublisherStore(JooqConfiguration configuration,
                                         JooqContext jooqContext,
                                         TitusRuntime runtime,
                                         LogStorageInfo<Task> logStorageInfo) {
        this.jooqContext = jooqContext;
        this.logStorageInfo = logStorageInfo;
        this.dslContext = jooqContext.getDslContext();
        this.databaseMetrics = new DatabaseMetrics(runtime.getRegistry(), "titus", JOOQ_METRICS_DATABASE_NAME);

        createSchemaIfNotExist(configuration.isCreateSchemaIfNotExist());

        queueIndex = new AtomicLong(getInitialQueueIndex());
        logger.info("Loaded initial job activity publisher queue index {}", queueIndex);
    }

    private void createSchemaIfNotExist(boolean createIfNotExists) {
        if (createIfNotExists) {
            logger.info("Creating/migrating JooqJobActivityPublisherStore DB schema...");
            Flyway flyway = Flyway.configure()
                    .schemas("activity")
                    .locations("classpath:db/migration/activity")
                    .dataSource(jooqContext.getDataSource())
                    .load();
            flyway.migrate();
        }
    }

    private long getInitialQueueIndex() {
        long startTimeMs = System.currentTimeMillis();
        Record1<Long> record = DSL.using(dslContext.configuration())
                .select(max(Activity.ACTIVITY.ACTIVITY_QUEUE.QUEUE_INDEX))
                .from(Activity.ACTIVITY.ACTIVITY_QUEUE)
                .fetchOne();
        databaseMetrics.registerSelectLatency(startTimeMs, Activity.ACTIVITY.ACTIVITY_QUEUE.getName(), Collections.emptyList());

        // No record is present, start index at 0
        if (null == record.value1()) {
            return 0;
        }
        // Otherwise start one higher than the current max
        return record.value1() + 1;
    }

    @VisibleForTesting
    public Mono<Void> clearStore() {
        return JooqUtils.executeAsyncMono(() -> dslContext.truncateTable(Activity.ACTIVITY.ACTIVITY_QUEUE).execute(), dslContext).then();
    }

    @VisibleForTesting
    public long getQueueIndex() {
        return queueIndex.get();
    }

    @Override
    public Mono<Void> publishJob(Job<?> job) {
        return publishByteString(JobActivityPublisherRecord.RecordType.JOB, job.getId(),
                JobActivityPublisherRecordUtils.jobToByteArry(job));
    }

    @Override
    public Mono<Void> publishTask(Task task) {
        return publishByteString(JobActivityPublisherRecord.RecordType.TASK, task.getId(),
                JobActivityPublisherRecordUtils.taskToByteArray(task, logStorageInfo));
    }

    private Mono<Void> publishByteString(JobActivityPublisherRecord.RecordType recordType, String recordId, byte[] serializedRecord) {
        long assignedQueueIndex = queueIndex.getAndIncrement();

        return JooqUtils.executeAsyncMono(() -> {
            long startTimeMs = System.currentTimeMillis();
            int numInserts = dslContext
                    .insertInto(Activity.ACTIVITY.ACTIVITY_QUEUE,
                            Activity.ACTIVITY.ACTIVITY_QUEUE.QUEUE_INDEX,
                            Activity.ACTIVITY.ACTIVITY_QUEUE.EVENT_TYPE,
                            Activity.ACTIVITY.ACTIVITY_QUEUE.SERIALIZED_EVENT)
                    .values(assignedQueueIndex,
                            (short) recordType.ordinal(),
                            serializedRecord)
                    .execute();
            databaseMetrics.registerInsertLatency(startTimeMs, 1, Activity.ACTIVITY.ACTIVITY_QUEUE.getName(), Collections.emptyList());
            return numInserts;
        }, dslContext)
                .onErrorMap(e -> JobActivityStoreException.jobActivityUpdateRecordException(recordId, e))
                .then();
    }

    @VisibleForTesting
    public Flux<JobActivityPublisherRecord> getRecords() {
        return JooqUtils.executeAsyncMono(() -> {
            long startTimeMs = System.currentTimeMillis();
            List<JobActivityPublisherRecord> records = dslContext
                    .selectFrom(Activity.ACTIVITY.ACTIVITY_QUEUE)
                    .orderBy(Activity.ACTIVITY.ACTIVITY_QUEUE.QUEUE_INDEX)
                    .fetchInto(JobActivityPublisherRecord.class);
            databaseMetrics.registerScanLatency(startTimeMs, Activity.ACTIVITY.ACTIVITY_QUEUE.getName(), Collections.emptyList());
            return records;
        }, dslContext)
                .flatMapIterable(jobActivityPublisherRecords -> jobActivityPublisherRecords);
    }

    @VisibleForTesting
    public Mono<Integer> getSize() {
        return JooqUtils.executeAsyncMono(() -> dslContext.fetchCount(Activity.ACTIVITY.ACTIVITY_QUEUE), dslContext);
    }
}
