/*
 *
 *  * Copyright 2019 Netflix, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netflix.titus.ext.jooq.jobactivity;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobactivity.service.JobActivityException;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherStore;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.ext.jooq.activity.schema.JActivity;
import com.netflix.titus.runtime.jobactivity.JobActivityPublisherRecordUtils;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.netflix.titus.ext.jooq.activity.schema.tables.JActivityQueue.ACTIVITY_QUEUE;
import static org.jooq.impl.DSL.max;

/**
 * Implementation of a {@link JobActivityPublisherStore} that persists records in a jOOQ SQL database.
 */
@Singleton
public class JooqJobActivityPublisherStore implements JobActivityPublisherStore {
    private static final Logger logger = LoggerFactory.getLogger(JooqJobActivityPublisherStore.class);

    private final DSLContext dslContext;

    /**
     * Tracks the current queue index (e.g., head is the lowest index value). This approach has the following caveats:
     * 1) The keyspace may be sparse if we some insert operations fail.
     * 2) We expect only a single producer/writer to the table.
     * 3) The value is interpreted as an unsigned long. Realistically it is not going to wrap. However, if we
     * wish to handle wrapping we could create a new column and begin inserting into the new column with index 0 again
     * so the prior and wrapped values never exist in the same space.
     */
    private AtomicLong queueIndex;

    @Inject
    public JooqJobActivityPublisherStore(DSLContext dslContext) {
        this(dslContext, true);
    }

    @VisibleForTesting
    public JooqJobActivityPublisherStore(DSLContext dslContext, boolean createIfNotExist) {
        this.dslContext = dslContext;

        if (createIfNotExist) {
            createSchemaIfNotExist();
        }
        queueIndex = new AtomicLong(getInitialQueueIndex());
        logger.info("Loaded initial job activity publisher queue index {}", queueIndex);
    }

    private void createSchemaIfNotExist() {
        dslContext.createSchemaIfNotExists(JActivity.ACTIVITY)
                .execute();

        int rc = dslContext.createTableIfNotExists(ACTIVITY_QUEUE)
                .column(ACTIVITY_QUEUE.QUEUE_INDEX)
                .column(ACTIVITY_QUEUE.EVENT_TYPE)
                .column(ACTIVITY_QUEUE.SERIALIZED_EVENT)
                .constraint(DSL.constraint("pk_activity_queue_index").primaryKey(ACTIVITY_QUEUE.QUEUE_INDEX))
                .execute();
        if (0 != rc) {
            throw JobActivityException.jobActivityCreateTableException(
                    ACTIVITY_QUEUE.getName(),
                    new RuntimeException(String.format("Unexpected table create return code %d", rc)));
        }
        logger.info("Created schema and table with return code {}", rc);
    }

    private long getInitialQueueIndex() {
        Record1<Long> record = DSL.using(dslContext.configuration())
                .select(max(ACTIVITY_QUEUE.QUEUE_INDEX))
                .from(ACTIVITY_QUEUE)
                .fetchOne();

        // No record is present, start index at 0
        if (null == record.value1()) {
            return 0;
        }
        // Otherwise start one higher than the current max
        return record.value1() + 1;
    }

    @VisibleForTesting
    public Mono<Void> clearStore() {
        return Mono.defer(() -> {
            try {
                int recordId = dslContext
                        .dropTable(ACTIVITY_QUEUE)
                        .execute();
                logger.warn("Successfully DROP'ed table {}", ACTIVITY_QUEUE.getName(), recordId);
            } catch (Exception e) {
                return Mono.error(e);
            }
            return Mono.empty();
        });
    }

    @VisibleForTesting
    public long getQueueIndex() {
        return queueIndex.get();
    }

    @Override
    public Mono<Void> publishJob(Job<?> job) {
        return Mono.defer(() ->
                publishByteString(JobActivityPublisherRecord.RecordType.JOB, job.getId(), JobActivityPublisherRecordUtils.jobToByteArry(job)));
    }

    @Override
    public Mono<Void> publishTask(Task task) {
        return Mono.defer(() ->
                publishByteString(JobActivityPublisherRecord.RecordType.TASK, task.getId(), JobActivityPublisherRecordUtils.taskToByteArray(task)));
    }

    private Mono<Void> publishByteString(JobActivityPublisherRecord.RecordType recordType, String recordId, byte[] serializedRecord) {
        long assignedQueueIndex = queueIndex.getAndIncrement();

        try {
            int numRecordsInserted = DSL.using(dslContext.configuration())
                    .insertInto(ACTIVITY_QUEUE,
                            ACTIVITY_QUEUE.QUEUE_INDEX,
                            ACTIVITY_QUEUE.EVENT_TYPE,
                            ACTIVITY_QUEUE.SERIALIZED_EVENT)
                    .values(assignedQueueIndex,
                            (short) recordType.ordinal(),
                            serializedRecord)
                    .execute();
            if (1 != numRecordsInserted) {
                return Mono.error(
                        JobActivityException.jobActivityUpdateRecordException(recordId,
                                new RuntimeException(String.format("Unexpected number of updated records: expected %d but got %d", 1, numRecordsInserted))));
            }
        } catch (Exception e) {
            return Mono.error(JobActivityException.jobActivityUpdateRecordException(recordId, e));
        }
        return Mono.empty();
    }

    @Override
    public Flux<JobActivityPublisherRecord> getRecords() {
        return Mono.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> DSL.using(dslContext.configuration())
                        .selectFrom(ACTIVITY_QUEUE)
                        .orderBy(ACTIVITY_QUEUE.QUEUE_INDEX)
                        .fetchInto(JobActivityPublisherRecord.class))
        ).flatMapIterable(jobActivityPublisherRecords -> jobActivityPublisherRecords);
    }

    @Override
    public Mono<Integer> getSize() {
        return Mono.just(DSL.using(dslContext.configuration())
                .fetchCount(ACTIVITY_QUEUE));
    }
}
