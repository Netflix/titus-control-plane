package com.netflix.titus.ext.jooqflyway.jobactivity;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobactivity.store.JobActivityStoreException;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.ext.jooqflyway.generated.activity.Activity;
import com.netflix.titus.ext.jooqflyway.jobactivity.publisher.JooqJobActivityPublisherStore;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.jobactivity.JobActivityPublisherRecordUtils;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static com.netflix.titus.ext.jooqflyway.generated.activity.tables.ActivityQueue.ACTIVITY_QUEUE;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
        properties = {
                "spring.application.name=test",
        },
        classes = {
                JooqJobActivityConsumerStore.class,
                JooqJobActivityConnectorComponent.class,
                JooqTitusRuntime.class,
                TitusRuntime.class,
                EmptyLogStorageInfo.class,
        }
)

@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
public class JooqJobActivityConsumerStoreTest {
    private final static Logger logger = LoggerFactory.getLogger(JooqJobActivityConsumerStoreTest.class);

    private DataGenerator<Job<BatchJobExt>> batchJobsGenerator = JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor());
    private DataGenerator<BatchJobTask> batchTasksGenerator = JobGenerator.batchTasks(JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor()).getValue());

    private JooqJobActivityConsumerStore consumer;
    public long queueIndex = 0;

    @Autowired
    @Qualifier("jobActivityDslContext")
    private DSLContext jobActivityDslContext;

    @Autowired
    @Qualifier("producerDslContext")
    private DSLContext producerDslContext;

    @Autowired
    TitusRuntime titusRuntime;

    @Before
    public void setUp() {
        createJooqConsumerStore();
        publishJobs();
    }

    private void createJooqConsumerStore() {
        consumer = new JooqJobActivityConsumerStore(titusRuntime, jobActivityDslContext, producerDslContext);
    }

    public void publishJobs() {
        producerDslContext.createSchemaIfNotExists(Activity.ACTIVITY)
                .execute();

        int rc = producerDslContext.createTableIfNotExists(ACTIVITY_QUEUE)
                .column(ACTIVITY_QUEUE.QUEUE_INDEX)
                .column(ACTIVITY_QUEUE.EVENT_TYPE)
                .column(ACTIVITY_QUEUE.SERIALIZED_EVENT)
                .constraint(DSL.constraint("pk_activity_queue_index").primaryKey(ACTIVITY_QUEUE.QUEUE_INDEX))
                .execute();
        if (0 != rc) {
            throw new RuntimeException(String.format("Unexpected table create return code %d", rc));
        }
        observeJobs(10)
                .flatMap(batchJobExtJob -> publishJob(batchJobExtJob))
                .then();
    }

    public Mono<Void> publishJob(Job<?> job) {
        return publishByteString(JobActivityPublisherRecord.RecordType.JOB, job.getId(),
                JobActivityPublisherRecordUtils.jobToByteArry(job));
    }

    public Mono<Void> publishByteString(JobActivityPublisherRecord.RecordType recordType, String recordId, byte[] serializedRecord) {
        long assignedQueueIndex = queueIndex + 1;
        return JooqUtils.executeAsyncMono(() -> {
            long startTimeMs = System.currentTimeMillis();
            int numInserts = producerDslContext
                    .insertInto(ACTIVITY_QUEUE,
                            ACTIVITY_QUEUE.QUEUE_INDEX,
                            ACTIVITY_QUEUE.EVENT_TYPE,
                            ACTIVITY_QUEUE.SERIALIZED_EVENT)
                    .values(queueIndex,
                            (short) recordType.ordinal(),
                            serializedRecord)
                    .execute();
            return numInserts;
        }, producerDslContext)
                .onErrorMap(e -> JobActivityStoreException.jobActivityUpdateRecordException(recordId, e))
                .then();
    }

    @Test
    public void consumeRecord() {
        consumer.consumeRecord();
        return;
    }

    /**
     * Produces a Flux stream a of batch jobs based on the provided count.
     */
    private Flux<Job<BatchJobExt>> observeJobs(int count) {
        return Flux.fromIterable(batchJobsGenerator.batch(count).getValue());
    }

    private Flux<BatchJobTask> observeTasks(int count) {
        return Flux.fromIterable(batchTasksGenerator.batch(count).getValue());
    }

}
