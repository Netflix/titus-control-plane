package com.netflix.titus.supplementary.jobactivity.store;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Stopwatch;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobactivity.store.JobActivityStoreException;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.jobactivity.JobActivityPublisherRecordUtils;
import com.netflix.titus.supplementary.jobactivity.JobActivityConnectorStubs;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
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

import static com.netflix.titus.supplementary.jobactivityhistory.generated.activity.Activity.ACTIVITY;
import static com.netflix.titus.supplementary.jobactivityhistory.generated.activity.tables.ActivityQueue.ACTIVITY_QUEUE;

import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
        properties = {
                "spring.application.name=test",
        },
        classes = {
                JooqJobActivityContextComponent.class,
        }
)

@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
public class JooqJobActivityStoreTest {
    private final static Logger logger = LoggerFactory.getLogger(JooqJobActivityStoreTest.class);

    private DataGenerator<Job<BatchJobExt>> batchJobsGenerator = JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor());
    private DataGenerator<BatchJobTask> batchTasksGenerator = JobGenerator.batchTasks(JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor()).getValue());

    private JooqJobActivityStore jooqJobActivityStore;
    public AtomicLong queueIndex = new AtomicLong(0);

    JobActivityConnectorStubs jobActivityConnectorStubs = new JobActivityConnectorStubs();

    private TitusRuntime titusRuntime = jobActivityConnectorStubs.getTitusRuntime();

    @Autowired
    @Qualifier("jobActivityJooqContext")
    private JooqContext jobActivityJooqContext;

    @Autowired
    @Qualifier("producerJooqContext")
    private JooqContext producerJooqContext;

    @Before
    public void setUp() {
        createJooqJobActivityStore();
    }

    @After
    public void shutdown() {
        producerJooqContext.getDslContext().deleteFrom(ACTIVITY.ACTIVITY_QUEUE).execute();
        //jobActivityJooqContext.getDslContext().dropTable(JOBACTIVITY.JOBS).execute();
        //jobActivityJooqContext.getDslContext().dropTable(JOBACTIVITY.TASKS).execute();
        jobActivityConnectorStubs.shutdown();
    }

    private void createJooqJobActivityStore() {
        jooqJobActivityStore = new JooqJobActivityStore(titusRuntime, jobActivityJooqContext, producerJooqContext, true);
    }

    public Mono<Void> publishJobs() {
        return observeJobs(10)
                .flatMap(batchJobExtJob -> publishJob(batchJobExtJob))
                .then();
    }

    public Mono<Void> publishJob(Job<?> job) {
        return publishByteString(JobActivityPublisherRecord.RecordType.JOB, job.getId(),
                JobActivityPublisherRecordUtils.jobToByteArry(job));
    }

    public Mono<Void> publishByteString(JobActivityPublisherRecord.RecordType recordType, String recordId, byte[] serializedRecord) {
        long assignedQueueIndex = queueIndex.getAndIncrement();
        DSLContext producerDslContext = producerJooqContext.getDslContext();
        return JooqUtils.executeAsyncMono(() -> {
            int numInserts = producerDslContext
                    .insertInto(ACTIVITY_QUEUE,
                            ACTIVITY_QUEUE.QUEUE_INDEX,
                            ACTIVITY_QUEUE.EVENT_TYPE,
                            ACTIVITY_QUEUE.SERIALIZED_EVENT)
                    .values(assignedQueueIndex,
                            (short) recordType.ordinal(),
                            serializedRecord)
                    .execute();
            return numInserts;
        }, producerDslContext)
                .onErrorMap(e -> {
                    System.out.println("FAIL");
                    return JobActivityStoreException.jobActivityUpdateRecordException(recordId, e);
                })
                .then();
    }

    @Test
    public void consumeRecord() {
        System.out.println("Running consumer");
        //publishJobs();
        StepVerifier.create(publishJobs()).verifyComplete();
        jooqJobActivityStore.consumeRecords();
        //StepVerifier.create(jooqJobActivityStore.readRecordFromPublisherQueue()).verifyComplete();
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
