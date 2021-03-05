package com.netflix.titus.ext.jooqflyway.jobactivity.publisher;

import java.util.concurrent.TimeUnit;

import javax.xml.ws.WebServiceRef;

import com.google.common.base.Stopwatch;
import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.ext.jooqflyway.jobactivity.JooqContext;
import com.netflix.titus.ext.jooqflyway.jobactivity.JooqJobActivityConnectorComponent;
import com.netflix.titus.ext.jooqflyway.jobactivity.JooqTitusRuntime;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.jobactivity.JobActivityPublisherRecordUtils;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
        properties = {
                "spring.application.name=test",
        },
        classes = {
                JooqJobActivityPublisherStore.class,
                JooqJobActivityConnectorComponent.class,
                JooqTitusRuntime.class,
                TitusRuntime.class,
                EmptyLogStorageInfo.class,
        }
)

@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
public class JooqJobActivityPublisherStoreTest {

    private final static Logger logger = LoggerFactory.getLogger(JooqJobActivityPublisherStoreTest.class);

    private DataGenerator<Job<BatchJobExt>> batchJobsGenerator = JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor());
    private DataGenerator<BatchJobTask> batchTasksGenerator = JobGenerator.batchTasks(JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor()).getValue());

    @Autowired
    @Qualifier("producerDslContext")
    private DSLContext producerDslContext;

    @Autowired
    private JooqJobActivityPublisherStore publisher;

    @Autowired
    private TitusRuntime titusRuntime;

    @Autowired
    @Qualifier("producerJooqContext")
    private JooqContext producerJooqContext;

    @Before
    public void  setUp() {
        createJooqPublisherStore();
    }

    @After
    public void tearDown() {
        StepVerifier.create(publisher.clearStore())
                .verifyComplete();
    }

    @Test
    public void testPublishJobs() {
        int numJobs = 10;

        Stopwatch insertStopwatch = Stopwatch.createStarted();
        StepVerifier.create(publishJobs(numJobs))
                .verifyComplete();
        long insertMs = insertStopwatch.elapsed(TimeUnit.MILLISECONDS);
        logger.info("Inserting {} took {}ms, which is {}ms/record", numJobs, insertMs, insertMs/numJobs);

        StepVerifier.create(publisher.getSize())
                .expectNext(numJobs)
                .verifyComplete();
    }


    @Test
    public void testPublishTasks() {
        int numTasks = 10;

        StepVerifier.create(publishTasks(numTasks))
                .verifyComplete();

        StepVerifier.create(publisher.getSize())
                .expectNext(numTasks)
                .verifyComplete();
    }

    @Test
    public void testActivityTableScan() {
        StepVerifier.create(publishJobs(20)).verifyComplete();

        StepVerifier.create(publisher
                .getRecords())
                .thenConsumeWhile(jobActivityPublisherRecord -> {
                    if (jobActivityPublisherRecord.getRecordType() == JobActivityPublisherRecord.RecordType.JOB) {
                        try {
                            logger.info("Read back job {}", JobActivityPublisherRecordUtils.getJobFromRecord(jobActivityPublisherRecord));
                        } catch (InvalidProtocolBufferException e) {
                            return false;
                        }
                    } else if (jobActivityPublisherRecord.getRecordType() == JobActivityPublisherRecord.RecordType.TASK) {
                        try {
                            logger.info("Read back task {}", JobActivityPublisherRecordUtils.getTaskFromRecord(batchJobsGenerator.getValue(), jobActivityPublisherRecord));
                        } catch (InvalidProtocolBufferException e) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                    return true;
                })
                .verifyComplete();
    }

    @Test
    public void testQueueIndexLoad() {
        int numRecords = 20;

        // Insert some records
        StepVerifier.create(publishJobs(numRecords)).verifyComplete();

        // Create a new publisher that will reload the queue index
        createJooqPublisherStore();

        assertThat(publisher.getQueueIndex()).isEqualTo(numRecords);
    }

    private void createJooqPublisherStore() {
        publisher = new JooqJobActivityPublisherStore(producerDslContext, titusRuntime, EmptyLogStorageInfo.empty());
    }

    private Mono<Void> publishJobs(int count) {
        return observeJobs(count)
                .flatMap(batchJobExtJob -> publisher.publishJob(batchJobExtJob))
                .then();
    }

    private Mono<Void> publishTasks(int count) {
        return observeTasks(count)
                .flatMap(batchJobTask -> publisher.publishTask(batchJobTask))
                .then();
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
