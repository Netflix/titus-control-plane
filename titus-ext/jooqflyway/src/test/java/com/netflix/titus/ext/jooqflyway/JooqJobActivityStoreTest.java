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

package com.netflix.titus.ext.jooqflyway;

import java.util.concurrent.atomic.AtomicLong;

import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobactivity.store.JobActivityStoreException;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.ext.jooqflyway.JobActivityConnectorStubs;
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

import static com.netflix.titus.supplementary.jobactivity.activity.tables.ActivityQueue.ACTIVITY_QUEUE;
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
        //producerJooqContext.getDslContext().deleteFrom(ACTIVITY.ACTIVITY_QUEUE).execute();
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
        int sizeBefore = producerJooqContext.getDslContext()
                .selectCount()
                .from(ACTIVITY_QUEUE)
                .fetchOneInto(Integer.class);
        //publishJobs();
        //JobActivityPublisherRecord tempRecord = new JobActivityPublisherRecord(9 , (short) 1, null);
        StepVerifier
                .create(jooqJobActivityStore.processRecords())
                .verifyComplete();
        int sizeAfter = producerJooqContext.getDslContext()
                .selectCount()
                .from(ACTIVITY_QUEUE)
                .fetchOneInto(Integer.class);
        System.out.println("Before: " + sizeBefore + " After: " + sizeAfter);
        //jooqJobActivityStore.consumeRecords();


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
