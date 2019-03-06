/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.jobactivity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.master.jobactivity.service.JobActivityPublisher;
import com.netflix.titus.master.jobactivity.service.JobActivityPublisherConfiguration;
import com.netflix.titus.master.jobactivity.store.InMemoryJobActivityPublisherStore;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobActivityPublisherTest {
    private final static Logger logger = LoggerFactory.getLogger(JobActivityPublisherTest.class);

    private InMemoryJobActivityPublisherStore publisherStore;
    private final JobActivityPublisherConfiguration configuration = mock(JobActivityPublisherConfiguration.class);
    private final V3JobOperations v3JobOperations = mock(V3JobOperations.class);
    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private int numBatchJobs = 10;
    private DataGenerator<Job<BatchJobExt>> batchJobsGenerator = JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor());
    private List<JobUpdateEvent> jobUpdateEvents;

    private JobActivityPublisher publisherService;

    @Before
    public void setUp() {
        when(configuration.getJobActivityPublisherMaxStreamSize()).thenReturn(5000);

        publisherStore = new InMemoryJobActivityPublisherStore();
        jobUpdateEvents = createJobUpdateEvents(numBatchJobs);
        publisherService = new JobActivityPublisher(configuration,
                publisherStore, v3JobOperations, job -> true, titusRuntime);
    }

    // Tests that emitted V3 events are consumed and written to the publisher store
    @Test
    public void testPublishJobUpdateEvent() throws Exception {
        when(v3JobOperations.observeJobs()).thenReturn(Observable.from(jobUpdateEvents));

        // Start consumption of the event stream until it fully consumes it
        publisherService.activate();

        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(() -> !publisherService.isActive());

        // Verify that all of the emitted events were consumed and published
        StepVerifier.create(publisherStore.getSize())
                .expectNext(numBatchJobs)
                .verifyComplete();

        // Verify that published records are correct
        StepVerifier.create(publisherStore.getRecords())
                .thenConsumeWhile(jobActivityPublisherRecord ->
                        jobActivityPublisherRecord.getRecordType() == JobActivityPublisherRecord.RecordType.JOB)
                .verifyComplete();
    }

    // Tests that deactivation properly stops stream consumption
    @Test
    public void testDeactivate() throws Exception {
        when(v3JobOperations.observeJobs()).thenReturn(Observable.from(jobUpdateEvents));
        publisherService.activate();
        publisherService.deactivate();
        assertThat(publisherService.isActive()).isFalse();
    }

    private List<JobUpdateEvent> createJobUpdateEvents(int numEvents) {
        List<JobUpdateEvent> events = new ArrayList<>();
        batchJobsGenerator.batch(numEvents).getValue().forEach(batchJobExtJob ->
                events.add(JobUpdateEvent.newJob(batchJobExtJob))
        );
        return events;
    }
}
