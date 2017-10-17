/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.perf.load.job;


import java.util.Collections;

import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobState;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.testkit.client.TitusMasterClient;
import io.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import io.netflix.titus.testkit.perf.load.ExecutionContext;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.subjects.PublishSubject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractJobExecutorTest {

    private final TitusV2ModelGenerator generator = new TitusV2ModelGenerator();

    private final TitusJobSpec jobSpec = generator.newJobSpec(TitusJobType.batch, "myJob");
    private final String jobId = generator.newJobInfo(jobSpec).getId();
    private final PublishSubject<ActiveJobsMonitor.ActiveJobs> activeJobsSubject = PublishSubject.create();

    private final ExecutionContext context = mock(ExecutionContext.class);
    private final TitusMasterClient client = mock(TitusMasterClient.class);

    private final AbstractJobExecutor jobExecutor = new BatchJobExecutor(jobSpec, () -> activeJobsSubject, context);
    private final ExtTestSubscriber<JobChangeEvent> eventSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() throws Exception {
        when(context.getClient()).thenReturn(client);
        jobExecutor.updates().subscribe(eventSubscriber);
    }

    @Test
    public void testSingleTaskBatchJob() throws Exception {
        // Submit a job
        when(client.submitJob(any())).thenReturn(Observable.just("Titus-1"));

        ExtTestSubscriber<Void> submitSubscriber = new ExtTestSubscriber<>();
        jobExecutor.submitJob().subscribe(submitSubscriber);

        submitSubscriber.assertOnCompleted();

        assertThat(eventSubscriber.takeNext()).isEqualTo(JobChangeEvent.onJobSubmit(jobId));

        // Reconcile initial job create
        TitusJobInfo jobInfo = generator.scheduleJob(jobId);
        TaskInfo task0 = jobInfo.getTasks().get(0);

        when(client.findJob(jobId, true)).thenReturn(Observable.just(jobInfo));
        pushActiveJobUpdateWith(jobInfo);
        assertThat(eventSubscriber.takeNext()).isEqualTo(JobChangeEvent.onTaskCreate(jobId, task0));

        // Finish task, and the job
        generator.moveWorkerToState(jobId, task0.getId(), TitusTaskState.FINISHED);
        generator.moveJobToState(jobId, TitusJobState.FINISHED);
        jobInfo = generator.getJob(jobId);

        when(client.findJob(jobId, true)).thenReturn(Observable.just(jobInfo));
        pushActiveJobUpdateWith(jobInfo);
        assertThat(eventSubscriber.takeNext()).isEqualTo(JobChangeEvent.onTaskStateChange(jobId, task0.getId(), TitusTaskState.FINISHED));
        assertThat(eventSubscriber.takeNext()).isEqualTo(JobChangeEvent.onJobFinished(jobId, TitusJobState.FINISHED));
        assertThat(eventSubscriber.isUnsubscribed()).isTrue();
    }

    private void pushActiveJobUpdateWith(TitusJobInfo jobInfo) {
        activeJobsSubject.onNext(new ActiveJobsMonitor.ActiveJobs(System.currentTimeMillis(), Collections.singletonMap(jobInfo.getId(), jobInfo)));
    }
}