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

package com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.NotificationCase;
import com.netflix.titus.master.integration.v3.job.CellAssertions;
import com.netflix.titus.api.model.event.JobStateChangeEvent;
import com.netflix.titus.api.model.event.TaskStateChangeEvent;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.model.v2.WorkerNaming;
import com.netflix.titus.api.model.v2.parameter.Parameters;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.util.rx.eventbus.RxEventBus;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.ApiOperations;
import com.netflix.titus.master.ConfigurationMockSamples;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.job.BaseJobMgr;
import com.netflix.titus.master.job.V2JobOperations;
import com.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import com.netflix.titus.master.store.V2JobMetadataWritable;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import com.netflix.titus.testkit.model.runtime.RuntimeModelGenerator;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.subjects.PublishSubject;

import static com.netflix.titus.master.integration.v3.job.CellAssertions.assertCellInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class V2GrpcTitusServiceGatewayTest {

    private final MasterConfiguration configuration = ConfigurationMockSamples.withExecutionEnvironment(mock(MasterConfiguration.class));

    private final V2JobOperations jobOperations = mock(V2JobOperations.class);

    private final JobSubmitLimiter jobSubmitLimiter = mock(JobSubmitLimiter.class);

    private final ApiOperations apiOperations = mock(ApiOperations.class);

    private final RxEventBus eventBus = mock(RxEventBus.class);

    private final LogStorageInfo<V2WorkerMetadata> logStorageInfo = mock(LogStorageInfo.class);

    private final EntitySanitizer entitySanitizer = mock(EntitySanitizer.class);

    private final V2GrpcTitusServiceGateway gateway = new V2GrpcTitusServiceGateway(configuration, jobOperations, jobSubmitLimiter, apiOperations, eventBus, logStorageInfo, entitySanitizer);

    private final String cellName = UUID.randomUUID().toString();
    private final RuntimeModelGenerator generator = new RuntimeModelGenerator(cellName);

    private final PublishSubject<Object> eventSubject = PublishSubject.create();

    @Before
    public void setUp() throws Exception {
        when(eventBus.listen(anyString(), any())).thenReturn(eventSubject);
        when(logStorageInfo.getLinks(any())).thenReturn(new LogStorageInfo.LogLinks(Optional.empty(), Optional.empty(), Optional.empty()));
        when(entitySanitizer.sanitize(any())).thenReturn(Optional.empty());
        when(entitySanitizer.validate(any())).thenReturn(Collections.emptySet());
    }

    @Test
    public void testObserveJob() throws Exception {
        V2JobMetadataWritable jobMetadata = (V2JobMetadataWritable) createJobManager("myJob").getJobMetadata();

        ExtTestSubscriber<JobChangeNotification> testSubscriber = new ExtTestSubscriber<>();
        gateway.observeJob(jobMetadata.getJobId()).subscribe(testSubscriber);

        // First is always snapshot
        JobChangeNotification firstNotification = testSubscriber.takeNext();
        assertThat(firstNotification.getNotificationCase()).isEqualByComparingTo(NotificationCase.JOBUPDATE);
        CellAssertions.assertCellInfo(firstNotification.getJobUpdate().getJob().getJobDescriptor(), cellName);
        assertThat(testSubscriber.takeNext().getNotificationCase()).isEqualByComparingTo(NotificationCase.SNAPSHOTEND);

        // Job size change
        eventSubject.onNext(new JobStateChangeEvent<V2JobMetadata>(jobMetadata.getJobId(), toJobState(jobMetadata.getState()), System.currentTimeMillis(), jobMetadata));
        assertThat(testSubscriber.takeNext().getNotificationCase()).isEqualByComparingTo(NotificationCase.JOBUPDATE);

        // Emit worker start event
        V2WorkerMetadata task = createTask(jobMetadata);
        String taskId = WorkerNaming.getWorkerName(jobMetadata.getJobId(), task.getWorkerIndex(), task.getWorkerNumber());
        eventSubject.onNext(new TaskStateChangeEvent<>(jobMetadata.getJobId(), taskId, task.getState(), System.currentTimeMillis(), Pair.of(jobMetadata, task)));
        JobChangeNotification taskNotification = testSubscriber.takeNext();
        assertThat(taskNotification.getNotificationCase()).isEqualByComparingTo(NotificationCase.TASKUPDATE);
        CellAssertions.assertCellInfo(taskNotification.getTaskUpdate().getTask(), cellName);

        // Terminate job
        jobMetadata.setJobState(V2JobState.Completed);
        eventSubject.onNext(new JobStateChangeEvent<V2JobMetadata>(jobMetadata.getJobId(), toJobState(jobMetadata.getState()), System.currentTimeMillis(), jobMetadata));
        assertThat(testSubscriber.takeNext().getNotificationCase()).isEqualByComparingTo(NotificationCase.JOBUPDATE);
        testSubscriber.assertOnCompleted();
    }

    @Test
    public void testObserveJobs() throws Exception {
        BaseJobMgr jobManager1 = createJobManager("myJob1");
        V2JobMetadataWritable jobMetadata1 = (V2JobMetadataWritable) jobManager1.getJobMetadata();
        BaseJobMgr jobManager2 = createJobManager("myJob2");
        when(jobOperations.getAllJobMgrs()).thenReturn(Arrays.asList(jobManager1, jobManager2));

        ExtTestSubscriber<JobChangeNotification> testSubscriber = new ExtTestSubscriber<>();
        gateway.observeJobs().subscribe(testSubscriber);

        // First are snapshots
        JobChangeNotification firstNotification = testSubscriber.takeNext();
        assertThat(firstNotification.getNotificationCase()).isEqualByComparingTo(NotificationCase.JOBUPDATE);
        CellAssertions.assertCellInfo(firstNotification.getJobUpdate().getJob().getJobDescriptor(), cellName);
        JobChangeNotification secondNotification = testSubscriber.takeNext();
        assertThat(secondNotification.getNotificationCase()).isEqualByComparingTo(NotificationCase.JOBUPDATE);
        CellAssertions.assertCellInfo(secondNotification.getJobUpdate().getJob().getJobDescriptor(), cellName);
        assertThat(testSubscriber.takeNext().getNotificationCase()).isEqualByComparingTo(NotificationCase.SNAPSHOTEND);

        // Job 1 size change
        eventSubject.onNext(new JobStateChangeEvent<V2JobMetadata>(jobMetadata1.getJobId(), toJobState(jobMetadata1.getState()), System.currentTimeMillis(), jobMetadata1));
        assertThat(testSubscriber.takeNext().getNotificationCase()).isEqualByComparingTo(NotificationCase.JOBUPDATE);

        // Terminate job 1
        jobMetadata1.setJobState(V2JobState.Completed);
        eventSubject.onNext(new JobStateChangeEvent<V2JobMetadata>(jobMetadata1.getJobId(), toJobState(jobMetadata1.getState()), System.currentTimeMillis(), jobMetadata1));
        assertThat(testSubscriber.takeNext().getNotificationCase()).isEqualByComparingTo(NotificationCase.JOBUPDATE);

        assertThat(testSubscriber.isUnsubscribed()).isFalse();

        // Emit job 3
        BaseJobMgr jobManager3 = createJobManager("myJob3");
        V2JobMetadataWritable jobMetadata3 = (V2JobMetadataWritable) jobManager3.getJobMetadata();
        eventSubject.onNext(new JobStateChangeEvent<V2JobMetadata>(jobMetadata3.getJobId(), toJobState(jobMetadata3.getState()), System.currentTimeMillis(), jobMetadata3));

        assertThat(testSubscriber.takeNext().getNotificationCase()).isEqualByComparingTo(NotificationCase.JOBUPDATE);
    }

    private BaseJobMgr createJobManager(String name) {
        V2JobMetadataWritable jobMetadata = (V2JobMetadataWritable) generator.newJobMetadata(Parameters.JobType.Service, name);
        BaseJobMgr jobManager = mock(BaseJobMgr.class);
        when(jobManager.getJobMetadata()).thenReturn(jobMetadata);
        when(jobOperations.getJobMgr(jobMetadata.getJobId())).thenReturn(jobManager);
        return jobManager;
    }

    private V2WorkerMetadata createTask(V2JobMetadataWritable jobMetadata) {
        String jobId = jobMetadata.getJobId();
        generator.scheduleJob(jobId);
        generator.moveWorkerToState(jobId, 0, V2JobState.Started);
        return generator.getRunningWorkers(jobId).get(0);
    }

    private JobStateChangeEvent.JobState toJobState(V2JobState state) {
        switch (state) {
            case Accepted:
            case Launched:
            case StartInitiated:
            case Started:
                return JobStateChangeEvent.JobState.Created;
            case Failed:
            case Completed:
                return JobStateChangeEvent.JobState.Finished;
        }
        throw new IllegalArgumentException("Invalid job state " + state);
    }
}