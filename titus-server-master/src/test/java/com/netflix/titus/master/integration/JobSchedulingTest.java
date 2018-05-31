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

package com.netflix.titus.master.integration;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.model.v2.JobCompletedReason;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.model.v2.parameter.Parameter;
import com.netflix.titus.api.model.v2.parameter.Parameters;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.master.Status;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.store.V2JobMetadataWritable;
import com.netflix.titus.master.store.V2StageMetadataWritable;
import com.netflix.titus.master.store.V2WorkerMetadataWritable;
import com.netflix.titus.testkit.client.TitusMasterClient;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedStorageProvider;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.JobObserver;
import com.netflix.titus.testkit.junit.master.TitusMasterResource;
import com.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.observers.TestSubscriber;

import static com.netflix.titus.testkit.embedded.cloud.SimulatedClouds.basicCloud;
import static com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMasters.basicMaster;
import static com.netflix.titus.testkit.model.v2.TitusV2ModelAsserts.assertAllTasksInState;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A collection of integration tests focused on basic job scheduling.
 */
@Category(IntegrationTest.class)
public class JobSchedulingTest extends BaseIntegrationTest {

    private final static Logger logger = LoggerFactory.getLogger(JobSchedulingTest.class);

    private final TitusMasterResource titusMasterResource = new TitusMasterResource(basicMaster(basicCloud(2)));

    private final InstanceGroupsScenarioBuilder scenarioBuilder = new InstanceGroupsScenarioBuilder(titusMasterResource);

    @Rule
    public final RuleChain chain = RuleChain.outerRule(titusMasterResource).around(scenarioBuilder);

    private EmbeddedTitusMaster titusMaster;

    private final TitusV2ModelGenerator generator = new TitusV2ModelGenerator(getClass().getSimpleName());

    private TitusMasterClient client;
    private ExtTestSubscriber<TaskExecutorHolder> taskExecutorHolders;
    private JobRunner jobRunner;


    @Before
    public void setUp() throws Exception {
        scenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicSetupActivation());

        titusMaster = titusMasterResource.getMaster();

        client = titusMaster.getClient();
        jobRunner = new JobRunner(titusMaster);
        taskExecutorHolders = new ExtTestSubscriber<>();
        titusMaster.observeLaunchedTasks().subscribe(taskExecutorHolders);
    }

    /**
     * Verify batch job submit with the expected state transitions.
     */
    @Test(timeout = 30_000)
    public void submitBatchJob() throws Exception {
        runBatchJob(generator.newJobSpec(TitusJobType.batch, "myjob"));
    }

    /**
     * Verify batch job submit with the expected state transitions.
     */
    @Test(timeout = 30_000)
    public void submitBatchJobStuckInLaunched() throws Exception {
        Observable<Status> checkStatusObservable = titusMaster.getWorkerStateMonitor().getAllStatusObservable().flatMap(status -> {
            logger.info(String.format("status %s-%s-%s - %s (%s)", status.getJobId(), status.getWorkerIndex(), status.getWorkerNumber(), status.getState(), status.getReason()));
            if (status.getState() == V2JobState.Failed) {
                assertThat(status.getReason()).isEqualTo(JobCompletedReason.Lost);
            }
            return Observable.just(status);
        });
        final TestSubscriber<Status> statusTestSubscriber = new TestSubscriber<>();
        checkStatusObservable.subscribe(statusTestSubscriber);

        try {
            runBatchJobStuckInLaunched(generator.newJobSpec(TitusJobType.batch, "myjob"));
        } catch (Exception ignored) {
        }

        statusTestSubscriber.assertNoErrors();
    }

    /**
     * Verify batch job submission for two agent clusters with identical fitness, but only one having required
     * resources.
     * TODO We should add second cluster in this test, but as adding cluster requires master restart, we provide two clusters in the initialization step
     */
    @Test(timeout = 30_000)
    public void submitBatchJobWhenTwoAgentClustersWithSameFitnessButDifferentResourceAmounts() throws Exception {
        TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.batch, "myjob")).cpu(7).build();
        TaskExecutorHolder taskHolder = runBatchJob(jobSpec);

        assertThat(taskHolder.getInstanceType()).isEqualTo(AwsInstanceType.M3_2XLARGE);
    }

    @Test(timeout = 30_000)
    public void submitGpuBatchJob() throws Exception {
        TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.batch, "myjob")).gpu(1).build();
        TaskExecutorHolder taskHolder = runBatchJob(jobSpec);

        assertThat(taskHolder.getInstanceType()).isEqualTo(AwsInstanceType.G2_2XLarge);
    }

    @Test(timeout = 30_000)
    public void submitBatchJobAndRebootTitusMaster() throws Exception {
        TaskExecutorHolder holder = jobRunner.runJob(generator.newJobSpec(TitusJobType.batch, "myjob")).get(0);
        titusMaster.reboot();
        TitusJobInfo jobInfo = client.findJob(holder.getJobId(), false).toBlocking().first();
        assertAllTasksInState(jobInfo, TitusTaskState.RUNNING);
    }

    @Test(timeout = 30_000)
    public void submitServiceJobWithTooFewRunningWorkersAndRebootTitusMaster() throws Exception {
        TaskExecutorHolder holder = jobRunner.runJob(generator.newJobSpec(TitusJobType.service, "myjob")).get(0);
        titusMaster.shutdown();

        // Change task state to force creation on startup
        EmbeddedStorageProvider storage = (EmbeddedStorageProvider) titusMaster.getStorageProvider();
        V2StageMetadataWritable stageMetadata = (V2StageMetadataWritable) storage.getJob(holder.getJobId()).getStageMetadata(1);

        V2WorkerMetadataWritable worker = (V2WorkerMetadataWritable) stageMetadata.getWorkerByIndex(0);
        worker.setState(V2JobState.Failed, System.currentTimeMillis(), JobCompletedReason.Killed);

        titusMaster.boot();

        TitusJobInfo jobInfo = client.findJob(holder.getJobId(), false).toBlocking().first();
        assertThat(jobInfo.getTasks()).hasSize(2);
        TaskInfo replacement = jobInfo.getTasks().get(0).getId().contains("worker-0")
                ? jobInfo.getTasks().get(0)
                : jobInfo.getTasks().get(1);
        assertThat(replacement.getId()).endsWith("-52");
    }

    private TaskExecutorHolder runBatchJob(TitusJobSpec myjob) throws InterruptedException {
        String jobId = client.submitJob(myjob).toBlocking().first();
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        TaskExecutorHolder taskHolder = taskExecutorHolders.takeNextOrWait();

        taskHolder.transitionTo(Protos.TaskState.TASK_STARTING);
        jobObserver.awaitJobInState(TitusTaskState.STARTING);

        taskHolder.transitionTo(Protos.TaskState.TASK_RUNNING);
        jobObserver.awaitJobInState(TitusTaskState.RUNNING);

        taskHolder.transitionTo(Protos.TaskState.TASK_FINISHED);
        jobObserver.awaitJobInState(TitusTaskState.FINISHED);

        return taskHolder;
    }

    private TaskExecutorHolder runBatchJobStuckInLaunched(TitusJobSpec myjob) throws InterruptedException {
        String jobId = client.submitJob(myjob).toBlocking().first();
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        TaskExecutorHolder taskHolder = taskExecutorHolders.takeNextOrWait();
        jobObserver.awaitJobInState(TitusTaskState.FINISHED);
        return taskHolder;
    }

    /**
     * Verify service job submit with the expected state transitions, and explicit termination (kill).
     */
    @Test(timeout = 30_000)
    public void submitServiceJob() throws Exception {
        TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "myjob"))
                .instancesMin(1).instancesDesired(1).instancesMax(1)
                .build();
        runServiceJobAndKillIt(jobSpec);
    }


    /**
     * Verify service job getting stuck in Launched assigns correct completedReason
     */
    @Test(timeout = 30_000)
    public void submitServiceJobStuckInLaunched() throws Exception {
        Observable<Status> checkStatusObservable = titusMaster.getWorkerStateMonitor().getAllStatusObservable().flatMap(status -> {
            logger.info(String.format("status %s-%s-%s - %s (%s)", status.getJobId(), status.getWorkerIndex(), status.getWorkerNumber(), status.getState(), status.getReason()));
            if (status.getState() == V2JobState.Failed) {
                assertThat(status.getReason()).isEqualTo(JobCompletedReason.Lost);
            }
            return Observable.just(status);
        });
        final TestSubscriber<Status> statusTestSubscriber = new TestSubscriber<>();
        checkStatusObservable.subscribe(statusTestSubscriber);

        try {
            TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "myjob"))
                    .instancesMin(1).instancesDesired(1).instancesMax(1)
                    .build();
            runServiceJobStuckInLaunched(jobSpec);
        } catch (Exception ignored) {
        }

        statusTestSubscriber.assertNoErrors();
    }

    /**
     * Verify that a service job is automatically restarted, after a task exits.
     */
    @Test(timeout = 30_000)
    public void submitServiceJobFinishItAndCheckThatItRestarts() throws Exception {
        TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "myjob"))
                .instancesMin(1).instancesDesired(1).instancesMax(1)
                .restartOnSuccess(true)
                .retries(2)
                .build();

        TaskExecutorHolder firstTaskHolder = runServiceJob(jobSpec);
        JobObserver jobObserver = new JobObserver(firstTaskHolder.getJobId(), titusMaster);

        // Finish the running task
        firstTaskHolder.transitionTo(Protos.TaskState.TASK_FINISHED);
        jobObserver.awaitTasksInState(TitusTaskState.FINISHED, firstTaskHolder.getTaskId());

        // Now check a new task was created in place
        TaskExecutorHolder secondTaskHolder = taskExecutorHolders.takeNextOrWait();
        secondTaskHolder.transitionTo(Protos.TaskState.TASK_STARTING);
        secondTaskHolder.transitionTo(Protos.TaskState.TASK_RUNNING);

        jobObserver.awaitTasksInState(TitusTaskState.RUNNING, secondTaskHolder.getTaskId());
    }

    @Test(timeout = 30_000)
    public void jobsAndTasksContainCellInfo() throws Exception {
        String cellName = UUID.randomUUID().toString();
        TitusV2ModelGenerator generator = new TitusV2ModelGenerator(cellName);
        TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "myjob"))
                .instancesMin(1).instancesDesired(1).instancesMax(1)
                .restartOnSuccess(true)
                .retries(2)
                .build();

        String jobId = runServiceJob(jobSpec).getJobId();

        final V2JobMetadataWritable job = ((EmbeddedStorageProvider) titusMaster.getStorageProvider()).getJob(jobId);
        List<Parameter> parameters = job.getParameters();
        final Map<String, String> labels = Parameters.getLabels(parameters);
        assertThat(labels).containsEntry(JobAttributes.JOB_ATTRIBUTES_CELL, EmbeddedTitusMaster.CELL_NAME);

        Collection<V2WorkerMetadata> tasksMetadata = job.getStageMetadata(1).getAllWorkers();
        tasksMetadata.forEach(workerMetadata ->
                assertThat(workerMetadata.getCell()).isEqualTo(EmbeddedTitusMaster.CELL_NAME)
        );
    }

    private TaskExecutorHolder runServiceJob(TitusJobSpec jobSpec) throws InterruptedException {
        String jobId = client.submitJob(jobSpec).toBlocking().first();
        TaskExecutorHolder taskHolder = taskExecutorHolders.takeNextOrWait();
        assertThat(taskHolder.getJobId()).isEqualTo(jobId);

        JobObserver jobObserver = new JobObserver(jobId, titusMaster);

        taskHolder.transitionTo(Protos.TaskState.TASK_STARTING);
        jobObserver.awaitJobInState(TitusTaskState.STARTING);

        taskHolder.transitionTo(Protos.TaskState.TASK_RUNNING);
        jobObserver.awaitJobInState(TitusTaskState.RUNNING);

        return taskHolder;
    }

    private TaskExecutorHolder runServiceJobStuckInLaunched(TitusJobSpec jobSpec) throws InterruptedException {
        String jobId = client.submitJob(jobSpec).toBlocking().first();
        TaskExecutorHolder taskHolder = taskExecutorHolders.takeNextOrWait();
        assertThat(taskHolder.getJobId()).isEqualTo(jobId);
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.awaitJobInState(TitusTaskState.STOPPED);
        return taskHolder;
    }

    private void runServiceJobAndKillIt(TitusJobSpec jobSpec) throws InterruptedException {
        TaskExecutorHolder taskHolder = runServiceJob(jobSpec);

        // Now kill the service
        String jobId = taskHolder.getJobId();
        client.killJob(jobId).toBlocking().firstOrDefault(null);

        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.awaitJobInState(TitusTaskState.STOPPED);
    }
}