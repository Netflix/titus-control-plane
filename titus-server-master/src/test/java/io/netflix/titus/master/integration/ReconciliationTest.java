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

package io.netflix.titus.master.integration;

import java.security.Permission;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import io.netflix.titus.master.mesos.MesosSchedulerCallbackHandler;
import io.netflix.titus.master.store.V2JobMetadataWritable;
import io.netflix.titus.master.store.V2WorkerMetadataWritable;
import io.netflix.titus.testkit.client.TitusMasterClient;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedMesosSchedulerDriver;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.master.EmbeddedStorageProvider;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.JobObserver;
import io.netflix.titus.testkit.junit.master.TitusMasterResource;
import io.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.apache.mesos.Protos;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static io.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates.basicSetupActivation;
import static io.netflix.titus.testkit.embedded.cloud.SimulatedClouds.basicCloud;
import static io.netflix.titus.testkit.embedded.master.EmbeddedTitusMasters.basicMaster;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class ReconciliationTest extends BaseIntegrationTest {

    private final EmbeddedStorageProvider storageProvider = new EmbeddedStorageProvider();

    private final TitusMasterResource titusMasterResource = new TitusMasterResource(
            basicMaster(basicCloud(1)).toBuilder().withStorageProvider(storageProvider).build()
    );

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusMasterResource);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(titusMasterResource).around(instanceGroupsScenarioBuilder);

    private EmbeddedTitusMaster titusMaster;

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(basicSetupActivation());
        titusMaster = titusMasterResource.getMaster();
    }

    @Test(timeout = 30000)
    public void testReconciliationAfterEniDuplicateError() throws Exception {
        ExtTestSubscriber<TaskExecutorHolder> taskExecutorHolders = new ExtTestSubscriber<>();
        titusMaster.observeLaunchedTasks().subscribe(taskExecutorHolders);
        TitusV2ModelGenerator generator = new TitusV2ModelGenerator();
        TitusMasterClient client = titusMaster.getClient();
        TitusJobSpec firstJob = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "firstJob"))
                .instancesMin(1).instancesDesired(1).instancesMax(1)
                .build();

        String firstJobId = client.submitJob(firstJob).toBlocking().first();
        TaskExecutorHolder firstTaskHolder = taskExecutorHolders.takeNextOrWait();
        assertThat(firstTaskHolder.getJobId()).isEqualTo(firstJobId);

        JobObserver firstJobObserver = new JobObserver(firstJobId, titusMaster);

        firstTaskHolder.transitionTo(Protos.TaskState.TASK_STARTING);
        firstJobObserver.awaitJobInState(TitusTaskState.STARTING);

        firstTaskHolder.transitionTo(Protos.TaskState.TASK_RUNNING);
        firstJobObserver.awaitJobInState(TitusTaskState.RUNNING);

        TitusJobSpec secondJob = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "secondJob"))
                .instancesMin(1).instancesDesired(1).instancesMax(1)
                .build();

        String secondJobId = client.submitJob(secondJob).toBlocking().first();
        TaskExecutorHolder secondTaskHolder = taskExecutorHolders.takeNextOrWait();
        assertThat(secondTaskHolder.getJobId()).isEqualTo(secondJobId);

        JobObserver secondJobObserver = new JobObserver(secondJobId, titusMaster);

        secondTaskHolder.transitionTo(Protos.TaskState.TASK_STARTING);
        secondJobObserver.awaitJobInState(TitusTaskState.STARTING);

        secondTaskHolder.transitionTo(Protos.TaskState.TASK_RUNNING);
        secondJobObserver.awaitJobInState(TitusTaskState.RUNNING);

        V2JobMetadataWritable firstJobMetadata = storageProvider.getJob(firstJobId);
        V2StageMetadata firstJobStageMetadata = firstJobMetadata.getStageMetadata(1);
        V2WorkerMetadata firstJobWorker = firstJobStageMetadata.getWorkerByIndex(0);
        List<V2WorkerMetadata.TwoLevelResource> firstJobResources = firstJobWorker.getTwoLevelResources();
        V2WorkerMetadata.TwoLevelResource twoLevelResource = firstJobResources.get(0);
        V2WorkerMetadata.TwoLevelResource newTwoLevelResource = new V2WorkerMetadata.TwoLevelResource(twoLevelResource.getAttrName(),
                twoLevelResource.getResName() + ":sg-9999", twoLevelResource.getLabel());

        V2JobMetadataWritable secondJobMetadata = storageProvider.getJob(secondJobId);
        V2StageMetadata secondJobStageMetadata = secondJobMetadata.getStageMetadata(1);
        V2WorkerMetadataWritable secondJobWorker = (V2WorkerMetadataWritable) secondJobStageMetadata.getWorkerByIndex(0);
        secondJobWorker.setTwoLevelResources(Collections.singletonList(newTwoLevelResource));

        final AtomicBoolean exited = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        class CheckExitSecurityManager extends SecurityManager {
            @Override
            public void checkPermission(Permission perm) {
            }

            @Override
            public void checkPermission(Permission perm, Object context) {
            }

            @Override
            public void checkExit(int status) {
                exited.set(true);
                latch.countDown();
                throw new IllegalStateException("Blocking the exit");
            }
        }

        SecurityManager securityManager = System.getSecurityManager();
        System.setSecurityManager(new CheckExitSecurityManager());
        titusMaster.reboot();
        latch.await();
        Assertions.assertThat(exited.get()).isTrue();
        System.setSecurityManager(securityManager);

        titusMaster.getConfig().setProperty("titus.scheduler.schedulerEnabled", "false");
        titusMaster.reboot();
        List<TitusJobInfo> jobs = titusMaster.getClient().findAllJobs().toList().toBlocking().first();
        Assertions.assertThat(jobs).isNotNull();
        Assertions.assertThat(jobs.size()).isEqualTo(2);
        SimulatedMesosSchedulerDriver mesosSchedulerDriver = titusMaster.getMesosSchedulerDriver();
        MesosSchedulerCallbackHandler mesosSchedulerCallbackHandler = mesosSchedulerDriver.getMesosSchedulerCallbackHandler();
        int secondJobArchivedWorkersBefore = storageProvider.getArchivedWorkers(secondJobId).size();

        mesosSchedulerCallbackHandler.reconcileTasks(mesosSchedulerDriver);
        mesosSchedulerCallbackHandler.reconcileTasks(mesosSchedulerDriver);

        int secondJobArchivedWorkersAfter = storageProvider.getArchivedWorkers(secondJobId).size();
        Assertions.assertThat(secondJobArchivedWorkersBefore + 1).isEqualTo(secondJobArchivedWorkersAfter);
    }
}
