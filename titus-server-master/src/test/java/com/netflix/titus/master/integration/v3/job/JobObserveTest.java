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

package com.netflix.titus.master.integration.v3.job;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.protobuf.Empty;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.NotificationCase;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.embedded.stack.EmbeddedTitusStacks;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 */
@Category(IntegrationTest.class)
public class JobObserveTest extends BaseIntegrationTest {

    private static final String V2_ENGINE_APP = "myV2App";

    private static final JobDescriptor<BatchJobExt> V3_ONE_TASK_BATCH_JOB = oneTaskBatchJobDescriptor().toBuilder()
            .withApplicationName(TitusStackResource.V3_ENGINE_APP_PREFIX)
            .build();

    private static final JobDescriptor<BatchJobExt> V2_ONE_TASK_BATCH_JOB = oneTaskBatchJobDescriptor().toBuilder()
            .withApplicationName(V2_ENGINE_APP)
            .build();

    private static final JobDescriptor<ServiceJobExt> V2_ONE_TASK_SERVICE_JOB = oneTaskServiceJobDescriptor().toBuilder()
            .withApplicationName(V2_ENGINE_APP)
            .build();

    private final TitusStackResource titusStackResource = new TitusStackResource(EmbeddedTitusStacks.basicStack(1));

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicSetupActivation());
    }

    @Test(timeout = 30_000)
    public void testObserveJobs() throws Exception {
        TestStreamObserver<JobChangeNotification> eventObserver = new TestStreamObserver<>();
        titusStackResource.getGateway().getV3GrpcClient().observeJobs(Empty.getDefaultInstance(), eventObserver);

        jobsScenarioBuilder.schedule(V2_ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startV2TasksInNewJob()));
        jobsScenarioBuilder.schedule(V3_ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startTasksInNewJob()));
        jobsScenarioBuilder.takeJob(0).template(ScenarioTemplates.killV2Job());
        jobsScenarioBuilder.takeJob(1).template(ScenarioTemplates.killJob());

        List<JobChangeNotification> emittedItems = eventObserver.getEmittedItems();
        assertThat(emittedItems).hasSize(16);
        emittedItems.stream()
                .filter(n -> n.getNotificationCase() == NotificationCase.JOBUPDATE)
                .forEach(n -> CellAssertions.assertCellInfo(n.getJobUpdate().getJob(), EmbeddedTitusMaster.CELL_NAME));
    }

    @Test(timeout = 30_000)
    public void testV2EngineEventStreamIntegrationForGoodJob() throws Exception {
        jobsScenarioBuilder.schedule(V2_ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startV2TasksInNewJob())
                .template(ScenarioTemplates.killV2Job())
        );
    }

    @Test(timeout = 30_000)
    public void testV2EngineEventStreamIntegrationForBadJob() throws Exception {
        jobsScenarioBuilder.schedule(V2_ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startV2TasksInNewJob())
                .inTask(0, taskScenario -> taskScenario.template(ScenarioTemplates.completeTask()))
                .template(ScenarioTemplates.jobFinished())
        );
    }

    @Test(timeout = 30_000)
    public void testV2EngineJobsEventStreamIncludesAllJobsAndTasks() throws Exception {
        // Prevent the tasks from being scheduled by requesting too many CPUs
        JobDescriptor<ServiceJobExt> v2Job = V2_ONE_TASK_SERVICE_JOB.toBuilder()
                .withExtensions(V2_ONE_TASK_SERVICE_JOB.getExtensions().toBuilder().withCapacity(
                        JobModel.newCapacity().withMin(0).withDesired(100).withMax(100).build()
                ).build())
                .withContainer(V2_ONE_TASK_BATCH_JOB.getContainer().toBuilder()
                        .withContainerResources(
                                V2_ONE_TASK_BATCH_JOB.getContainer().getContainerResources().toBuilder().withCpu(64).build()
                        ).build()
                ).build();

        Iterator<JobChangeNotification> beforeIt = titusStackResource.getGateway().getV3BlockingGrpcClient().observeJobs(Empty.getDefaultInstance());
        beforeIt.next(); // Take snapshot marker

        jobsScenarioBuilder.schedule(v2Job, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.jobAccepted()));

        Iterator<JobChangeNotification> afterIt = titusStackResource.getGateway().getV3BlockingGrpcClient().observeJobs(Empty.getDefaultInstance());

        // Check events from 'after' iterator (all events expected in snapshot)
        List<JobChangeNotification> allEvents = new ArrayList<>();
        while (afterIt.hasNext()) {
            JobChangeNotification event = afterIt.next();
            if (event.getNotificationCase() == NotificationCase.SNAPSHOTEND) {
                break;
            }
            allEvents.add(event);
        }
        assertThat(allEvents).hasSize(1 + 100);

        // Check events from 'before' iterator (all events expected after snapshot)
        for (int i = 0; i < 100; i++) {
            assertThat(beforeIt.hasNext()).isTrue();
            beforeIt.next();
        }
    }
}