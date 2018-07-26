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

import java.util.List;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.NotificationCase;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
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

    private final TitusStackResource titusStackResource = new TitusStackResource(EmbeddedTitusCells.basicCell(2));

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
    }

    @Test(timeout = 30_000)
    public void testObserveJobs() throws Exception {
        TestStreamObserver<JobChangeNotification> eventObserver = new TestStreamObserver<>();
        titusStackResource.getGateway().getV3GrpcClient().observeJobs(Empty.getDefaultInstance(), eventObserver);

        jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startTasksInNewJob()));
        jobsScenarioBuilder.schedule(oneTaskServiceJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startTasksInNewJob()));
        jobsScenarioBuilder.takeJob(0).template(ScenarioTemplates.killJob());
        jobsScenarioBuilder.takeJob(1).template(ScenarioTemplates.killJob());

        List<JobChangeNotification> emittedItems = eventObserver.getEmittedItems();
        assertThat(emittedItems).hasSize(19);
        emittedItems.stream()
                .filter(n -> n.getNotificationCase() == NotificationCase.JOBUPDATE)
                .forEach(n -> CellAssertions.assertCellInfo(n.getJobUpdate().getJob(), EmbeddedTitusMaster.CELL_NAME));
    }
}