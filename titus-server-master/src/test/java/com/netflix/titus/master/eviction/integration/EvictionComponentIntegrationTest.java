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

package com.netflix.titus.master.eviction.integration;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionFactory;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.grpc.protogen.EvictionQuota;
import com.netflix.titus.grpc.protogen.EvictionServiceEvent;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc.EvictionServiceBlockingStub;
import com.netflix.titus.grpc.protogen.ObserverEventRequest;
import com.netflix.titus.grpc.protogen.Reference;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.eviction.service.quota.system.SystemDisruptionBudgetDescriptor;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCell;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMasters;
import com.netflix.titus.testkit.embedded.cloud.SimulatedClouds;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.hourlyRatePercentage;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.newBatchJobDescriptor;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.percentageOfHealthyPolicy;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class EvictionComponentIntegrationTest extends BaseIntegrationTest {

    private static final JobDescriptor<BatchJobExt> JOB_TEMPLATE = newBatchJobDescriptor(
            2,
            budget(percentageOfHealthyPolicy(50), hourlyRatePercentage(50), Collections.emptyList())
    );

    private static final TitusStackResource titusStackResource = new TitusStackResource(EmbeddedTitusCell.aTitusCell()
            .withMaster(EmbeddedTitusMasters.basicMaster(SimulatedClouds.basicCloud(2)).toBuilder()
                    .withCellName("test")
                    .withSystemDisruptionBudgetDescriptor(new SystemDisruptionBudgetDescriptor(10, 10, Collections.emptyList()))
                    .build()
            )
            .withDefaultGateway()
            .build(), false);

    private static final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private static final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    private static EvictionServiceBlockingStub client;

    @BeforeClass
    public static void setUp() {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
        client = titusStackResource.getOperations().getBlockingGrpcEvictionClient();
    }

    @Test
    public void testGlobalQuotaAccess() {
        EvictionQuota quota = client.getEvictionQuota(Reference.newBuilder().setSystem(Reference.System.getDefaultInstance()).build());
        assertThat(quota.getTarget().getReferenceCase()).isEqualTo(Reference.ReferenceCase.SYSTEM);
        assertThat(quota.getQuota()).isGreaterThan(0);
    }

    @Test(timeout = LONG_TEST_TIMEOUT_MS)
    public void testJobQuotaAccess() throws Exception {
        jobsScenarioBuilder.schedule(JOB_TEMPLATE, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .andThen(() -> awaitQuota(jobScenarioBuilder, 1))
                .inTask(0, tsb -> tsb.transitionTo(TaskStatus.TaskState.Finished))
                .andThen(() -> awaitQuota(jobScenarioBuilder, 0))
        );
    }

    @Test(timeout = LONG_TEST_TIMEOUT_MS)
    public void testEventStream() throws Exception {
        Iterator<EvictionServiceEvent> eventIt = client.observeEvents(ObserverEventRequest.getDefaultInstance());

        jobsScenarioBuilder.schedule(JOB_TEMPLATE, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .andThen(() -> await().until(() -> hasJobQuotaEvent(eventIt, 1)))
                .inTask(0, TaskScenarioBuilder::evictTask)
                .andThen(() -> await().until(() -> hasJobQuotaEvent(eventIt, 0)))
                .inTask(1, tsb -> {
                            try {
                                tsb.evictTask();
                                throw new IllegalStateException("Error expected");
                            } catch (Exception e) {
                                // Expected
                            }
                            return tsb.andThen(() -> await().until(() -> awaitTaskTerminationError(eventIt, tsb.getTask().getId())));
                        }
                )
        );
    }

    private ConditionFactory await() {
        return Awaitility.await().timeout(10, TimeUnit.SECONDS);
    }

    private void awaitQuota(JobScenarioBuilder jobScenarioBuilder, int expectedQuota) {
        await().until(() -> {
            EvictionQuota quota = client.getEvictionQuota(Reference.newBuilder().setJobId(jobScenarioBuilder.getJobId()).build());
            assertThat(quota.getTarget().getReferenceCase()).isEqualTo(Reference.ReferenceCase.JOBID);
            assertThat(quota.getQuota()).isEqualTo(expectedQuota);
        });
    }

    private boolean awaitTaskTerminationError(Iterator<EvictionServiceEvent> eventIt, String taskId) {
        while (eventIt.hasNext()) {
            EvictionServiceEvent event = eventIt.next();
            if (event.getEventCase() == EvictionServiceEvent.EventCase.TASKTERMINATIONEVENT) {
                EvictionServiceEvent.TaskTerminationEvent termination = event.getTaskTerminationEvent();
                if (termination.getTaskId().equals(taskId)) {
                    assertThat(termination.getApproved()).isFalse();
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasJobQuotaEvent(Iterator<EvictionServiceEvent> eventIt, int quotaLevel) {
        while (eventIt.hasNext()) {
            EvictionServiceEvent event = eventIt.next();
            if (event.getEventCase() == EvictionServiceEvent.EventCase.EVICTIONQUOTAEVENT) {
                EvictionQuota quota = event.getEvictionQuotaEvent().getQuota();
                if (quota.getTarget().getReferenceCase() == Reference.ReferenceCase.JOBID) {
                    return quota.getQuota() == quotaLevel;
                }
            }
        }
        return false;
    }
}
