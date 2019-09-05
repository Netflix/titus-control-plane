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

package com.netflix.titus.master.service.management.internal;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.master.service.management.CompositeResourceConsumption;
import com.netflix.titus.master.service.management.ResourceConsumption;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.master.service.management.ResourceConsumptions.findConsumption;
import static com.netflix.titus.master.service.management.internal.ResourceConsumptionEvaluator.toResourceDimension;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResourceConsumptionEvaluatorTest {

    private static final ContainerResources CONTAINER_RESOURCES = ContainerResources.newBuilder()
            .withCpu(1)
            .withMemoryMB(1024)
            .withDiskMB(512)
            .withNetworkMbps(128)
            .build();

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final ApplicationSlaManagementService applicationSlaManagementService = mock(ApplicationSlaManagementService.class);

    private final V3JobOperations v3JobOperations = mock(V3JobOperations.class);

    private final JobComponentStub jobComponentStub = new JobComponentStub(titusRuntime);

    private final V3JobOperations jobOperations = jobComponentStub.getJobOperations();

    @Before
    public void setUp() throws Exception {
        when(v3JobOperations.getJobsAndTasks()).then(invocation -> jobOperations.getJobsAndTasks());
    }

    @Test
    public void testEvaluation() {
        when(applicationSlaManagementService.getApplicationSLAs()).thenReturn(asList(ConsumptionModelGenerator.DEFAULT_SLA, ConsumptionModelGenerator.CRITICAL_SLA_1, ConsumptionModelGenerator.NOT_USED_SLA));

        // Job with defined capacity group SLA
        Pair<Job, List<Task>> goodCapacity = newJob(
                "goodCapacityJob",
                jd -> jd.toBuilder().withCapacityGroup(ConsumptionModelGenerator.CRITICAL_SLA_1.getAppName()).build()
        );
        Job goodCapacityJob = goodCapacity.getLeft();
        List<Task> goodCapacityTasks = goodCapacity.getRight();

        // Job without appName defined
        Pair<Job, List<Task>> noAppName = newJob(
                "badCapacityJob",
                jd -> jd.toBuilder()
                        .withApplicationName("")
                        .withCapacityGroup(ConsumptionModelGenerator.DEFAULT_SLA.getAppName())
                        .build()
        );
        Job noAppNameJob = noAppName.getLeft();
        List<Task> noAppNameTasks = noAppName.getRight();

        // Job with capacity group for which SLA is not defined
        Pair<Job, List<Task>> badCapacity = newJob(
                "goodCapacityJob",
                jd -> jd.toBuilder().withCapacityGroup("missingCapacityGroup").build()
        );
        Job badCapacityJob = badCapacity.getLeft();
        List<Task> badCapacityTasks = badCapacity.getRight();

        // Evaluate
        ResourceConsumptionEvaluator evaluator = new ResourceConsumptionEvaluator(applicationSlaManagementService, v3JobOperations);

        Set<String> undefined = evaluator.getUndefinedCapacityGroups();
        assertThat(undefined).contains("missingCapacityGroup");

        CompositeResourceConsumption systemConsumption = evaluator.getSystemConsumption();
        Map<String, ResourceConsumption> tierConsumptions = systemConsumption.getContributors();
        assertThat(tierConsumptions).containsKeys(Tier.Critical.name(), Tier.Flex.name());

        // Critical capacity group
        CompositeResourceConsumption criticalConsumption = (CompositeResourceConsumption) findConsumption(
                systemConsumption, Tier.Critical.name(), ConsumptionModelGenerator.CRITICAL_SLA_1.getAppName()
        ).get();
        assertThat(criticalConsumption.getCurrentConsumption()).isEqualTo(toResourceDimension(goodCapacityJob, goodCapacityTasks)); // We have single worker in Started state

        assertThat(criticalConsumption.getAllowedConsumption()).isEqualTo(ConsumptionModelGenerator.capacityGroupLimit(ConsumptionModelGenerator.CRITICAL_SLA_1));
        assertThat(criticalConsumption.isAboveLimit()).isTrue();

        // Default capacity group
        CompositeResourceConsumption defaultConsumption = (CompositeResourceConsumption) findConsumption(
                systemConsumption, Tier.Flex.name(), ConsumptionModelGenerator.DEFAULT_SLA.getAppName()
        ).get();
        assertThat(defaultConsumption.getCurrentConsumption()).isEqualTo(ResourceDimensions.add(
                toResourceDimension(noAppNameJob, noAppNameTasks),
                toResourceDimension(badCapacityJob, badCapacityTasks)
        ));

        assertThat(defaultConsumption.getAllowedConsumption()).isEqualTo(ConsumptionModelGenerator.capacityGroupLimit(ConsumptionModelGenerator.DEFAULT_SLA));
        assertThat(defaultConsumption.isAboveLimit()).isFalse();

        // Not used capacity group
        CompositeResourceConsumption notUsedConsumption = (CompositeResourceConsumption) findConsumption(
                systemConsumption, Tier.Critical.name(), ConsumptionModelGenerator.NOT_USED_SLA.getAppName()
        ).get();
        assertThat(notUsedConsumption.getCurrentConsumption()).isEqualTo(ResourceDimension.empty());
        assertThat(notUsedConsumption.getAllowedConsumption()).isEqualTo(ConsumptionModelGenerator.capacityGroupLimit(ConsumptionModelGenerator.NOT_USED_SLA));
        assertThat(notUsedConsumption.isAboveLimit()).isFalse();
    }

    private Pair<Job, List<Task>> newJob(String name, Function<JobDescriptor, JobDescriptor> transformer) {
        jobComponentStub.addJobTemplate(name, JobDescriptorGenerator.serviceJobDescriptors()
                .map(jd -> jd.but(self -> self.getContainer().but(c -> CONTAINER_RESOURCES)))
                .map(transformer::apply)
        );
        return jobComponentStub.createJobAndTasks(
                name,
                (job, tasks) -> jobComponentStub.moveTaskToState(tasks.get(0), TaskState.Started)
        );
    }
}