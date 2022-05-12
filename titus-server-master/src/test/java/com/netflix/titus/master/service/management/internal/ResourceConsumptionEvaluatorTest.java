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
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
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
import static com.netflix.titus.master.service.management.internal.ResourceConsumptionEvaluator.perTaskResourceDimension;
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
        Pair<Job, List<Task>> goodCapacity = newServiceJob(
                "goodCapacityJob",
                jd -> jd.toBuilder().withCapacityGroup(ConsumptionModelGenerator.CRITICAL_SLA_1.getAppName()).build()
        );
        Job goodCapacityJob = goodCapacity.getLeft();

        // Job without appName defined
        Pair<Job, List<Task>> noAppName = newServiceJob(
                "badCapacityJob",
                jd -> jd.toBuilder()
                        .withApplicationName("")
                        .withCapacityGroup(ConsumptionModelGenerator.DEFAULT_SLA.getAppName())
                        .build()
        );
        Job noAppNameJob = noAppName.getLeft();

        // Job with capacity group for which SLA is not defined
        Pair<Job, List<Task>> badCapacity = newServiceJob(
                "goodCapacityJob",
                jd -> jd.toBuilder().withCapacityGroup("missingCapacityGroup").build()
        );
        Job badCapacityJob = badCapacity.getLeft();

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
        assertThat(criticalConsumption.getCurrentConsumption()).isEqualTo(perTaskResourceDimension(goodCapacityJob)); // We have single worker in Started state

        assertThat(criticalConsumption.getAllowedConsumption()).isEqualTo(ConsumptionModelGenerator.capacityGroupLimit(ConsumptionModelGenerator.CRITICAL_SLA_1));
        assertThat(criticalConsumption.isAboveLimit()).isTrue();

        // Default capacity group
        CompositeResourceConsumption defaultConsumption = (CompositeResourceConsumption) findConsumption(
                systemConsumption, Tier.Flex.name(), ConsumptionModelGenerator.DEFAULT_SLA.getAppName()
        ).get();
        assertThat(defaultConsumption.getCurrentConsumption()).isEqualTo(ResourceDimensions.add(
                perTaskResourceDimension(noAppNameJob),
                perTaskResourceDimension(badCapacityJob)
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

    @SuppressWarnings("unchecked")
    @Test
    public void batchJobWithMultipleTasks() {
        when(applicationSlaManagementService.getApplicationSLAs()).thenReturn(asList(ConsumptionModelGenerator.DEFAULT_SLA, ConsumptionModelGenerator.CRITICAL_SLA_1, ConsumptionModelGenerator.NOT_USED_SLA));

        // Job with defined capacity group SLA
        Job<BatchJobExt> goodCapacityJob = newBatchJob(
                "goodCapacityJob",
                jd -> jd.toBuilder()
                        .withExtensions(jd.getExtensions().toBuilder().withSize(2).build())
                        .withCapacityGroup(ConsumptionModelGenerator.CRITICAL_SLA_1.getAppName())
                        .build()
        ).getLeft();
        List<Task> goodCapacityTasks = jobComponentStub.getJobOperations().getTasks(goodCapacityJob.getId());

        // Job without appName defined
        Job<BatchJobExt> noAppNameJob = newBatchJob(
                "badCapacityJob",
                jd -> jd.toBuilder()
                        .withApplicationName("")
                        .withExtensions(jd.getExtensions().toBuilder().withSize(2).build())
                        .withCapacityGroup(ConsumptionModelGenerator.DEFAULT_SLA.getAppName())
                        .build()
        ).getLeft();
        List<Task> noAppNameTasks = jobComponentStub.getJobOperations().getTasks(noAppNameJob.getId());

        // Job with capacity group for which SLA is not defined
        Job<BatchJobExt> badCapacityJob = newBatchJob(
                "badCapacityJob",
                jd -> jd.toBuilder()
                        .withExtensions(jd.getExtensions().toBuilder().withSize(2).build())
                        .withCapacityGroup("missingCapacityGroup")
                        .build()
        ).getLeft();
        List<Task> badCapacityTasks = jobComponentStub.getJobOperations().getTasks(badCapacityJob.getId());

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
        assertThat(criticalConsumption.getCurrentConsumption()).isEqualTo(expectedCurrentConsumptionForBatchJob(goodCapacityJob, goodCapacityTasks));
        assertThat(criticalConsumption.getMaxConsumption()).isEqualTo(expectedMaxConsumptionForBatchJob(goodCapacityJob));
        assertThat(criticalConsumption.getAllowedConsumption()).isEqualTo(ConsumptionModelGenerator.capacityGroupLimit(ConsumptionModelGenerator.CRITICAL_SLA_1));
        assertThat(criticalConsumption.isAboveLimit()).isTrue();

        // Default capacity group
        CompositeResourceConsumption defaultConsumption = (CompositeResourceConsumption) findConsumption(
                systemConsumption, Tier.Flex.name(), ConsumptionModelGenerator.DEFAULT_SLA.getAppName()
        ).get();
        assertThat(defaultConsumption.getCurrentConsumption()).isEqualTo(ResourceDimensions.add(
                expectedCurrentConsumptionForBatchJob(noAppNameJob, noAppNameTasks),
                expectedCurrentConsumptionForBatchJob(badCapacityJob, badCapacityTasks)
        ));
        assertThat(defaultConsumption.getMaxConsumption()).isEqualTo(ResourceDimensions.add(
                expectedMaxConsumptionForBatchJob(noAppNameJob),
                expectedMaxConsumptionForBatchJob(badCapacityJob)
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

    private Pair<Job, List<Task>> newServiceJob(String name, Function<JobDescriptor, JobDescriptor> transformer) {
        jobComponentStub.addJobTemplate(name, JobDescriptorGenerator.serviceJobDescriptors()
                .map(jd -> jd.but(self -> self.getContainer().but(c -> CONTAINER_RESOURCES)))
                .map(transformer::apply)
        );
        return jobComponentStub.createJobAndTasks(
                name,
                (job, tasks) -> jobComponentStub.moveTaskToState(tasks.get(0), TaskState.Started)
        );
    }

    private Pair<Job, List<Task>> newBatchJob(String name, Function<JobDescriptor<BatchJobExt>, JobDescriptor<BatchJobExt>> transformer) {
        jobComponentStub.addJobTemplate(name, JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(self -> self.getContainer().but(c -> CONTAINER_RESOURCES)))
                .map(transformer::apply)
        );
        return jobComponentStub.createJobAndTasks(
                name,
                (job, tasks) -> tasks.forEach(task -> jobComponentStub.moveTaskToState(task, TaskState.Started))
        );
    }

    private static ResourceDimension expectedCurrentConsumptionForBatchJob(Job<BatchJobExt> job, List<Task> tasks) {
        return ResourceDimensions.multiply(perTaskResourceDimension(job), job.getJobDescriptor().getExtensions().getSize());
    }

    private static ResourceDimension expectedMaxConsumptionForBatchJob(Job<BatchJobExt> job) {
        return ResourceDimensions.multiply(perTaskResourceDimension(job), job.getJobDescriptor().getExtensions().getSize());
    }
}