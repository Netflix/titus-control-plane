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

package com.netflix.titus.supplementary.relocation.descheduler;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.common.data.generator.MutableDataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.runtime.RelocationAttributes;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import com.netflix.titus.supplementary.relocation.RelocationConnectorStubs;
import com.netflix.titus.supplementary.relocation.TestDataFactory;
import com.netflix.titus.supplementary.relocation.connector.KubernetesNodeDataResolver;
import com.netflix.titus.supplementary.relocation.connector.TitusNode;
import com.netflix.titus.supplementary.relocation.model.DeschedulingResult;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.ofServiceSize;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.withDisruptionBudget;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.withJobId;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.selfManagedPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.unlimitedRate;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultDeschedulerServiceTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final TestClock clock = (TestClock) titusRuntime.getClock();

    protected final RelocationConfiguration configuration = Archaius2Ext.newConfiguration(RelocationConfiguration.class);

    private final MutableDataGenerator<Job<ServiceJobExt>> jobGenerator = new MutableDataGenerator<>(
            JobGenerator.serviceJobs(oneTaskServiceJobDescriptor().but(
                    ofServiceSize(4),
                    withDisruptionBudget(budget(selfManagedPolicy(0), unlimitedRate(), Collections.emptyList()))
            ))
    );

    private final RelocationConnectorStubs dataGenerator = new RelocationConnectorStubs()
            .addActiveInstanceGroup("active1", 10)
            .addRemovableInstanceGroup("removable1", 10)
            .addJob(jobGenerator.getValue().but(withJobId("job1")))
            .addJob(jobGenerator.getValue().but(withJobId("job2")))
            .addJob(jobGenerator.getValue().but(withJobId("jobImmediate")));

    private final ReadOnlyJobOperations jobOperations = dataGenerator.getJobOperations();

    private final DefaultDeschedulerService deschedulerService = new DefaultDeschedulerService(
            dataGenerator.getJobOperations(),
            dataGenerator.getEvictionOperations(),
            dataGenerator.getNodeDataResolver(),
            () -> "foo|bar",
            titusRuntime
    );

    @Test
    public void testAllExpectedJobMigrationsAreFound() {
        List<Task> tasksOfJob1 = jobOperations.getTasks("job1");
        dataGenerator.place("active1", tasksOfJob1.get(0), tasksOfJob1.get(1));
        dataGenerator.place("removable1", tasksOfJob1.get(2), tasksOfJob1.get(3));
        dataGenerator.setQuota("job1", 2);

        List<Task> tasksOfJob2 = jobOperations.getTasks("job2");
        dataGenerator.place("active1", tasksOfJob2.get(0), tasksOfJob2.get(1));
        dataGenerator.place("removable1", tasksOfJob2.get(2), tasksOfJob2.get(3));
        dataGenerator.setQuota("job2", 2);

        dataGenerator.addJobAttribute("jobImmediate", RelocationAttributes.RELOCATION_REQUIRED_BY_IMMEDIATELY, "" + (clock.wallTime() + 1));
        Task taskImmediate = jobOperations.getTasks("jobImmediate").get(0);
        dataGenerator.place("active1", taskImmediate);

        List<DeschedulingResult> results = deschedulerService.deschedule(Collections.emptyMap());
        assertThat(results).hasSize(5);
        for (DeschedulingResult result : results) {
            boolean isImmediateJobMigration = result.getTask().getId().equals(taskImmediate.getId());
            if (isImmediateJobMigration) {
                assertThat(result.getAgentInstance().getServerGroupId()).isEqualTo("active1");
            } else {
                assertThat(result.getAgentInstance().getServerGroupId()).isEqualTo("removable1");
            }
            TaskRelocationPlan plan = result.getTaskRelocationPlan();
            if (plan.getTaskId().startsWith("jobImmediate")) {
                assertThat(plan.getReason()).isEqualTo(TaskRelocationReason.TaskMigration);
            } else {
                assertThat(plan.getReason()).isEqualTo(TaskRelocationReason.AgentEvacuation);
            }
            if (isImmediateJobMigration) {
                assertThat(plan.getReasonMessage()).containsSequence("Job marked for immediate eviction");
            } else {
                assertThat(plan.getReasonMessage()).isEqualTo("Enough quota to migrate the task (no migration delay configured)");
            }
        }
    }

    @Test
    public void verifySelfManagedRelocationPlanWithDelay() {
        verifyRelocationPlan(10_000, "Agent instance tagged for eviction");
    }

    @Test
    public void verifyRelocationPlanWithNoDelay() {
        verifyRelocationPlan(0, "Enough quota to migrate the task (no migration delay configured)");
    }

    private void verifyRelocationPlan(long relocationDelay, String reasonMessage) {
        ReadOnlyJobOperations jobOperations = mock(ReadOnlyJobOperations.class);
        DefaultDeschedulerService dds = new DefaultDeschedulerService(
                jobOperations,
                mock(ReadOnlyEvictionOperations.class),
                new KubernetesNodeDataResolver(configuration, TestDataFactory.mockFabric8IOConnector(), node -> true),
                () -> "foo|bar",
                titusRuntime
        );

        Job<ServiceJobExt> job = JobGenerator.serviceJobs(
                oneTaskServiceJobDescriptor()
                        .but(ofServiceSize(2),
                                withDisruptionBudget(budget(selfManagedPolicy(relocationDelay), unlimitedRate(), Collections.emptyList()))))
                .getValue();

        ServiceJobTask task = JobGenerator.serviceTasks(job).getValue();
        when(jobOperations.getJob(job.getId())).thenReturn(Optional.of(job));

        TitusNode node = TitusNode.newBuilder()
                .withId("node1")
                .withServerGroupId("asg1")
                .withRelocationRequired(true).withBadCondition(false).build();

        // Advance test clock
        long clockAdvancedMs = 5_000;
        TestClock testClock = (TestClock) titusRuntime.getClock();
        testClock.advanceTime(Duration.ofMillis(clockAdvancedMs));

        Optional<TaskRelocationPlan> relocationPlanForTask = dds.getRelocationPlanForTask(node, task, Collections.emptyMap());
        assertThat(relocationPlanForTask).isPresent();
        assertThat(relocationPlanForTask.get().getTaskId()).isEqualTo(task.getId());
        // relocation time is expected to be decision clock time + retentionTimeMs
        assertThat(relocationPlanForTask.get().getRelocationTime()).isEqualTo(relocationDelay + clockAdvancedMs);
        assertThat(relocationPlanForTask.get().getDecisionTime()).isEqualTo(clockAdvancedMs);
        assertThat(relocationPlanForTask.get().getReasonMessage()).isEqualTo(reasonMessage);
    }
}