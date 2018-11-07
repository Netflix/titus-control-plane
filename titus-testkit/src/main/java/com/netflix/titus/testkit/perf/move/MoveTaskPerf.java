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

package com.netflix.titus.testkit.perf.move;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Owner;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.UnlimitedDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.migration.SelfManagedMigrationPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

/**
 * Performance test tool that creates jobs in pairs, and moves tasks between them. In each pair one of the job is created with
 * the requested number of tasks, and the other one empty. All tasks are moved from the first one to the other.
 */
public class MoveTaskPerf {

    private static final Logger logger = LoggerFactory.getLogger(MoveTaskPerf.class);

    private final List<JobPairTasksMover> movers;

    public MoveTaskPerf(JobManagementClient client, int jobPairCount, int jobSize, int batchSize) {
        JobDescriptor<ServiceJobExt> jobDescriptor = newJobDescriptor();
        this.movers = Flux.merge(Evaluators.evaluateTimes(jobPairCount, index -> JobPairTasksMover.newTaskMover(client, jobDescriptor, jobSize, batchSize)))
                .collectList()
                .block();
    }

    public void shutdown() {
        logger.info("Shutdown initiated...");
        movers.forEach(JobPairTasksMover::shutdown);
    }

    public void moveAll() {
        Flux.merge(movers.stream().map(JobPairTasksMover::waitForTasksToStart).collect(Collectors.toList())).blockFirst();

        logger.info("Starting moving tasks...");
        Stopwatch stopwatch = Stopwatch.createStarted();
        Flux.merge(movers.stream().map(JobPairTasksMover::move).collect(Collectors.toList())).blockFirst();
        logger.info("Finished in {}", DateTimeExt.toTimeUnitString(stopwatch.elapsed(TimeUnit.MILLISECONDS)));
    }

    protected JobDescriptor<ServiceJobExt> newJobDescriptor() {
        return JobDescriptor.<ServiceJobExt>newBuilder()
                .withOwner(Owner.newBuilder().withTeamEmail("titusops@netflix.com").build())
                .withApplicationName("moveTaskPerfTestJob")
                .withCapacityGroup("DEFAULT")
                .withDisruptionBudget(DisruptionBudget.newBuilder()
                        .withDisruptionBudgetPolicy(SelfManagedDisruptionBudgetPolicy.newBuilder().build())
                        .withDisruptionBudgetRate(UnlimitedDisruptionBudgetRate.newBuilder().build())
                        .withContainerHealthProviders(Collections.emptyList())
                        .withTimeWindows(Collections.emptyList())
                        .build()
                )
                .withAttributes(Collections.emptyMap())
                .withContainer(Container.newBuilder()
                        .withContainerResources(ContainerResources.newBuilder()
                                .withCpu(1)
                                .withMemoryMB(10)
                                .withNetworkMbps(128)
                                .withDiskMB(128)
                                .build()
                        )
                        .withImage(Image.newBuilder()
                                .withName("trustybase")
                                .withTag("latest")
                                .build()
                        )
                        .withEntryPoint(Collections.singletonList("sleep 3600"))
                        .withCommand(Collections.emptyList())
                        .withEnv(Collections.singletonMap("TASK_LIFECYCLE_1", "launched: delay=1s; startInitiated: delay=1s; started: delay=1h; killInitiated: delay=1s;"))
                        .build()
                )
                .withExtensions(ServiceJobExt.newBuilder()
                        .withCapacity(Capacity.newBuilder().build())
                        .withRetryPolicy(ImmediateRetryPolicy.newBuilder().build())
                        .withServiceJobProcesses(ServiceJobProcesses.newBuilder().build())
                        .withMigrationPolicy(SelfManagedMigrationPolicy.newBuilder().build())
                        .build()
                )
                .build();
    }
}
