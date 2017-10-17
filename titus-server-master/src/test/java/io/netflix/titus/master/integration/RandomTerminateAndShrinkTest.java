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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.endpoint.v2.rest.representation.JobSetInstanceCountsCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.testkit.client.TitusMasterClient;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.JobObserver;
import io.netflix.titus.testkit.junit.master.TitusMasterResource;
import io.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;
import static io.netflix.titus.testkit.util.TitusTaskIdParser.getTaskIndexFromTaskId;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class RandomTerminateAndShrinkTest extends BaseIntegrationTest {

    private final Random random = new Random();

    @Rule
    public final TitusMasterResource titusMasterResource = new TitusMasterResource(
            EmbeddedTitusMaster.testTitusMaster()
                    .withFlexTier(0.1, AwsInstanceType.M4_10XLarge)
                    .withAgentCluster(aTitusAgentCluster("flexCluster", 0).withSize(100).withInstanceType(AwsInstanceType.M4_10XLarge))
                    .build()
    );

    private EmbeddedTitusMaster titusMaster;

    private final TitusV2ModelGenerator generator = new TitusV2ModelGenerator();

    private TitusMasterClient client;
    private ExtTestSubscriber<TaskExecutorHolder> taskExecutorHolders;

    @Before
    public void setUp() throws Exception {
        titusMaster = titusMasterResource.getMaster();
        client = titusMaster.getClient();
        taskExecutorHolders = new ExtTestSubscriber<>();
        titusMaster.observeLaunchedTasks().subscribe(taskExecutorHolders);
    }

    @Test
    @Ignore
    public void testRandom() throws Exception {
        TitusJobSpec jobSpec = new TitusJobSpec.Builder(
                generator.newJobSpec(TitusJobType.service, "flexJob")
        ).instancesDesired(1).build();

        // Initial setup
        String jobId = client.submitJob(jobSpec).toBlocking().first();
        List<TaskExecutorHolder> holders = starting(jobId, 1);

        JobObserver jobObserver = new JobObserver(jobId, titusMaster);

        // Main loop
        for (int i = 0; i < 10; i++) {
            // Set new desired
            int desired = random.nextInt(20);
            if (desired > holders.size()) {
                holders.addAll(scaleUp(jobId, holders.size(), desired));
            } else {
                int toRemove = holders.size() - desired;
                for (int j = 0; j < toRemove; j++) {
                    scaleDown(holders, jobObserver);
                }
            }
        }
    }

    private List<TaskExecutorHolder> scaleUp(String jobId, int current, int desired) throws InterruptedException {
        client.setInstanceCount(
                new JobSetInstanceCountsCmd("test", jobId, desired, 0, 100)
        ).toBlocking().firstOrDefault(null);

        return starting(jobId, desired - current);
    }

    private void scaleDown(List<TaskExecutorHolder> holders, JobObserver jobObserver) throws InterruptedException {
        int idx = random.nextInt(holders.size());
        TaskExecutorHolder holder = holders.remove(idx);

        client.killTaskAndShrink(holder.getTaskId()).toBlocking().firstOrDefault(null);

        jobObserver.awaitTasksInState(TitusTaskState.STOPPED, holder.getTaskId());
    }

    private List<TaskExecutorHolder> starting(String jobId, int count) throws InterruptedException {
        List<TaskExecutorHolder> holders = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            TaskExecutorHolder taskHolder = taskExecutorHolders.takeNext(30, TimeUnit.SECONDS);
            assertThat(taskHolder).overridingErrorMessage("Timed out during waiting on the next task executor holder for job " + jobId).isNotNull();
            assertThat(taskHolder.getJobId()).isEqualTo(jobId);

            taskHolder.transitionTo(Protos.TaskState.TASK_STARTING);
            taskHolder.transitionTo(Protos.TaskState.TASK_RUNNING);

            holders.add(taskHolder);
        }
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.awaitTasksInState(TitusTaskState.RUNNING, getTaskIds(holders));

        Collections.sort(holders, Comparator.comparingInt(left -> getTaskIndexFromTaskId(left.getTaskId())));

        return holders;
    }

    private String[] getTaskIds(Collection<TaskExecutorHolder> holders) {
        return holders.stream().map(TaskExecutorHolder::getTaskId).toArray(String[]::new);
    }
}
