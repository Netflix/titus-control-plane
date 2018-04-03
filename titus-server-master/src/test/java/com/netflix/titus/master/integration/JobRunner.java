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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import com.netflix.titus.testkit.client.TitusMasterClient;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.junit.master.JobObserver;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.apache.mesos.Protos;

import static com.netflix.titus.testkit.util.TitusTaskIdParser.getTaskIndexFromTaskId;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A helper class to run different kinds of Titus jobs used within the integration test suite.
 */
class JobRunner {

    private final EmbeddedTitusMaster titusMaster;
    private final TitusMasterClient client;
    private final ExtTestSubscriber<TaskExecutorHolder> taskExecutorHolders;

    JobRunner(EmbeddedTitusMaster titusMaster) {
        this.titusMaster = titusMaster;
        this.client = titusMaster.getClient();
        this.taskExecutorHolders = new ExtTestSubscriber<>();
        titusMaster.observeLaunchedTasks().subscribe(taskExecutorHolders);
    }

    List<TaskExecutorHolder> runJob(TitusJobSpec jobSpec) throws InterruptedException {
        int expected = jobSpec.getType() == TitusJobType.batch
                ? jobSpec.getInstances()
                : jobSpec.getInstancesDesired();

        String jobId = client.submitJob(jobSpec).toBlocking().first();
        List<TaskExecutorHolder> holders = new ArrayList<>(expected);
        for (int i = 0; i < expected; i++) {
            TaskExecutorHolder taskHolder = taskExecutorHolders.takeNext(5, TimeUnit.SECONDS);
            assertThat(taskHolder).overridingErrorMessage("Timed out during waiting on the next task executor holder for job " + jobId).isNotNull();
            assertThat(taskHolder.getJobId()).isEqualTo(jobId);

            taskHolder.transitionTo(Protos.TaskState.TASK_STARTING);
            taskHolder.transitionTo(Protos.TaskState.TASK_RUNNING);

            holders.add(taskHolder);
        }
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.awaitTasksInState(TitusTaskState.RUNNING, getTaskIds(holders));

        holders.sort(Comparator.comparingInt(left -> getTaskIndexFromTaskId(left.getTaskId())));

        return holders;
    }

    ExtTestSubscriber<TaskExecutorHolder> getTaskExecutorHolders() {
        return taskExecutorHolders;
    }

    private String[] getTaskIds(Collection<TaskExecutorHolder> holders) {
        return holders.stream().map(TaskExecutorHolder::getTaskId).toArray(String[]::new);
    }
}
