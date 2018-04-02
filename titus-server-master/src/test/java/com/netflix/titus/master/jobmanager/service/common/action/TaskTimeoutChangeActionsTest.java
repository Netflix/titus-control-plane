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

package com.netflix.titus.master.jobmanager.service.common.action;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.master.jobmanager.service.common.action.task.TaskTimeoutChangeActions;
import com.netflix.titus.master.jobmanager.service.common.action.task.TaskTimeoutChangeActions.TimeoutStatus;
import org.junit.Test;

import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static com.netflix.titus.testkit.model.job.JobGenerator.batchJobs;
import static com.netflix.titus.testkit.model.job.JobGenerator.batchTasks;
import static org.assertj.core.api.Assertions.assertThat;

public class TaskTimeoutChangeActionsTest {

    private static final long DEADLINE_INTERVAL_MS = 100;

    private final TestClock testClock = Clocks.test();

    private final Job<BatchJobExt> job = batchJobs(batchJobDescriptors().getValue()).getValue();
    private final BatchJobTask task = batchTasks(job).getValue();

    @Test
    public void testTimeout() throws Exception {
        BatchJobTask launchedTask = createTaskInState(TaskState.Launched);
        EntityHolder initialRoot = rootFrom(job, launchedTask);
        EntityHolder initialChild = initialRoot.getChildren().first();

        // Initially there is no timeout associated
        TimeoutStatus timeoutStatus = TaskTimeoutChangeActions.getTimeoutStatus(initialChild, testClock);
        assertThat(timeoutStatus).isEqualTo(TimeoutStatus.NotSet);

        // Apply timeout
        List<ModelActionHolder> modelActionHolders = TaskTimeoutChangeActions.setTimeout(
                launchedTask.getId(),
                launchedTask.getStatus().getState(),
                DEADLINE_INTERVAL_MS,
                testClock
        ).apply().toBlocking().first();

        EntityHolder rootWithTimeout = modelActionHolders.get(0).getAction().apply(initialRoot).get().getLeft();
        assertThat(TaskTimeoutChangeActions.getTimeoutStatus(rootWithTimeout.getChildren().first(), testClock)).isEqualTo(TimeoutStatus.Pending);

        // Advance time to trigger timeout
        testClock.advanceTime(DEADLINE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(TaskTimeoutChangeActions.getTimeoutStatus(rootWithTimeout.getChildren().first(), testClock)).isEqualTo(TimeoutStatus.TimedOut);
    }

    private EntityHolder rootFrom(Job<BatchJobExt> job, BatchJobTask task) {
        return EntityHolder.newRoot(job.getId(), job).addChild(EntityHolder.newRoot(task.getId(), task));
    }

    private BatchJobTask createTaskInState(TaskState taskState) {
        return BatchJobTask.newBuilder(task).withStatus(TaskStatus.newBuilder().withState(taskState).build()).build();
    }
}