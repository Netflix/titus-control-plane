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

package com.netflix.titus.master.job;

import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.model.v2.JobCompletedReason;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.model.v2.parameter.Parameters;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.testkit.model.runtime.RuntimeModelGenerator;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.common.util.AwaitExt.awaitUntil;
import static com.netflix.titus.common.util.CollectionsExt.first;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExcludedAgentsTrackerTest {

    private final RuntimeModelGenerator generator = new RuntimeModelGenerator(getClass().getSimpleName());

    private final JobManagerConfiguration configuration = mock(JobManagerConfiguration.class);
    private final ExcludedAgentsTracker tracker = new ExcludedAgentsTracker("jobId#1", "myApp", configuration, new DefaultRegistry());

    @Before
    public void setUp() throws Exception {
        when(configuration.getAgentDisableTimeMs()).thenReturn(10L);
        when(configuration.isEnableAgentTracking()).thenReturn(true);
    }

    @Test
    public void testImmediatelyFailingTaskExcludesAgent() throws Exception {
        // Fail task
        V2WorkerMetadata failedTask = createTask();
        tracker.update(failedTask, V2JobState.Failed, JobCompletedReason.Error);
        assertThat(tracker.getExcludedAgents()).contains(failedTask.getSlave());

        // Check that agent is enabled again after disable time passes
        awaitUntil(() -> tracker.getExcludedAgents().isEmpty(), 30, TimeUnit.SECONDS);
        assertThat(tracker.getExcludedAgents()).isEmpty();
    }

    private V2WorkerMetadata createTask() {
        String jobId = generator.newJobMetadata(Parameters.JobType.Batch, "myApp").getJobId();
        V2JobMetadata job = generator.scheduleJob(jobId);
        V2WorkerMetadata task0 = first(job.getStageMetadata(1).getAllWorkers());
        generator.moveWorkerToState(jobId, task0.getWorkerIndex(), V2JobState.Started);
        return task0;
    }
}