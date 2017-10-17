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

package io.netflix.titus.testkit.perf.load.job;

import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobState;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import org.junit.Test;

import static io.netflix.titus.testkit.perf.load.job.JobChangeEvent.onJobFinished;
import static io.netflix.titus.testkit.perf.load.job.JobChangeEvent.onTaskStateChange;
import static org.assertj.core.api.Assertions.assertThat;

public class JobReconcilerTest {

    private final TitusV2ModelGenerator generator = new TitusV2ModelGenerator();

    @Test
    public void testSingleTaskBatchJob() throws Exception {
        JobReconciler reconciler = new JobReconciler(1);

        String jobId = generator.newJobInfo(TitusJobType.batch, "myJob").getId();
        TitusJobInfo jobInfo = generator.scheduleJob(jobId);

        // Initial job schedule
        assertThat(reconciler.jobSubmitted(jobId)).containsExactly(JobChangeEvent.onJobSubmit(jobId));
        TaskInfo task0 = jobInfo.getTasks().get(0);
        assertThat(reconciler.reconcile(jobInfo)).containsExactly(
                JobChangeEvent.onTaskCreate(jobId, task0)
        );

        // Task state change
        generator.moveWorkerToState(jobId, task0.getId(), TitusTaskState.RUNNING);
        jobInfo = generator.getJob(jobId);
        assertThat(reconciler.reconcile(jobInfo)).containsExactly(
                onTaskStateChange(jobId, task0.getId(), TitusTaskState.RUNNING)
        );

        // Task and job termination
        generator.moveWorkerToState(jobId, task0.getId(), TitusTaskState.FINISHED);
        generator.moveJobToState(jobId, TitusJobState.FINISHED);
        jobInfo = generator.getJob(jobId);

        assertThat(reconciler.jobFinished(jobInfo)).containsExactly(
                onTaskStateChange(jobId, task0.getId(), TitusTaskState.FINISHED),
                onJobFinished(jobId, TitusJobState.FINISHED)
        );
    }

    @Test
    public void testServiceJobWithTerminatingTasks() throws Exception {
        JobReconciler reconciler = new JobReconciler(2);

        TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "myJob"))
                .instancesDesired(2)
                .build();
        String jobId = generator.newJobInfo(jobSpec).getId();
        TitusJobInfo jobInfo = generator.scheduleJob(jobId);

        // Initial job schedule
        assertThat(reconciler.jobSubmitted(jobId)).containsExactly(JobChangeEvent.onJobSubmit(jobId));
        TaskInfo task0 = jobInfo.getTasks().get(0);
        TaskInfo task1 = jobInfo.getTasks().get(1);
        assertThat(reconciler.reconcile(jobInfo)).containsExactlyInAnyOrder(
                JobChangeEvent.onTaskCreate(jobId, task0),
                JobChangeEvent.onTaskCreate(jobId, task1)
        );

        // Finish a task, and provide a replacement
        TaskInfo task0r = generator.replaceTask(jobId, task0.getId(), TitusTaskState.FINISHED);
        jobInfo = generator.getJob(jobId);
        assertThat(reconciler.reconcile(jobInfo)).containsExactly(
                JobChangeEvent.onTaskStateChange(jobId, task0.getId(), TitusTaskState.FINISHED),
                JobChangeEvent.onTaskCreate(jobId, task0r)
        );
    }
}
