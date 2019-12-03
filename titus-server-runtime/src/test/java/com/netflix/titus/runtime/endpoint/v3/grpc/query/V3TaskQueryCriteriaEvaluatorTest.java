/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.endpoint.v3.grpc.query;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class V3TaskQueryCriteriaEvaluatorTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    @Test
    public void testTaskWithSystemErrorFilter() {
        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria = JobQueryCriteria.<TaskStatus.TaskState, JobDescriptor.JobSpecCase>newBuilder()
                .withSkipSystemFailures(true)
                .build();
        V3TaskQueryCriteriaEvaluator evaluator = new V3TaskQueryCriteriaEvaluator(criteria, titusRuntime);

        Job<?> job = JobGenerator.oneBatchJob();

        // Check non finished task
        Task task = JobGenerator.oneBatchTask();
        assertThat(evaluator.test(Pair.of(job, task))).isTrue();

        // Check finished task with non system error reason code
        Task finishedTask = task.toBuilder().withStatus(com.netflix.titus.api.jobmanager.model.job.TaskStatus.newBuilder()
                .withState(TaskState.Finished)
                .withReasonCode(com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL)
                .build()
        ).build();
        assertThat(evaluator.test(Pair.of(job, finishedTask))).isTrue();

        // Check finish that with system error
        Task taskWithSystemError = task.toBuilder().withStatus(com.netflix.titus.api.jobmanager.model.job.TaskStatus.newBuilder()
                .withState(TaskState.Finished)
                .withReasonCode(com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_LOCAL_SYSTEM_ERROR)
                .build()
        ).build();
        assertThat(evaluator.test(Pair.of(job, taskWithSystemError))).isFalse();
    }
}