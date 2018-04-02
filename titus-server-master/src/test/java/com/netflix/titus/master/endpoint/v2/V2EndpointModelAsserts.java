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

package com.netflix.titus.master.endpoint.v2;

import java.util.List;
import java.util.Set;

import com.netflix.titus.master.endpoint.EndpointModelAsserts;
import com.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import com.netflix.titus.master.endpoint.EndpointModelAsserts;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static org.assertj.core.api.Assertions.assertThat;

class V2EndpointModelAsserts implements EndpointModelAsserts<
        String, TitusJobSpec, TitusJobType, TitusJobInfo, TitusTaskInfo, TitusTaskState> {

    private static final Set<TitusTaskState> TASK_TERMINAL_STATES = asSet(
            TitusTaskState.FINISHED, TitusTaskState.ERROR, TitusTaskState.STOPPED, TitusTaskState.CRASHED, TitusTaskState.FAILED
    );

    @Override
    public void assertJobId(TitusJobInfo actualJob, String jobId) {
        assertThat(actualJob.getId()).isEqualTo(jobId);
    }

    @Override
    public void assertSpecOfJob(TitusJobInfo titusJobInfo, TitusJobSpec titusJobSpec) {
        assertThat(titusJobInfo.getName()).isEqualTo(titusJobSpec.getName());
    }

    @Override
    public void assertCellInfo(TitusJobInfo titusJobInfo, String cellName) {
        assertThat(titusJobInfo.getLabels()).containsEntry("titus.cell", cellName);
        assertThat(titusJobInfo.getLabels()).containsEntry("titus.stack", cellName);
        final List<TaskInfo> tasks = titusJobInfo.getTasks();
        for (TaskInfo taskInfo : tasks) {
            assertThat(taskInfo.getCell()).isEqualTo(cellName);
        }
    }

    @Override
    public void assertTaskCell(TitusTaskInfo task, String cellName) {
        assertThat(task.getCell()).isEqualTo(cellName);
    }

    @Override
    public void assertJobKilled(TitusJobInfo titusJobInfo) {
        for (TaskInfo task : titusJobInfo.getTasks()) {
            assertThat(task.getState()).isIn(TASK_TERMINAL_STATES);
        }
    }

    @Override
    public void assertServiceJobSize(TitusJobInfo job, int desired, int min, int max) {
        assertThat(job.getInstances()).isEqualTo(desired);
        assertThat(job.getInstancesDesired()).isEqualTo(desired);
        assertThat(job.getInstancesMin()).isEqualTo(min);
        assertThat(job.getInstancesMax()).isEqualTo(max);
    }

    @Override
    public void assertJobInService(TitusJobInfo titusJobInfo, boolean inService) {
        assertThat(titusJobInfo.isInService()).isEqualTo(inService);
    }
}
