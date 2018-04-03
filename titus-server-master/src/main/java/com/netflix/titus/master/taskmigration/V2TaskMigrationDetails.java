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

package com.netflix.titus.master.taskmigration;

import java.util.List;
import java.util.Map;

import com.netflix.fenzo.TaskRequest;
import com.netflix.titus.api.model.MigrationPolicy;
import com.netflix.titus.api.model.v2.V2JobDefinition;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.model.v2.WorkerNaming;
import com.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import com.netflix.titus.api.model.v2.parameter.Parameter;
import com.netflix.titus.api.model.v2.parameter.Parameters;
import com.netflix.titus.api.store.v2.InvalidJobException;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.master.job.V2JobMgrIntf;

/**
 * {@link V2TaskMigrationDetails} is responsible for making it easier to gather information required to perform a task
 * migration orchestrated by {@link TaskMigrator} and handled by {@link TaskMigrationManager}.
 */
public class V2TaskMigrationDetails implements TaskMigrationDetails {

    private final TaskRequest taskRequest;
    private final V2JobMgrIntf jobManager;
    private final long createdTimeMillis;

    public V2TaskMigrationDetails(TaskRequest taskRequest, V2JobMgrIntf jobManager) {
        this.taskRequest = taskRequest;
        this.jobManager = jobManager;
        this.createdTimeMillis = System.currentTimeMillis();
    }

    @Override
    public String getId() {
        return taskRequest.getId();
    }

    @Override
    public String getJobId() {
        return jobManager.getJobId();
    }

    private TaskRequest getTaskRequest() {
        return taskRequest;
    }

    public V2JobMgrIntf getJobManager() {
        return jobManager;
    }

    @Override
    public long getCreatedTimeMillis() {
        return createdTimeMillis;
    }

    private V2JobDefinition getJobDefinition() {
        return jobManager.getJobDefinition();
    }

    private StageSchedulingInfo getSchedulingInfo() {
        return jobManager.getJobDefinition().getSchedulingInfo().getStages().values().iterator().next();
    }

    @Override
    public int getNumberOfInstances() {
        return getSchedulingInfo().getNumberOfInstances();
    }

    private List<Parameter> getJobParameters() {
        return getJobDefinition().getParameters();
    }

    @Override
    public boolean isService() {
        return Parameters.getJobType(getJobParameters()) == Parameters.JobType.Service;
    }

    @Override
    public String getApplicationName() {
        return Parameters.getAppName(getJobParameters());
    }

    @Override
    public String getStack() {
        return Parameters.getJobGroupStack(getJobParameters());
    }

    @Override
    public String getDetail() {
        return Parameters.getJobGroupDetail(getJobParameters());
    }

    @Override
    public String getSequence() {
        return Parameters.getJobGroupSeq(getJobParameters());
    }

    @Override
    public String getImageName() {
        return Parameters.getImageName(getJobParameters());
    }

    @Override
    public String getImageVersion() {
        return Parameters.getVersion(getJobParameters());
    }

    public Map<String, String> getLabels() {
        return Parameters.getLabels(getJobParameters());
    }

    @Override
    public String getAttribute(String key) {
        return getLabels().get(key);
    }

    public MigrationPolicy getMigrationPolicy() {
        return Parameters.getMigrationPolicy(getJobParameters());
    }

    private V2JobState getState() {
        V2WorkerMetadata workerMetadata = getWorkerMetadata();
        if (workerMetadata != null) {
            return workerMetadata.getState();
        }
        return null;
    }

    public V2WorkerMetadata getWorkerMetadata() {
        V2JobMetadata jobMetadata = jobManager.getJobMetadata();
        WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(taskRequest.getId());
        try {
            return jobMetadata.getWorkerByNumber(jobAndWorkerId.workerNumber);
        } catch (InvalidJobException ignored) {
        }
        return null;
    }

    @Override
    public boolean isActive() {
        try {
            if (taskRequest != null && jobManager != null) {
                return !V2JobState.isTerminalState(getState());
            }
        } catch (Exception ignored) {
        }
        return false;
    }

    public long getMigrationDeadline() {
        V2WorkerMetadata workerMetadata = getWorkerMetadata();
        if (workerMetadata != null) {
            return workerMetadata.getMigrationDeadline();
        }

        return 0;
    }

    public void setMigrationDeadline(long migrationDeadline) {
        jobManager.setMigrationDeadline(getId(), migrationDeadline);
    }

    @Override
    public String getHostInstanceId() {
        V2WorkerMetadata workerMetadata = getWorkerMetadata();
        String hostInstanceId = "";
        if (workerMetadata != null) {
            hostInstanceId = workerMetadata.getSlaveAttributes().getOrDefault("id", "");
        }

        return hostInstanceId;
    }
}
