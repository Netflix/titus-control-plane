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

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.migration.MigrationDetails;
import com.netflix.titus.api.jobmanager.model.job.migration.SelfManagedMigrationPolicy;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.common.util.tuple.Pair;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Launched;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.StartInitiated;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Started;

public class V3TaskMigrationDetails implements TaskMigrationDetails {

    private final String jobId;
    private final String taskId;
    private final V3JobOperations v3JobOperations;
    private final long createdTimeMillis;

    public V3TaskMigrationDetails(Job<?> job, Task task, V3JobOperations v3JobOperations) {
        this.jobId = job.getId();
        this.taskId = task.getId();
        this.v3JobOperations = v3JobOperations;
        this.createdTimeMillis = System.currentTimeMillis();
    }

    @Override
    public String getId() {
        return taskId;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public long getCreatedTimeMillis() {
        return createdTimeMillis;
    }

    @Override
    public int getNumberOfInstances() {
        return v3JobOperations.getTasks(getJobId()).size();
    }

    @Override
    public boolean isService() {
        return getJobDescriptor().getExtensions() instanceof ServiceJobExt;
    }

    @Override
    public String getApplicationName() {
        return getJobDescriptor().getApplicationName();
    }

    @Override
    public String getStack() {
        return getJobDescriptor().getJobGroupInfo().getStack();
    }

    @Override
    public String getDetail() {
        return getJobDescriptor().getJobGroupInfo().getDetail();
    }

    @Override
    public String getSequence() {
        return getJobDescriptor().getJobGroupInfo().getSequence();
    }

    @Override
    public String getImageName() {
        return getJobDescriptor().getContainer().getImage().getName();
    }

    @Override
    public String getImageVersion() {
        return getImage().getTag();
    }

    @Override
    public String getAttribute(String key) {
        return getJobDescriptor().getAttributes().get(key);
    }

    @Override
    public boolean isActive() {
        Optional<Pair<Job<?>, Task>> jobTaskPair = v3JobOperations.findTaskById(taskId);
        if (jobTaskPair.isPresent()) {
            TaskState state = jobTaskPair.get().getRight().getStatus().getState();
            return state == Launched || state == StartInitiated || state == Started;
        }
        return false;
    }

    @Override
    public long getMigrationDeadline() {
        Task task = getTask();
        if (task instanceof ServiceJobTask) {
            ServiceJobTask serviceTask = (ServiceJobTask) task;
            MigrationDetails migrationDetails = serviceTask.getMigrationDetails();
            if (migrationDetails != null) {
                return migrationDetails.getDeadline();
            }
        }
        return 0;
    }

    @Override
    public void setMigrationDeadline(long migrationDeadline) {
        Task task = getTask();
        if (task instanceof ServiceJobTask) {
            MigrationDetails migrationDetails = ((ServiceJobTask) task).getMigrationDetails();
            if (migrationDetails != null && migrationDetails.getDeadline() == 0) {
                v3JobOperations.updateTask(taskId, t -> {
                    if (t instanceof ServiceJobTask) {
                        ServiceJobTask serviceTask = (ServiceJobTask) t;
                        MigrationDetails newMigrationDetails = MigrationDetails.newBuilder()
                                .withNeedsMigration(true)
                                .withDeadline(migrationDeadline)
                                .build();
                        return Optional.of(serviceTask.toBuilder().withMigrationDetails(newMigrationDetails).build());
                    }
                    return Optional.empty();
                }, Trigger.TaskMigration, "Updating migration details").await(30_000, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public boolean isSelfManaged() {
        if (isService()) {
            ServiceJobExt extensions = (ServiceJobExt) getJobDescriptor().getExtensions();
            return (extensions.getMigrationPolicy() instanceof SelfManagedMigrationPolicy);
        }
        return false;
    }

    @Override
    public String getHostInstanceId() {
        return getTask().getTaskContext().getOrDefault(TASK_ATTRIBUTES_AGENT_INSTANCE_ID, "");
    }

    public Job<?> getJob() {
        return v3JobOperations.getJob(jobId).orElseThrow(() -> JobManagerException.jobNotFound(jobId));
    }

    public Task getTask() {
        return v3JobOperations.findTaskById(taskId).map(Pair::getRight).orElseThrow(() -> JobManagerException.taskNotFound(taskId));
    }

    public V3JobOperations getV3JobOperations() {
        return v3JobOperations;
    }

    private JobDescriptor<?> getJobDescriptor() {
        return getJob().getJobDescriptor();
    }

    private Image getImage() {
        return getJob().getJobDescriptor().getContainer().getImage();
    }
}
