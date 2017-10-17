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

package io.netflix.titus.master.taskmigration;

import java.util.Optional;

import io.netflix.titus.api.jobmanager.model.job.Image;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.common.util.tuple.Pair;

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
        return getTask().getStatus().getState() != TaskState.Finished;
    }

    public Job<?> getJob() {
        Optional<Job<?>> jobOpt = v3JobOperations.getJob(jobId);
        if (!jobOpt.isPresent()) {
            throw new IllegalStateException("jobId: " + jobId + " was not present");
        }
        return jobOpt.get();
    }

    public Task getTask() {
        Optional<Pair<Job<?>, Task>> taskByIdOpt = v3JobOperations.findTaskById(taskId);
        if (!taskByIdOpt.isPresent()) {
            throw new IllegalStateException("taskId: " + taskId + " was not present");
        }
        Pair<Job<?>, Task> jobTaskPair = taskByIdOpt.get();
        return jobTaskPair.getRight();
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
