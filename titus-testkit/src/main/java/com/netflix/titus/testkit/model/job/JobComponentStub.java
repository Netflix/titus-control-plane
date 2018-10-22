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

package com.netflix.titus.testkit.model.job;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.data.generator.MutableDataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import rx.Observable;

public class JobComponentStub {

    private static final JobChangeNotification GRPC_SNAPSHOT_MARKER = JobChangeNotification.newBuilder()
            .setSnapshotEnd(JobChangeNotification.SnapshotEnd.getDefaultInstance())
            .build();

    private final Clock clock;

    private final StubbedJobData stubJobData;
    private final StubbedJobOperations stubbedJobOperations;

    private final MutableDataGenerator<Job> jobGenerator;
    private final Map<String, MutableDataGenerator<JobDescriptor>> jobTemplates = new HashMap<>();

    public JobComponentStub(TitusRuntime titusRuntime) {
        this.stubJobData = new StubbedJobData(titusRuntime);
        this.stubbedJobOperations = new StubbedJobOperations(stubJobData);
        this.jobGenerator = new MutableDataGenerator<>(JobGenerator.jobs(titusRuntime.getClock()));
        this.clock = titusRuntime.getClock();
    }

    public V3JobOperations getJobOperations() {
        return stubbedJobOperations;
    }

    public JobComponentStub addJobTemplate(String templateId, DataGenerator<JobDescriptor> jobDescriptorGenerator) {
        jobTemplates.put(templateId, new MutableDataGenerator<>(jobDescriptorGenerator));
        return this;
    }

    public JobComponentStub addBatchTemplate(String templateId, DataGenerator<JobDescriptor<BatchJobExt>> jobDescriptorGenerator) {
        return addJobTemplate(templateId, (DataGenerator) jobDescriptorGenerator);
    }

    public Job createJob(String templateId) {
        Job job = jobGenerator.getValue().toBuilder().withJobDescriptor(jobTemplates.get(templateId).getValue()).build();
        return createJob(job);
    }

    public Job createJob(Job<?> job) {
        stubJobData.addJob(job);
        return job;
    }

    public List<Task> createDesiredTasks(Job<?> job) {
        return stubJobData.createDesiredTasks(job);
    }

    public Pair<Job, List<Task>> createJobAndTasks(String templateId) {
        Job job = createJob(templateId);
        List<Task> tasks = createDesiredTasks(job);
        return Pair.of(job, tasks);
    }

    public Pair<Job, List<Task>> createJobAndTasks(Job<?> job) {
        createJob(job);
        List<Task> tasks = createDesiredTasks(job);
        return Pair.of(job, tasks);
    }

    public Job createJobAndTasks(String templateId, BiConsumer<Job, List<Task>> processor) {
        Pair<Job, List<Task>> pair = createJobAndTasks(templateId);
        processor.accept(pair.getLeft(), pair.getRight());
        return pair.getLeft();
    }

    public List<Pair<Job, List<Task>>> creteMultipleJobsAndTasks(String... templateIds) {
        List<Pair<Job, List<Task>>> result = new ArrayList<>();
        for (String templateId : templateIds) {
            result.add(createJobAndTasks(templateId));
        }
        return result;
    }

    public Job moveJobToKillInitiatedState(Job job) {
        return stubJobData.moveJobToKillInitiatedState(job);
    }

    public Job finishJob(Job job) {
        return stubJobData.finishJob(job);
    }

    public Task moveTaskToState(String taskId, TaskState newState) {
        Pair<Job<?>, Task> jobTaskPair = stubbedJobOperations.findTaskById(taskId).orElseThrow(() -> new IllegalArgumentException("Task not found: " + taskId));
        return moveTaskToState(jobTaskPair.getRight(), newState);
    }

    public Task moveTaskToState(Task task, TaskState newState) {
        return stubJobData.moveTaskToState(task, newState);
    }

    public void place(String taskId, AgentInstance agentInstance) {
        stubJobData.changeTask(taskId, task ->
                task.toBuilder()
                        .withStatus(TaskStatus.newBuilder()
                                .withState(TaskState.Started)
                                .withReasonCode("placed")
                                .withReasonMessage("Task placed on agent")
                                .withTimestamp(clock.wallTime())
                                .build()
                        )
                        .addToTaskContext(TaskAttributes.TASK_ATTRIBUTES_AGENT_ID, agentInstance.getId())
                        .addToTaskContext(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, agentInstance.getId())
                        .addToTaskContext(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST, agentInstance.getIpAddress())
                        .build());
    }

    public void forget(Task task) {
        stubJobData.removeTask(task, false);
    }

    public Observable<JobManagerEvent<?>> observeJobs(boolean snapshot) {
        return stubJobData.events(snapshot);
    }

    public Observable<JobChangeNotification> grpcObserveJobs(boolean snapshot) {
        return observeJobs(snapshot).map(coreEvent -> {
            if (coreEvent == JobManagerEvent.snapshotMarker()) {
                return GRPC_SNAPSHOT_MARKER;
            }
            return V3GrpcModelConverters.toGrpcJobChangeNotification(coreEvent, EmptyLogStorageInfo.empty());
        });
    }
}
