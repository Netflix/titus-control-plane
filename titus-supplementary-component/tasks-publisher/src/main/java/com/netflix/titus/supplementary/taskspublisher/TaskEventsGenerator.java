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
package com.netflix.titus.supplementary.taskspublisher;

import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.supplementary.taskspublisher.es.ElasticSearchUtils;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class TaskEventsGenerator {

    private final Map<String, String> taskDocumentBaseContext;
    private TitusClient titusClient;
    private ConnectableFlux<TaskDocument> taskEvents;

    public TaskEventsGenerator(TitusClient titusClient,
                               Map<String, String> taskDocumentBaseContext) {
        this.titusClient = titusClient;
        this.taskDocumentBaseContext = taskDocumentBaseContext;
        buildEventStream();
    }

    public ConnectableFlux<TaskDocument> getTaskEvents() {
        return taskEvents;
    }

    private void buildEventStream() {
        taskEvents = titusClient.getJobAndTaskUpdates()
                .publishOn(Schedulers.elastic())
                .flatMap(jobOrTaskUpdate -> jobOrTaskUpdate.hasTask() ? Flux.just(jobOrTaskUpdate.getTask()) : Flux.empty())
                .map(task -> {
                    final Mono<Job> jobById = titusClient.getJobById(task.getJobId());
                    return Pair.of(task, jobById);
                })
                .flatMap(taskMonoPair -> {
                    final Task task = taskMonoPair.getLeft();
                    return taskMonoPair.getRight()
                            .map(job -> {
                                final com.netflix.titus.api.jobmanager.model.job.Job coreJob = GrpcJobManagementModelConverters.toCoreJob(job);
                                final com.netflix.titus.api.jobmanager.model.job.Task coreTask = GrpcJobManagementModelConverters.toCoreTask(coreJob, task);
                                return TaskDocument.fromV3Task(coreTask, coreJob, ElasticSearchUtils.DATE_FORMAT, buildTaskContext(task));
                            }).flux();
                })
                .retryWhen(TaskPublisherRetryUtil.buildRetryHandler(TaskPublisherRetryUtil.INITIAL_RETRY_DELAY_MS,
                        TaskPublisherRetryUtil.MAX_RETRY_DELAY_MS, -1))
                .publish();
    }

    private Map<String, String> buildTaskContext(Task task) {
        String stack = "";
        if (task.getTaskContextMap().containsKey(JobAttributes.JOB_ATTRIBUTES_CELL)) {
            stack = task.getTaskContextMap().get(JobAttributes.JOB_ATTRIBUTES_CELL);
        }
        final HashMap<String, String> taskContext = new HashMap<>(taskDocumentBaseContext);
        taskContext.put("stack", stack);
        return taskContext;
    }
}
