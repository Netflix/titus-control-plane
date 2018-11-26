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

package com.netflix.titus.gateway.service.v3.internal;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.gateway.service.v3.JobManagerConfiguration;
import com.netflix.titus.grpc.protogen.MigrationDetails;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.relocation.RelocationServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

@Singleton
class TaskRelocationDataInjector {

    private static final Logger logger = LoggerFactory.getLogger(TaskRelocationDataInjector.class);

    private final GrpcClientConfiguration configuration;
    private final JobManagerConfiguration jobManagerConfiguration;
    private final FeatureActivationConfiguration featureActivationConfiguration;
    private final RelocationServiceClient relocationServiceClient;
    private final Scheduler scheduler;

    @Inject
    TaskRelocationDataInjector(
            GrpcClientConfiguration configuration,
            JobManagerConfiguration jobManagerConfiguration,
            FeatureActivationConfiguration featureActivationConfiguration,
            RelocationServiceClient relocationServiceClient) {
        this(configuration, jobManagerConfiguration, featureActivationConfiguration, relocationServiceClient, Schedulers.computation());

    }

    @VisibleForTesting
    TaskRelocationDataInjector(
            GrpcClientConfiguration configuration,
            JobManagerConfiguration jobManagerConfiguration,
            FeatureActivationConfiguration featureActivationConfiguration,
            RelocationServiceClient relocationServiceClient,
            Scheduler scheduler) {
        this.configuration = configuration;
        this.jobManagerConfiguration = jobManagerConfiguration;
        this.featureActivationConfiguration = featureActivationConfiguration;
        this.relocationServiceClient = relocationServiceClient;
        this.scheduler = scheduler;
    }

    Observable<Task> injectIntoTask(String taskId, Observable<Task> taskObservable) {
        if (!featureActivationConfiguration.isMergeTaskMigrationPlanInGateway()) {
            return taskObservable;
        }

        Observable<Optional<TaskRelocationPlan>> relocationPlanResolver = ReactorExt.toObservable(relocationServiceClient.findTaskRelocationPlan(taskId))
                .timeout(getTaskRelocationTimeout(), TimeUnit.MILLISECONDS, scheduler)
                .doOnError(error -> logger.info("Could not resolve task relocation status for task: taskId={}, error={}", taskId, ExceptionExt.toMessageChain(error)))
                .onErrorReturn(e -> Optional.empty());

        return Observable.zip(
                taskObservable,
                relocationPlanResolver,
                (task, planOpt) -> planOpt.map(plan -> newTaskWithRelocationPlan(task, plan)).orElse(task)
        );
    }

    Observable<TaskQueryResult> injectIntoTaskQueryResult(Observable<TaskQueryResult> tasksObservable) {
        if (!featureActivationConfiguration.isMergeTaskMigrationPlanInGateway()) {
            return tasksObservable;
        }

        return tasksObservable.flatMap(queryResult -> {
            Set<String> taskIds = queryResult.getItemsList().stream().map(Task::getId).collect(Collectors.toSet());

            return ReactorExt.toObservable(relocationServiceClient.findTaskRelocationPlans(taskIds))
                    .timeout(getTaskRelocationTimeout(), TimeUnit.MILLISECONDS, scheduler)
                    .doOnError(error -> logger.info("Could not resolve task relocation status for tasks: taskIds={}, error={}", taskIds, ExceptionExt.toMessageChain(error)))
                    .onErrorReturn(e -> Collections.emptyList())
                    .map(relocationPlans -> {
                        Map<String, TaskRelocationPlan> plansById = relocationPlans.stream().collect(Collectors.toMap(TaskRelocationPlan::getTaskId, p -> p));
                        if (plansById.isEmpty()) {
                            return queryResult;
                        }
                        List<Task> newTaskList = queryResult.getItemsList().stream()
                                .map(task -> {
                                    TaskRelocationPlan plan = plansById.get(task.getId());
                                    return plan != null ? newTaskWithRelocationPlan(task, plan) : task;
                                })
                                .collect(Collectors.toList());
                        return queryResult.toBuilder().clearItems().addAllItems(newTaskList).build();
                    });
        });
    }

    private long getTaskRelocationTimeout() {
        return (long) (configuration.getRequestTimeout() * jobManagerConfiguration.getRelocationTimeoutCoefficient());
    }

    private Task newTaskWithRelocationPlan(Task task, TaskRelocationPlan relocationPlan) {
        // If already set, assume this comes from the legacy task migration
        if (task.getMigrationDetails().getNeedsMigration()) {
            return task;
        }

        if (relocationPlan.getRelocationTime() <= 0) {
            return task;
        }
        return task.toBuilder().setMigrationDetails(
                MigrationDetails.newBuilder()
                        .setNeedsMigration(true)
                        .setDeadline(relocationPlan.getRelocationTime())
                        .build()
        ).build();
    }
}
