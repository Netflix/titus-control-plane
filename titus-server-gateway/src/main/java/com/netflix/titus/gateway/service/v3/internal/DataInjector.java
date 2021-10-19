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
import com.netflix.titus.grpc.protogen.MigrationDetails;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.kubernetes.Fabric8IOClients;
import com.netflix.titus.runtime.connector.relocation.RelocationDataReplicator;
import com.netflix.titus.runtime.connector.relocation.RelocationServiceClient;
import com.netflix.titus.runtime.jobmanager.JobManagerConfiguration;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

@Singleton
class DataInjector {

    private static final Logger logger = LoggerFactory.getLogger(DataInjector.class);

    /**
     * We can tolerate task relocation cache staleness up to 60sec. This should be ok, as the relocation service
     * itself runs on 30sec plan refresh interval.
     */
    private static final long MAX_RELOCATION_DATA_STALENESS_MS = 30_000;

    private final GrpcClientConfiguration configuration;
    private final JobManagerConfiguration jobManagerConfiguration;
    private final FeatureActivationConfiguration featureActivationConfiguration;
    private final RelocationServiceClient relocationServiceClient;
    private final RelocationDataReplicator relocationDataReplicator;
    private final Scheduler scheduler;
    private final NamespacedKubernetesClient namespacedKubernetesClient;
    /*getFabric8IOClient() {
        return Fabric8IOClients.mustHaveKubeConnectivity(
                Fabric8IOClients.createFabric8IOClient()
        );
    }*/

    @Inject
    DataInjector(
            GrpcClientConfiguration configuration,
            JobManagerConfiguration jobManagerConfiguration,
            FeatureActivationConfiguration featureActivationConfiguration,
            RelocationServiceClient relocationServiceClient,
            RelocationDataReplicator relocationDataReplicator,
            NamespacedKubernetesClient namespacedKubernetesClient) {
        this(configuration, jobManagerConfiguration, featureActivationConfiguration, relocationServiceClient, relocationDataReplicator,
                Schedulers.computation(), namespacedKubernetesClient);
    }


    @VisibleForTesting
    DataInjector(
            GrpcClientConfiguration configuration,
            JobManagerConfiguration jobManagerConfiguration,
            FeatureActivationConfiguration featureActivationConfiguration,
            RelocationServiceClient relocationServiceClient,
            RelocationDataReplicator relocationDataReplicator,
            Scheduler scheduler, NamespacedKubernetesClient namespacedKubernetesClient) {
        this.configuration = configuration;
        this.jobManagerConfiguration = jobManagerConfiguration;
        this.featureActivationConfiguration = featureActivationConfiguration;
        this.relocationServiceClient = relocationServiceClient;
        this.relocationDataReplicator = relocationDataReplicator;
        this.scheduler = scheduler;
        this.namespacedKubernetesClient = namespacedKubernetesClient;
    }

    Observable<Task> injectIntoTask(String taskId, Observable<Task> taskObservable) {
        if (!featureActivationConfiguration.isMergingTaskMigrationPlanInGatewayEnabled()) {
            return taskObservable;
        }

        if (shouldUseRelocationCache()) {
            return taskObservable.map(task -> newTaskWithRelocationPlan(task, relocationDataReplicator.getCurrent().getPlans().get(taskId)));
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
        if (!featureActivationConfiguration.isMergingTaskMigrationPlanInGatewayEnabled()) {
            return tasksObservable;
        }

        return tasksObservable.flatMap(queryResult -> {
            Set<String> taskIds = queryResult.getItemsList().stream().map(Task::getId).collect(Collectors.toSet());

            if (shouldUseRelocationCache()) {
                Map<String, TaskRelocationPlan> plans = relocationDataReplicator.getCurrent().getPlans();
                List<Task> newTaskList = queryResult.getItemsList().stream()
                        .map(task -> {
                            TaskRelocationPlan plan = plans.get(task.getId());
                            return plan != null ? newTaskWithRelocationPlan(task, plan) : task;
                        })
                        .collect(Collectors.toList());
                return Observable.just(queryResult.toBuilder().clearItems().addAllItems(newTaskList).build());
            }

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

    private boolean shouldUseRelocationCache() {
        return jobManagerConfiguration.isUseRelocationCache() && relocationDataReplicator.getStalenessMs() < MAX_RELOCATION_DATA_STALENESS_MS;
    }

    private long getTaskRelocationTimeout() {
        return (long) (configuration.getRequestTimeout() * jobManagerConfiguration.getRelocationTimeoutCoefficient());
    }

    static Task newTaskWithRelocationPlan(Task task, TaskRelocationPlan relocationPlan) {
        if(relocationPlan == null) {
            return task;
        }

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
                        .setStarted(relocationPlan.getDecisionTime())
                        .setDeadline(relocationPlan.getRelocationTime())
                        .build()
        ).build();
    }
}
