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

import java.util.ArrayList;
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
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.grpc.protogen.BasicImage;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.MigrationDetails;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.kubernetes.fabric8io.Fabric8IOConnector;
import com.netflix.titus.runtime.connector.relocation.RelocationDataReplicator;
import com.netflix.titus.runtime.connector.relocation.RelocationServiceClient;
import com.netflix.titus.runtime.jobmanager.JobManagerConfiguration;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.ANNOTATION_KEY_IMAGE_TAG_PREFIX;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.ANNOTATION_KEY_SUFFIX_CONTAINERS;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.ANNOTATION_KEY_SUFFIX_CONTAINERS_SIDECAR;

@Singleton
class TaskDataInjector {

    private static final Logger logger = LoggerFactory.getLogger(TaskDataInjector.class);

    /**
     * We can tolerate task relocation cache staleness up to 60sec. This should be ok, as the relocation service
     * itself runs on 30sec plan refresh interval.
     */
    private static final long MAX_RELOCATION_DATA_STALENESS_MS = 30_000;

    private static Fabric8IOConnector kubeApiConnector;

    private final GrpcClientConfiguration configuration;
    private final JobManagerConfiguration jobManagerConfiguration;
    private final FeatureActivationConfiguration featureActivationConfiguration;
    private final RelocationServiceClient relocationServiceClient;
    private final RelocationDataReplicator relocationDataReplicator;
    private final Scheduler scheduler;

    @Inject
    TaskDataInjector(
            GrpcClientConfiguration configuration,
            JobManagerConfiguration jobManagerConfiguration,
            FeatureActivationConfiguration featureActivationConfiguration,
            RelocationServiceClient relocationServiceClient,
            RelocationDataReplicator relocationDataReplicator,
            Fabric8IOConnector kubeApiConnector) {
        this(configuration, jobManagerConfiguration, featureActivationConfiguration, relocationServiceClient,
                relocationDataReplicator, kubeApiConnector, Schedulers.computation());
    }

    @VisibleForTesting
    TaskDataInjector(
            GrpcClientConfiguration configuration,
            JobManagerConfiguration jobManagerConfiguration,
            FeatureActivationConfiguration featureActivationConfiguration,
            RelocationServiceClient relocationServiceClient,
            RelocationDataReplicator relocationDataReplicator,
            Fabric8IOConnector kubeApiConnector,
            Scheduler scheduler) {
        this.configuration = configuration;
        this.jobManagerConfiguration = jobManagerConfiguration;
        this.featureActivationConfiguration = featureActivationConfiguration;
        this.relocationServiceClient = relocationServiceClient;
        this.relocationDataReplicator = relocationDataReplicator;
        this.kubeApiConnector = kubeApiConnector;
        this.scheduler = scheduler;
    }

    JobChangeNotification injectIntoTaskUpdateEvent(JobChangeNotification event) {
        if (event.getNotificationCase() != JobChangeNotification.NotificationCase.TASKUPDATE) {
            return event;
        }

        Task updatedTask = event.getTaskUpdate().getTask();
        if (featureActivationConfiguration.isInjectingContainerStatesEnabled()) {
            updatedTask = newTaskWithContainerState(updatedTask);
        }
        if (featureActivationConfiguration.isMergingTaskMigrationPlanInGatewayEnabled()) {
            updatedTask = newTaskWithRelocationPlan(updatedTask, relocationDataReplicator.getCurrent().getPlans().get(updatedTask.getId()));
        }

        // Nothing changed so return the input event.
        if (updatedTask == event.getTaskUpdate().getTask()) {
            return event;
        }
        return event.toBuilder().setTaskUpdate(
                event.getTaskUpdate().toBuilder().setTask(updatedTask).build()
        ).build();
    }

    Observable<Task> injectIntoTask(String taskId, Observable<Task> taskObservable) {
        Observable<Task> taskObservableWithContainerState = taskObservable;

        if (featureActivationConfiguration.isInjectingContainerStatesEnabled()) {
            taskObservableWithContainerState = taskObservable.map(this::newTaskWithContainerState);
        }

        if (!featureActivationConfiguration.isMergingTaskMigrationPlanInGatewayEnabled()) {
            return taskObservableWithContainerState;
        }

        if (shouldUseRelocationCache()) {
            return taskObservableWithContainerState.map(task -> newTaskWithRelocationPlan(task, relocationDataReplicator.getCurrent().getPlans().get(taskId)));
        }

        Observable<Optional<TaskRelocationPlan>> relocationPlanResolver = ReactorExt.toObservable(relocationServiceClient.findTaskRelocationPlan(taskId))
                .timeout(getTaskRelocationTimeout(), TimeUnit.MILLISECONDS, scheduler)
                .doOnError(error -> logger.info("Could not resolve task relocation status for task: taskId={}, error={}", taskId, ExceptionExt.toMessageChain(error)))
                .onErrorReturn(e -> Optional.empty());

        return Observable.zip(
                taskObservableWithContainerState,
                relocationPlanResolver,
                (task, planOpt) -> planOpt.map(plan -> newTaskWithRelocationPlan(task, plan)).orElse(task)
        );
    }

    Observable<TaskQueryResult> injectIntoTaskQueryResult(Observable<TaskQueryResult> tasksObservable) {
        Observable<TaskQueryResult> tasksObservableWithContainerState = tasksObservable;

        if (featureActivationConfiguration.isInjectingContainerStatesEnabled()) {
            tasksObservableWithContainerState.flatMap(queryResult -> {
                List<Task> newTaskList = queryResult.getItemsList().stream()
                        .map(task -> newTaskWithContainerState(task))
                        .collect(Collectors.toList());
                return Observable.just(queryResult.toBuilder().clearItems().addAllItems(newTaskList).build());
            });
        }

        if (!featureActivationConfiguration.isMergingTaskMigrationPlanInGatewayEnabled()) {
            return tasksObservableWithContainerState;
        }

        return tasksObservableWithContainerState.flatMap(queryResult -> {
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

    private Task newTaskWithContainerState(Task task) {
        if (task.hasStatus()) {
            return task.toBuilder().setStatus(task.getStatus().toBuilder()
                    .addAllContainerState(getContainerState(task.getId()))).build();
        }
        return task.toBuilder().setStatus(TaskStatus.newBuilder()
                .addAllContainerState(getContainerState(task.getId()))).build();
    }

    private List<TaskStatus.ContainerState> getContainerState(String taskId) {
        Map<String, Pod> pods = kubeApiConnector.getPods();
        if (pods.get(taskId) == null) {
            return Collections.emptyList();
        }
        Pod pod = pods.get(taskId);
        List<ContainerStatus> containerStatuses = pod.getStatus().getContainerStatuses();
        if (CollectionsExt.isNullOrEmpty(containerStatuses)) {
            return Collections.emptyList();
        }
        List<TaskStatus.ContainerState> containerStates = new ArrayList<>();
        for (ContainerStatus containerStatus : containerStatuses) {
            ContainerState status = containerStatus.getState();
            String containerName = containerStatus.getName();
            TaskStatus.ContainerState.ContainerHealth containerHealth = TaskStatus.ContainerState.ContainerHealth.Unset;
            if (status.getRunning() != null) {
                containerHealth = TaskStatus.ContainerState.ContainerHealth.Healthy;
            } else if (status.getTerminated() != null) {
                containerHealth = TaskStatus.ContainerState.ContainerHealth.Unhealthy;
            }
            BasicImage basicImage = buildBasicImageFromContainerStatus(pod, containerStatus.getImage(), containerName);
            String platformSidecar = getPlatformSidecarThatCreatedContainer(pod, containerName);
            containerStates.add(TaskStatus.ContainerState.newBuilder()
                    .setContainerName(containerStatus.getName())
                    .setContainerHealth(containerHealth)
                    .setContainerImage(basicImage)
                    .setPlatformSidecar(platformSidecar)
                    .build());
        }
        return containerStates;
    }

    /**
     * Generates a Titus object for basic image from then general string object that k8s provides
     * (the kind you might provide to `docker pull`)
     * But the original tag is often not available. For that we have to look at annotations to see
     * the tag that generated the digest (if available, not all images come from tags).
     */
    static BasicImage buildBasicImageFromContainerStatus(Pod pod, String image, String containerName) {
        BasicImage.Builder bi = BasicImage.newBuilder();
        String imageWithoutRegistry = stripRegistryFromImage(image);
        String name = getNameFromImageString(imageWithoutRegistry);
        bi.setName(name);
        bi.setDigest(getDigestFromImageString(imageWithoutRegistry));
        bi.setTag(getTagFromImageString(imageWithoutRegistry, containerName, pod));
        return bi.build();
    }

    private static String stripRegistryFromImage(String image) {
        int slashStart = image.indexOf("/");
        if (slashStart < 0) {
            return image;
        } else {
            return image.substring(slashStart + 1);
        }
    }

    private static String getTagFromImageString(String image, String containerName,  Pod pod) {
        int tagStart = image.lastIndexOf(":");
        if (tagStart < 0) {
            return getTagFromAnnotation(containerName, pod);
        } else {
            return image.substring(tagStart + 1);
        }
    }

    private static String getTagFromAnnotation(String containerName, Pod pod) {
        String key = ANNOTATION_KEY_IMAGE_TAG_PREFIX + containerName;
        return pod.getMetadata().getAnnotations().getOrDefault(key, "");
    }

    private static String getDigestFromImageString(String image) {
        int digestStart = image.lastIndexOf("@");
        if (digestStart < 0) {
            return "";
        } else {
            return image.substring(digestStart + 1);
        }
    }

    private static String getNameFromImageString(String image) {
        int digestStart = image.lastIndexOf("@");
        if (digestStart < 0) {
            int tagStart = image.lastIndexOf(":");
            if (tagStart < 0) {
                return image;
            }
            return image.substring(0, tagStart);
        } else {
            return image.substring(0, digestStart);
        }
    }

    /**
     * Inspects a pod and determines what platform sidecar originally injected the container.
     * Returns empty string in the case that no platform sidecar injected the container (implies the container was user-defined)
     *
     * @param pod pod to inspect
     * @param containerName the name of the container that was (potentially) added
     * @return platform sidecar name
     */
    private String getPlatformSidecarThatCreatedContainer(Pod pod, String containerName) {
        String key = containerName + "." + ANNOTATION_KEY_SUFFIX_CONTAINERS + "/" + ANNOTATION_KEY_SUFFIX_CONTAINERS_SIDECAR;
        return pod.getMetadata().getAnnotations().getOrDefault(key, "");
    }

    static Task newTaskWithRelocationPlan(Task task, TaskRelocationPlan relocationPlan) {
        if (relocationPlan == null) {
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
