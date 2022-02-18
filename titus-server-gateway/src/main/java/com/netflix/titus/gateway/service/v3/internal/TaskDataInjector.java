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
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.grpc.protogen.BasicImage;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.MigrationDetails;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.connector.kubernetes.fabric8io.Fabric8IOConnector;
import com.netflix.titus.runtime.connector.relocation.RelocationDataReplicator;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.ANNOTATION_KEY_IMAGE_TAG_PREFIX;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.ANNOTATION_KEY_SUFFIX_CONTAINERS;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.ANNOTATION_KEY_SUFFIX_CONTAINERS_SIDECAR;

@Singleton
class TaskDataInjector {

    private final Fabric8IOConnector kubeApiConnector;
    private final FeatureActivationConfiguration featureActivationConfiguration;
    private final RelocationDataReplicator relocationDataReplicator;

    @Inject
    TaskDataInjector(
            FeatureActivationConfiguration featureActivationConfiguration,
            RelocationDataReplicator relocationDataReplicator,
            Fabric8IOConnector kubeApiConnector) {
        this.featureActivationConfiguration = featureActivationConfiguration;
        this.relocationDataReplicator = relocationDataReplicator;
        this.kubeApiConnector = kubeApiConnector;
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

    Task injectIntoTask(Task task) {
        Task decoratedTask = task;
        if (featureActivationConfiguration.isInjectingContainerStatesEnabled()) {
            decoratedTask = newTaskWithContainerState(task);
        }
        if (featureActivationConfiguration.isMergingTaskMigrationPlanInGatewayEnabled()) {
            decoratedTask = newTaskWithRelocationPlan(decoratedTask, relocationDataReplicator.getCurrent().getPlans().get(decoratedTask.getId()));
        }
        return decoratedTask;
    }

    public List<Task> injectIntoTasks(List<Task> tasks) {
        List<Task> decoratedTasks = new ArrayList<>();
        tasks.forEach(task -> decoratedTasks.add(injectIntoTask(task)));
        return decoratedTasks;
    }

    public TaskQueryResult injectIntoTaskQueryResult(TaskQueryResult queryResult) {
        return queryResult.toBuilder()
                .clearItems()
                .addAllItems(injectIntoTasks(queryResult.getItemsList()))
                .build();
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
        String digestFromImageString = getDigestFromImageString(imageWithoutRegistry);
        if (digestFromImageString.equals("")) {
            // If the image string doesn't have a digest, then the best we can do is
            // set the tag from whatever the image string has
            bi.setTag(getTagFromImageString(imageWithoutRegistry));
        } else {
            // If we *do* have a digest, then we can set it, but we have to get the tag
            // from an annotation. This is because the k8s pod object doesn't have room
            // for both pieces of data
            bi.setDigest(digestFromImageString);
            bi.setTag(getTagFromAnnotation(containerName, pod));
        }
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

    private static String getTagFromImageString(String image) {
        int tagStart = image.lastIndexOf(":");
        if (tagStart < 0) {
            return "";
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
     * @param pod           pod to inspect
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
