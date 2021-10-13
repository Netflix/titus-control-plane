/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.pod;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.PlatformSidecar;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.VolumeMount;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolumeUtils;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.master.kubernetes.PerformanceToolUtil;
import com.netflix.titus.master.kubernetes.client.KubeModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubePodUtil {

    private static final String MOUNT_PROPAGATION_BIDIRECTIONAL = com.netflix.titus.grpc.protogen.VolumeMount.MountPropagation.MountPropagationBidirectional.toString();
    private static final String MOUNT_PROPAGATION_HOST_TO_CONTAINER = com.netflix.titus.grpc.protogen.VolumeMount.MountPropagation.MountPropagationHostToContainer.toString();
    private static final String MOUNT_PROPAGATION_NONE = com.netflix.titus.grpc.protogen.VolumeMount.MountPropagation.MountPropagationNone.toString();

    private static final Logger logger = LoggerFactory.getLogger(KubePodUtil.class);

    private static final JsonFormat.Printer grpcJsonPrinter = JsonFormat.printer().includingDefaultValueFields();

    public static Map<String, String> createPodAnnotations(
            Job<?> job,
            Task task,
            byte[] containerInfoData,
            Map<String, String> passthroughAttributes,
            boolean includeJobDescriptor
    ) {
        String encodedContainerInfo = Base64.getEncoder().encodeToString(containerInfoData);

        Map<String, String> annotations = new HashMap<>(passthroughAttributes);
        annotations.putAll(PerformanceToolUtil.toAnnotations(job));
        annotations.put("containerInfo", encodedContainerInfo);
        Evaluators.acceptNotNull(
                job.getJobDescriptor().getAttributes().get(JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC),
                runtimeInSec -> annotations.put(KubeConstants.JOB_RUNTIME_PREDICTION, runtimeInSec + "s")
        );
        Evaluators.acceptNotNull(
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                count -> annotations.put(KubeConstants.OPPORTUNISTIC_CPU_COUNT, count)
        );
        Evaluators.acceptNotNull(
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION),
                id -> annotations.put(KubeConstants.OPPORTUNISTIC_ID, id)
        );
        Evaluators.acceptNotNull(
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID),
                id -> annotations.put(KubeConstants.STATIC_IP_ALLOCATION_ID, id)
        );
        Evaluators.acceptNotNull(
                job.getJobDescriptor().getNetworkConfiguration().getNetworkModeName(),
                modeName -> annotations.put(KubeConstants.NETWORK_MODE, modeName)
        );

        annotations.putAll(createEbsPodAnnotations(job, task));

        if (includeJobDescriptor) {
            annotations.put("jobDescriptor", createEncodedJobDescriptor(job));
        }

        annotations.putAll(createPodAnnotationsFromJobParameters(job));

        return annotations;
    }

    public static String createEncodedJobDescriptor(Job<?> job) {
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> filteredJobDescriptor = filterPodJobDescriptor(job.getJobDescriptor());
        JobDescriptor grpcJobDescriptor = GrpcJobManagementModelConverters.toGrpcJobDescriptor(filteredJobDescriptor);
        try {
            String jobDescriptorJson = grpcJsonPrinter.print(grpcJobDescriptor);
            return StringExt.gzipAndBase64Encode(jobDescriptorJson);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Unable to convert protobuf message into json: ", e);
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> createPodAnnotationsFromJobParameters(Job<?> job) {
        Map<String, String> annotations = new HashMap<>();
        Map<String, String> containerAttributes = job.getJobDescriptor().getContainer().getAttributes();
        Evaluators.acceptNotNull(
                containerAttributes.get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_ACCOUNT_ID),
                accountId -> annotations.put(KubeConstants.POD_LABEL_ACCOUNT_ID, accountId)
        );
        Evaluators.acceptNotNull(
                containerAttributes.get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_SUBNETS),
                accountId -> annotations.put(KubeConstants.POD_LABEL_SUBNETS, accountId)
        );
        return annotations;
    }

    public static Map<String, String> createEbsPodAnnotations(Job<?> job, Task task) {
        Map<String, String> annotations = new HashMap<>();

        List<EbsVolume> ebsVolumes = job.getJobDescriptor().getContainer().getContainerResources().getEbsVolumes();
        if (ebsVolumes.isEmpty()) {
            return Collections.emptyMap();
        }
        EbsVolume ebsVolume = job.getJobDescriptor().getContainer().getContainerResources().getEbsVolumes().get(0);

        String ebsVolumeId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID);
        if (ebsVolumeId == null) {
            logger.error("Expected to find assigned EBS volume ID to task {} from volumes {}", task.getId(), ebsVolumes);
            return Collections.emptyMap();
        }

        annotations.put(KubeConstants.EBS_VOLUME_ID, ebsVolumeId);
        annotations.put(KubeConstants.EBS_MOUNT_PERMISSIONS, ebsVolume.getMountPermissions().toString());
        annotations.put(KubeConstants.EBS_MOUNT_PATH, ebsVolume.getMountPath());
        annotations.put(KubeConstants.EBS_FS_TYPE, ebsVolume.getFsType());

        return annotations;
    }

    /**
     * Looks at a job's PlatformSidecars and converts them to the correct annotations
     * for the platform sidecar mutator to use.
     */
    public static Map<String, String> createPlatformSidecarAnnotations(Job<?> job) {
        Map<String, String> annotations = new HashMap<>();
        for (PlatformSidecar ps : job.getJobDescriptor().getPlatformSidecars()) {
            annotations.putAll(createSinglePlatformSidecarAnnotations(ps));
        }
        return annotations;
    }

    private static Map<String, String> createSinglePlatformSidecarAnnotations(PlatformSidecar ps) {
        Map<String, String> annotations = new HashMap<>();
        String nameKey = ps.getName() + KubeConstants.PLATFORM_SIDECAR_SUFFIX;
        annotations.put(nameKey, "true");
        String channelKey = ps.getName() + KubeConstants.PLATFORM_SIDECAR_CHANNEL_SUFFIX;
        annotations.put(channelKey, ps.getChannel());
        String argumentsKey = ps.getName() + KubeConstants.PLATFORM_SIDECAR_ARGS_SUFFIX;
        annotations.put(argumentsKey, ps.getArguments());
        return annotations;
    }

    /**
     * Returns a job descriptor with fields unnecessary for inclusion on the pod removed.
     */
    public static com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> filterPodJobDescriptor(com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> jobDescriptor) {
        // Metatron auth context is not needed on the pod.
        return JobFunctions.deleteJobSecurityAttributes(jobDescriptor, Collections.singleton(JobAttributes.JOB_SECURITY_ATTRIBUTE_METATRON_AUTH_CONTEXT));
    }

    /**
     * Builds the various objects needed to for PersistentVolume and Pod objects to use an volume.
     */
    public static Optional<Pair<V1Volume, V1VolumeMount>> buildV1VolumeInfo(Job<?> job, Task task) {
        return EbsVolumeUtils.getEbsVolumeForTask(job, task)
                .map(ebsVolume -> {
                    boolean readOnly = ebsVolume.getMountPermissions().equals(EbsVolume.MountPerm.RO);
                    V1Volume v1Volume = new V1Volume()
                            // The resource name matches the volume ID so that the resource is independent of the job.
                            .name(ebsVolume.getVolumeId())
                            .persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
                                    .claimName(KubeModelConverters.toPvcName(ebsVolume.getVolumeId(), task.getId())));

                    V1VolumeMount v1VolumeMount = new V1VolumeMount()
                            // The mount refers to the V1Volume being mounted
                            .name(ebsVolume.getVolumeId())
                            .mountPath(ebsVolume.getMountPath())
                            .readOnly(readOnly);

                    return Pair.of(v1Volume, v1VolumeMount);
                });
    }

    /**
     * Converts a list of VolumeMounts and converts them to the k8s VolumeMounts
     */
    public static List<V1VolumeMount> buildV1VolumeMounts(List<VolumeMount> volumeMounts) {
        List<V1VolumeMount> v1VolumeMounts = new ArrayList<>();
        if (volumeMounts == null) {
            return v1VolumeMounts;
        }
        for (VolumeMount vm : volumeMounts) {
            v1VolumeMounts.add(buildV1VolumeMount(vm));
        }
        return v1VolumeMounts;
    }

    private static V1VolumeMount buildV1VolumeMount(VolumeMount vm) {
        return new V1VolumeMount()
                .name(vm.getVolumeName())
                .mountPath(vm.getMountPath())
                .mountPropagation(buildV1VolumeMountPropagation(vm.getMountPropagation()))
                .readOnly(vm.getReadOnly())
                .subPath(vm.getSubPath());
    }

    private static String buildV1VolumeMountPropagation(String mountPropagation) {
        if (mountPropagation.equals(MOUNT_PROPAGATION_BIDIRECTIONAL)) {
            return "Bidirectional";
        } else if (mountPropagation.equals(MOUNT_PROPAGATION_HOST_TO_CONTAINER)) {
            return "HostToContainer";
        } else if (mountPropagation.equals(MOUNT_PROPAGATION_NONE)) {
            return "None";
        } else {
            return "None";
        }
    }


    /**
     * Builds the image string for Kubernetes pod spec
     */
    public static String buildImageString(String registryUrl, Image image) {
        return registryUrl + image.getName() + "@" + image.getDigest();
    }

    /**
     * Convert map into list of Kubernetes env var objects.
     */
    public static List<V1EnvVar> toV1EnvVar(Map<String, String> env) {
        return env.entrySet().stream()
                .map(entry -> new V1EnvVar().name(entry.getKey()).value(entry.getValue()))
                .collect(Collectors.toList());
    }

    /**
     *
     * Sanitize name based on Kubernetes regex: [a-z0-9]([-a-z0-9]*[a-z0-9])?.
     */
    public static String sanitizeVolumeName(String name) {
        return name.toLowerCase().replaceAll("[^a-z0-9]([^-a-z0-9]*[^a-z0-9])?", "-");
    }
}
