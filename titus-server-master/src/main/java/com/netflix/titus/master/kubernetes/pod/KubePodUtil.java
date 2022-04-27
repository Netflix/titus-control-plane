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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
import com.netflix.titus.api.jobmanager.model.job.volume.SaaSVolumeSource;
import com.netflix.titus.api.jobmanager.model.job.volume.SharedContainerVolumeSource;
import com.netflix.titus.api.jobmanager.model.job.volume.Volume;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.models.V1AWSElasticBlockStoreVolumeSource;
import io.kubernetes.client.openapi.models.V1CephFSVolumeSource;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1FlexVolumeSource;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.common.kube.Annotations.AnnotationKeyStorageEBSFSType;
import static com.netflix.titus.common.kube.Annotations.AnnotationKeyStorageEBSMountPath;
import static com.netflix.titus.common.kube.Annotations.AnnotationKeyStorageEBSMountPerm;
import static com.netflix.titus.common.kube.Annotations.AnnotationKeyStorageEBSVolumeID;
import static com.netflix.titus.common.kube.Annotations.AnnotationKeySuffixSidecars;

public class KubePodUtil {

    private static final String MOUNT_PROPAGATION_BIDIRECTIONAL = com.netflix.titus.grpc.protogen.VolumeMount.MountPropagation.MountPropagationBidirectional.toString();
    private static final String MOUNT_PROPAGATION_HOST_TO_CONTAINER = com.netflix.titus.grpc.protogen.VolumeMount.MountPropagation.MountPropagationHostToContainer.toString();
    private static final String MOUNT_PROPAGATION_NONE = com.netflix.titus.grpc.protogen.VolumeMount.MountPropagation.MountPropagationNone.toString();

    private static final Logger logger = LoggerFactory.getLogger(KubePodUtil.class);

    private static final JsonFormat.Printer grpcJsonPrinter = JsonFormat.printer().includingDefaultValueFields();

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

        annotations.put(AnnotationKeyStorageEBSVolumeID, ebsVolumeId);
        annotations.put(AnnotationKeyStorageEBSMountPerm, ebsVolume.getMountPermissions().toString());
        annotations.put(AnnotationKeyStorageEBSMountPath, ebsVolume.getMountPath());
        annotations.put(AnnotationKeyStorageEBSFSType, ebsVolume.getFsType());

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
        String nameKey = ps.getName() + "." + AnnotationKeySuffixSidecars;
        annotations.put(nameKey, "true");
        String channelKey = ps.getName() + "." + AnnotationKeySuffixSidecars + "/channel";
        annotations.put(channelKey, ps.getChannel());
        String argumentsKey = ps.getName() + "." + AnnotationKeySuffixSidecars + "/arguments";
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
     * Builds the various objects needed to for Pod objects to use a volume.
     */
    public static Optional<Pair<V1Volume, V1VolumeMount>> buildV1EBSObjects(Job<?> job, Task task) {
        return EbsVolumeUtils.getEbsVolumeForTask(job, task)
                .map(ebsVolume -> {
                    boolean readOnly = ebsVolume.getMountPermissions().equals(EbsVolume.MountPerm.RO);
                    V1AWSElasticBlockStoreVolumeSource ebsVolumeSource = new V1AWSElasticBlockStoreVolumeSource()
                            .volumeID(ebsVolume.getVolumeId())
                            .fsType(ebsVolume.getFsType())
                            .readOnly(false);
                    V1Volume v1Volume = new V1Volume()
                            // The resource name matches the volume ID so that the resource is independent of the job.
                            .name(ebsVolume.getVolumeId())
                            .awsElasticBlockStore(ebsVolumeSource);

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

    public static List<V1Volume> buildV1Volumes(List<Volume> volumes) {
        if (volumes == null) {
            return Collections.emptyList();
        }
        List<V1Volume> v1Volumes = new ArrayList<>();
        for (Volume v : volumes) {
            buildV1Volume(v).ifPresent(v1Volumes::add);
        }
        return v1Volumes;
    }

    private static Optional<V1Volume> buildV1Volume(Volume volume) {
        if (volume.getVolumeSource() instanceof SharedContainerVolumeSource) {
            V1FlexVolumeSource flexVolume = getV1FlexVolumeForSharedContainerVolumeSource(volume);
            return Optional.ofNullable(new V1Volume()
                    .name(volume.getName())
                    .flexVolume(flexVolume));
        } else if (volume.getVolumeSource() instanceof SaaSVolumeSource) {
            V1CephFSVolumeSource cephVolume = getV1CephVolumeSource(volume);
            return Optional.ofNullable(new V1Volume()
                    .name(volume.getName())
                    .cephfs(cephVolume));
        } else {
            return Optional.empty();
        }

    }

    private static V1FlexVolumeSource getV1FlexVolumeForSharedContainerVolumeSource(Volume volume) {
        SharedContainerVolumeSource sharedContainerVolumeSource = (SharedContainerVolumeSource) volume.getVolumeSource();
        Map<String, String> options = new HashMap<>();
        options.put("sourceContainer", sharedContainerVolumeSource.getSourceContainer());
        options.put("sourcePath", sharedContainerVolumeSource.getSourcePath());
        return new V1FlexVolumeSource()
                .driver("SharedContainerVolumeSource")
                .options(options);
    }

    private static V1CephFSVolumeSource getV1CephVolumeSource(Volume volume) {
        SaaSVolumeSource saaSVolumeSource = (SaaSVolumeSource) volume.getVolumeSource();
        // This Ceph volume source is not "truly" representative of all the data we will
        // need to mount a SaaS volume. The actual connection string is built just-in-time
        // by the executor.
        // `monitors` must be set, however, to pass KubeAPI validation.
        // The "path" is what we use to store the actual SaaS volume ID.
        return new V1CephFSVolumeSource()
                .monitors(Collections.singletonList(""))
                .path(saaSVolumeSource.getSaaSVolumeID());
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
     * Sanitize name based on Kubernetes regex: [a-z0-9]([-a-z0-9]*[a-z0-9])?.
     */
    public static String sanitizeVolumeName(String name) {
        String replaced = name.toLowerCase().replaceAll("[^a-z0-9]([^-a-z0-9]*[^a-z0-9])?", "-");
        // All volume names *must* end with a normal char, so we always append something to the end
        return replaced + "-vol";
    }

    public static List<String> getVolumeNames(List<V1Volume> volumes) {
        return volumes.stream()
                .map(e -> e.getName())
                .collect(Collectors.toList());
    }

    public static String selectScheduler(SchedulerConfiguration schedulerConfiguration, ApplicationSLA capacityGroupDescriptor, KubePodConfiguration configuration) {
        String schedulerName;
        if (capacityGroupDescriptor != null && capacityGroupDescriptor.getTier() == Tier.Critical) {
            if (schedulerConfiguration.isCriticalServiceJobSpreadingEnabled()) {
                schedulerName = configuration.getReservedCapacityKubeSchedulerName();
            } else {
                schedulerName = configuration.getReservedCapacityKubeSchedulerNameForBinPacking();
            }
        } else {
            schedulerName = configuration.getKubeSchedulerName();
        }
        return schedulerName;
    }
}
