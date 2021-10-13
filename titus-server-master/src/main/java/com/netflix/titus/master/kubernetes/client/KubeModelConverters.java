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

package com.netflix.titus.master.kubernetes.client;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.master.kubernetes.KubeUtil;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1CSIPersistentVolumeSource;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimSpec;
import io.kubernetes.client.openapi.models.V1PersistentVolumeSpec;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;

import static com.netflix.titus.common.util.StringExt.splitByDot;

/**
 * A collection of helper functions to convert core and Kube objects to other Kube model objects.
 */
public class KubeModelConverters {

    public static final String KUBE_VOLUME_CSI_EBS_DRIVER = "ebs.csi.aws.com";
    public static final String KUBE_VOLUME_CSI_EBS_VOLUME_KEY = "volumeHandle";

    private static final String KUBE_VOLUME_API_VERSION = "v1";
    private static final String KUBE_VOLUME_ACCESS_MODE = "ReadWriteOnce";
    private static final String KUBE_VOLUME_VOLUME_MODE_FS = "Filesystem";
    private static final String KUBE_VOLUME_RECLAIM_POLICY = "Retain";

    private static final String KUBE_VOLUME_CAPACITY_KEY = "storage";

    public static Map<String, String> toVolumeLabelMap(String volumeName) {
        return Collections.singletonMap(KUBE_VOLUME_CSI_EBS_VOLUME_KEY, volumeName);
    }

    public static V1LabelSelector toVolumeMatchSelector(String volumeName) {
        return new V1LabelSelector()
                .matchLabels(toVolumeLabelMap(volumeName));
    }

    public static V1PersistentVolume toEbsV1PersistentVolume(EbsVolume ebsVolume) {
        V1ObjectMeta v1ObjectMeta = new V1ObjectMeta()
                .labels(toVolumeLabelMap(ebsVolume.getVolumeId()))
                .name(ebsVolume.getVolumeId());

        V1PersistentVolumeSpec v1PersistentVolumeSpec = new V1PersistentVolumeSpec()
                .addAccessModesItem(KUBE_VOLUME_ACCESS_MODE)
                .volumeMode(KUBE_VOLUME_VOLUME_MODE_FS)
                .persistentVolumeReclaimPolicy(KUBE_VOLUME_RECLAIM_POLICY)
                .putCapacityItem(KUBE_VOLUME_CAPACITY_KEY, new Quantity(volumeCapacityGiBToString(ebsVolume.getVolumeCapacityGB())))
                .csi(new V1CSIPersistentVolumeSource()
                        .driver(KUBE_VOLUME_CSI_EBS_DRIVER)
                        .volumeHandle(ebsVolume.getVolumeId())
                        .fsType(ebsVolume.getFsType()));

        return new V1PersistentVolume()
                .apiVersion(KUBE_VOLUME_API_VERSION)
                .metadata(v1ObjectMeta)
                .spec(v1PersistentVolumeSpec);
    }

    public static V1PersistentVolumeClaim toV1PersistentVolumeClaim(V1PersistentVolume v1PersistentVolume, V1Pod v1Pod) {
        String volumeName = Optional.ofNullable(v1PersistentVolume.getSpec().getCsi())
                .orElseThrow(() -> new IllegalStateException(String.format("Expected CSI persistent volume for %s", v1PersistentVolume)))
                .getVolumeHandle();

        // The claim name should be specific to this volume and task
        V1ObjectMeta v1ObjectMeta = new V1ObjectMeta()
                .name(KubeModelConverters.toPvcName(KubeUtil.getMetadataName(v1PersistentVolume.getMetadata()), KubeUtil.getMetadataName(v1Pod.getMetadata())));

        // The claim spec should be scoped to this specific volume
        V1PersistentVolumeClaimSpec v1PersistentVolumeClaimSpec = new V1PersistentVolumeClaimSpec()
                .volumeName(volumeName)
                .addAccessModesItem(KUBE_VOLUME_ACCESS_MODE)
                .volumeMode(KUBE_VOLUME_VOLUME_MODE_FS)
                .resources(new V1ResourceRequirements().requests(v1PersistentVolume.getSpec().getCapacity()))
                .selector(KubeModelConverters.toVolumeMatchSelector(volumeName));

        return new V1PersistentVolumeClaim()
                .apiVersion(KUBE_VOLUME_API_VERSION)
                .metadata(v1ObjectMeta)
                .spec(v1PersistentVolumeClaimSpec);
    }

    public static String toPvcName(String volumeName, String taskId) {
        // Combine PV and task ID to create a unique PVC name
        return volumeName + "." + taskId;
    }

    public static String getTaskIdFromPvc(V1PersistentVolumeClaim v1PersistentVolumeClaim) {
        // We expect the volume ID to precede task ID and to be separated via a '.'
        List<String> parts = splitByDot(KubeUtil.getMetadataName(v1PersistentVolumeClaim.getMetadata()));
        return parts.size() < 2 ? "" : parts.get(1);
    }

    private static String volumeCapacityGiBToString(int capacityGiB) {
        return String.format("%dGi", capacityGiB);
    }
}
