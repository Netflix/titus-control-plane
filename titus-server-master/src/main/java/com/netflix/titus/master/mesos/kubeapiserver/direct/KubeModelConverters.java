/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.mesos.kubeapiserver.direct;

import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolumeUtils;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1AWSElasticBlockStoreVolumeSource;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeSpec;
import io.kubernetes.client.openapi.models.V1Volume;

/**
 * A collection of helper functions to convert core and Kube objects to other Kube model objects.
 */
public class KubeModelConverters {

    private static final String KUBE_VOLUME_ACCESS_MODE = "ReadWriteOnce";
    private static final String KUBE_VOLUME_VOLUME_MODE_FS = "Filesystem";
    private static final String KUBE_VOLUME_RECLAIM_POLICY = "Retain";

    private static final String KUBE_VOLUME_CAPACITY_KEY = "storage";

    public static V1PersistentVolume toV1PersistentVolume(Job<?> job, Task task, V1Volume v1Volume) {
        Optional<EbsVolume> optionalEbsVolume = EbsVolumeUtils.getEbsVolumeForTask(job, task);
        if (!optionalEbsVolume.isPresent()) {
            throw new IllegalStateException(String.format("Expected EBS volume for job %s and task %s", job, task));
        }
        EbsVolume ebsVolume = optionalEbsVolume.get();

        V1ObjectMeta v1ObjectMeta = new V1ObjectMeta()
                .name(v1Volume.getName());

        V1PersistentVolumeSpec v1PersistentVolumeSpec = new V1PersistentVolumeSpec()
                .addAccessModesItem(KUBE_VOLUME_ACCESS_MODE)
                .volumeMode(KUBE_VOLUME_VOLUME_MODE_FS)
                .persistentVolumeReclaimPolicy(KUBE_VOLUME_RECLAIM_POLICY)
                .putCapacityItem(KUBE_VOLUME_CAPACITY_KEY, new Quantity(ebsVolume.getVolumeCapacityGB() + "Gi"));

        if (null != v1Volume.getAwsElasticBlockStore()) {
            v1PersistentVolumeSpec
                    .awsElasticBlockStore(new V1AWSElasticBlockStoreVolumeSource()
                            .volumeID(v1Volume.getAwsElasticBlockStore().getVolumeID())
                            .fsType(v1Volume.getAwsElasticBlockStore().getFsType()));
        }

        return new V1PersistentVolume()
                .apiVersion("v1")
                .metadata(v1ObjectMeta)
                .spec(v1PersistentVolumeSpec);
    }
}
