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

package com.netflix.titus.testkit.model.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.testkit.model.PrimitiveValueGenerators;

public class JobEbsVolumeGenerator {

    private static DataGenerator<String> ebsVolumeIds() {
        return PrimitiveValueGenerators.hexValues(8).map(hex -> "vol-" + hex);
    }

    private static DataGenerator<String> availabilityZones() {
        return DataGenerator.items("us-east-1a", "us-east-1b", "us-east-1c")
                .loop();
    }

    public static DataGenerator<EbsVolume> jobEbsVolumes(int count) {
        List<String> ebsVolumeIds = ebsVolumeIds().getValues(count);
        List<String> availabilityZones = availabilityZones().getValues(count);
        List<EbsVolume> ebsVolumes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ebsVolumes.add(EbsVolume.newBuilder()
                    .withVolumeId(ebsVolumeIds.get(i))
                    .withVolumeAvailabilityZone(availabilityZones.get(i))
                    .withVolumeCapacityGB(5)
                    .withMountPath("/ebs_mnt")
                    .withMountPermissions(EbsVolume.MountPerm.RW)
                    .withFsType("xfs")
                    .build());
        }
        return DataGenerator.items(ebsVolumes);
    }

    public static Map<String, String> jobEbsVolumesToAttributes(List<EbsVolume> ebsVolumes) {
        Map<String, String> ebsAttributes = new HashMap<>();

        String ebsVolumeIds = ebsVolumes.stream()
                .map(EbsVolume::getVolumeId)
                .collect(Collectors.joining(","));
        ebsAttributes.put(JobAttributes.JOB_ATTRIBUTES_EBS_VOLUME_IDS, ebsVolumeIds);
        ebsAttributes.put(JobAttributes.JOB_ATTRIBUTES_EBS_MOUNT_POINT, ebsVolumes.stream().findFirst().map(EbsVolume::getMountPath).orElse("/ebs_mnt"));
        ebsAttributes.put(JobAttributes.JOB_ATTRIBUTES_EBS_MOUNT_PERM, ebsVolumes.stream().findFirst().map(EbsVolume::getMountPermissions).orElse(EbsVolume.MountPerm.RW).toString());
        ebsAttributes.put(JobAttributes.JOB_ATTRIBUTES_EBS_FS_TYPE, ebsVolumes.stream().findFirst().map(EbsVolume::getFsType).orElse("xfs"));

        return ebsAttributes;
    }

    public static Task appendEbsVolumeAttribute(Task task, String ebsVolumeId) {
        return JobFunctions.appendTaskContext(task, TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID, ebsVolumeId);
    }
}
