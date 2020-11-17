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

package com.netflix.titus.api.jobmanager.model.job.ebs;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.StringExt;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID;

/**
 * Helper utilities for processing EBS volume related info.
 */
public class EbsVolumeUtils {

    public static <E extends JobDescriptor.JobDescriptorExt> List<EbsVolume> getEbsVolumes(JobDescriptor<E> jobDescriptor) {
        List<String> volumeIds = getVolumeIds(jobDescriptor);
        Optional<String> mountPointOptional = getMountPoint(jobDescriptor);
        Optional<EbsVolume.MountPerm> mountPermOptional = getEbsMountPerm(jobDescriptor);
        Optional<String> fsTypeOptional = getEbsFsType(jobDescriptor);

        if (!(mountPointOptional.isPresent() && mountPermOptional.isPresent() && fsTypeOptional.isPresent())) {
            return Collections.emptyList();
        }

        String mountPoint = mountPointOptional.get();
        EbsVolume.MountPerm mountPerm = mountPermOptional.get();
        String fsType = fsTypeOptional.get();

        return volumeIds.stream()
                .map(volumeId -> EbsVolume.newBuilder()
                        .withVolumeId(volumeId)
                        .withMountPath(mountPoint)
                        .withMountPermissions(mountPerm)
                        .withFsType(fsType)
                        .build())
                .collect(Collectors.toList());
    }

    public static <E extends JobDescriptor.JobDescriptorExt> List<String> getVolumeIds(JobDescriptor<E> jobDescriptor) {
        String volumeIdsStr = StringExt.nonNull(jobDescriptor.getAttributes().get(JobAttributes.JOB_ATTRIBUTES_EBS_VOLUME_IDS));
        return StringExt.splitByComma(volumeIdsStr);
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Optional<String> getMountPoint(JobDescriptor<E> jobDescriptor) {
        return Optional.ofNullable(jobDescriptor.getAttributes().get(JobAttributes.JOB_ATTRIBUTES_EBS_MOUNT_POINT));
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Optional<EbsVolume.MountPerm> getEbsMountPerm(JobDescriptor<E> jobDescriptor) throws IllegalArgumentException {
        String mountPermStr = StringExt.nonNull(jobDescriptor.getAttributes().get(JobAttributes.JOB_ATTRIBUTES_EBS_MOUNT_PERM)).toUpperCase();
        return mountPermStr.isEmpty() ? Optional.empty() : Optional.of(EbsVolume.MountPerm.valueOf(mountPermStr));
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Optional<String> getEbsFsType(JobDescriptor<E> jobDescriptor) {
        return Optional.ofNullable(jobDescriptor.getAttributes().get(JobAttributes.JOB_ATTRIBUTES_EBS_FS_TYPE));
    }

    public static Optional<EbsVolume> getEbsVolumeForTask(Job<?> job, Task task) {
        String ebsVolumeId = task.getTaskContext().get(TASK_ATTRIBUTES_EBS_VOLUME_ID);
        if (null == ebsVolumeId) {
            return Optional.empty();
        }

        return job.getJobDescriptor().getContainer().getContainerResources().getEbsVolumes()
                .stream()
                .filter(ebsVolume -> ebsVolume.getVolumeId().equals(ebsVolumeId))
                .findFirst();
    }
}
