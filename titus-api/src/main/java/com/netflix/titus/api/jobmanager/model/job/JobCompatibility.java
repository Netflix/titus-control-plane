/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.jobmanager.model.job;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.migration.SystemDefaultMigrationPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;

/**
 * Represents compatibility between Jobs for tasks to be
 * {@link com.netflix.titus.api.jobmanager.service.V3JobOperations#moveServiceTask(String, String, String, CallMetadata)}  moved}
 * across them.
 * <p>
 * Jobs are compatible when their descriptors are identical, ignoring the following values:
 *
 * <ol>
 * <li>Owner</li>
 * <li>Application name</li>
 * <li>Job group info (stack, details, sequence)</li>
 * <li>Disruption budget</li>
 * <li>Any container attributes not prefixed with <tt>titus.</tt> or <tt>titusParameter.</tt></li>
 * <li>Any container.securityProfile attributes not prefixed with <tt>titus.</tt> or <tt>titusParameter.</tt></li>
 * <li>All information {@link ServiceJobExt specific to Service jobs}: capacity, retry policy, etc</li>
 * </ol>
 */
public class JobCompatibility {

    private final JobDescriptor<ServiceJobExt> from;
    private final JobDescriptor<ServiceJobExt> to;
    private final boolean compatible;

    private JobCompatibility(JobDescriptor<ServiceJobExt> from, JobDescriptor<ServiceJobExt> to, boolean compatible) {
        this.from = from;
        this.to = to;
        this.compatible = compatible;
    }

    public static JobCompatibility of(Job<ServiceJobExt> from, Job<ServiceJobExt> to) {
        return of(from.getJobDescriptor(), to.getJobDescriptor());
    }

    public static JobCompatibility of(JobDescriptor<ServiceJobExt> from, JobDescriptor<ServiceJobExt> to) {
        boolean ignoreImage = isImageSanitizationSkipped(from) || isImageSanitizationSkipped(to);
        boolean ignoreIam = isIamSanitizationSkipped(from) || isIamSanitizationSkipped(to);
        JobDescriptor<ServiceJobExt> jobFromNormalized = unsetIgnoredFieldsForCompatibility(from, ignoreImage, ignoreIam);
        JobDescriptor<ServiceJobExt> jobToNormalized = unsetIgnoredFieldsForCompatibility(to, ignoreImage, ignoreIam);
        boolean identical = jobFromNormalized.equals(jobToNormalized);
        return new JobCompatibility(jobFromNormalized, jobToNormalized, identical);
    }

    private static boolean isImageSanitizationSkipped(JobDescriptor<?> jobDescriptor) {
        return Boolean.parseBoolean(jobDescriptor.getAttributes().get(JobAttributes.JOB_ATTRIBUTES_SANITIZATION_SKIPPED_IMAGE));
    }

    private static boolean isIamSanitizationSkipped(JobDescriptor<?> jobDescriptor) {
        return Boolean.parseBoolean(jobDescriptor.getAttributes().get(JobAttributes.JOB_ATTRIBUTES_SANITIZATION_SKIPPED_IAM));
    }

    private static JobDescriptor<ServiceJobExt> unsetIgnoredFieldsForCompatibility(JobDescriptor<ServiceJobExt> descriptor,
                                                                                   boolean ignoreImage, boolean ignoreIam) {
        Container container = descriptor.getContainer();
        SecurityProfile securityProfile = container.getSecurityProfile();
        Map<String, String> onlyTitusContainerAttributes = filterOutNonTitusAttributes(container.getAttributes());
        Map<String, String> onlyTitusSecurityAttributes = filterOutNonTitusAttributes(securityProfile.getAttributes());

        return descriptor.toBuilder()
                .withOwner(Owner.newBuilder().withTeamEmail("").build())
                .withApplicationName("")
                .withJobGroupInfo(JobGroupInfo.newBuilder().build())
                .withAttributes(Collections.emptyMap())
                .withContainer(container.toBuilder()
                        .withImage(ignoreImage ? Image.newBuilder().build() : container.getImage())
                        .withAttributes(onlyTitusContainerAttributes)
                        .withSecurityProfile(securityProfile.toBuilder()
                                .withAttributes(onlyTitusSecurityAttributes)
                                .withIamRole(ignoreIam ? "" : container.getSecurityProfile().getIamRole())
                                .build())
                        .build())
                .withExtensions(ServiceJobExt.newBuilder()
                        .withRetryPolicy(ImmediateRetryPolicy.newBuilder().withRetries(0).build())
                        .withMigrationPolicy(SystemDefaultMigrationPolicy.newBuilder().build())
                        .withCapacity(Capacity.newBuilder().build())
                        .withServiceJobProcesses(ServiceJobProcesses.newBuilder().build())
                        .withEnabled(true)
                        .build())
                .withDisruptionBudget(DisruptionBudget.none())
                .build();
    }

    private static Map<String, String> filterOutNonTitusAttributes(Map<String, String> attributes) {
        Map<String, String> onlyTitus = new HashMap<>(attributes);
        onlyTitus.entrySet().removeIf(entry ->
                !entry.getKey().startsWith(JobAttributes.TITUS_ATTRIBUTE_PREFIX) &&
                        !entry.getKey().startsWith(JobAttributes.TITUS_PARAMETER_ATTRIBUTE_PREFIX)
        );
        return onlyTitus;
    }

    /**
     * @return {@link JobDescriptor} with values non-relevant for compatibility unset
     */
    public JobDescriptor<ServiceJobExt> getNormalizedDescriptorFrom() {
        return from;
    }

    /**
     * @return {@link JobDescriptor} with values non-relevant for compatibility unset
     */
    public JobDescriptor<ServiceJobExt> getNormalizedDescriptorTo() {
        return to;
    }

    public boolean isCompatible() {
        return compatible;
    }
}
