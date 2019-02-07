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

import java.util.HashMap;

import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.migration.SystemDefaultMigrationPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;

/**
 * Represents compatibility between Jobs for tasks to be
 * {@link com.netflix.titus.api.jobmanager.service.V3JobOperations#moveServiceTask(String, String, String) moved}
 * across them.
 * <p>
 * Jobs are compatible when their descriptors are identical, ignoring the following values:
 *
 * <ol>
 * <li>Owner</li>
 * <li>Application name</li>
 * <li>Disruption budget</li>
 * <li>Any attributes not prefixed with <tt>titus.</tt> or <tt>titusParameter.</tt></li>
 * <li>Any container attributes not prefixed with <tt>titus.</tt> or <tt>titusParameter.</tt></li>
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
        JobDescriptor<ServiceJobExt> jobFromNormalized = unsetIgnoredFieldsForCompatibility(from);
        JobDescriptor<ServiceJobExt> jobToNormalized = unsetIgnoredFieldsForCompatibility(to);
        boolean identical = jobFromNormalized.equals(jobToNormalized);
        return new JobCompatibility(jobFromNormalized, jobToNormalized, identical);
    }

    private static JobDescriptor<ServiceJobExt> unsetIgnoredFieldsForCompatibility(JobDescriptor<ServiceJobExt> descriptor) {
        HashMap<String, String> onlyTitusAttributes = new HashMap<>(descriptor.getAttributes());
        onlyTitusAttributes.entrySet().removeIf(entry ->
                !entry.getKey().startsWith("titus.") && !entry.getKey().startsWith("titusParameter.")
        );
        HashMap<String, String> onlyTitusContainerAttributes = new HashMap<>(descriptor.getContainer().getAttributes());
        onlyTitusContainerAttributes.entrySet().removeIf(entry ->
                !entry.getKey().startsWith("titus.") && !entry.getKey().startsWith("titusParameter.")
        );

        return descriptor.toBuilder()
                .withOwner(Owner.newBuilder().withTeamEmail("").build())
                .withApplicationName("")
                .withAttributes(onlyTitusAttributes)
                .withContainer(descriptor.getContainer().toBuilder()
                        .withAttributes(onlyTitusContainerAttributes)
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
