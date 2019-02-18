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

package com.netflix.titus.gateway.service.v3.internal;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.netflix.titus.api.FeatureRolloutPlans;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobAssertions;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.feature.FeatureCompliance;
import com.netflix.titus.common.util.feature.FeatureCompliance.NonComplianceList;
import com.netflix.titus.runtime.jobmanager.JobManagerConfiguration;

import static com.netflix.titus.api.FeatureRolloutPlans.ENTRY_POINT_STRICT_VALIDATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.IAM_ROLE_REQUIRED_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.MIN_DISK_SIZE_STRICT_VALIDATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.SECURITY_GROUPS_REQUIRED_FEATURE;

class JobFeatureComplianceChecks {

    private static final Map<String, String> NO_IAM_ROLE_CONTEXT = Collections.singletonMap("noIamRole", "IAM role not set");
    private static final Map<String, String> NO_SECURITY_GROUPS_CONTEXT = Collections.singletonMap("noSecurityGroups", "Security groups not set");
    private static final Map<String, String> ENTRY_POINT_WITH_SPACES_CONTEXT = Collections.singletonMap("entryPointBinaryWithSpaces", "Entry point contains spaces");

    private static final Predicate<String> CONTAINS_SPACES = Pattern.compile(".*\\s+.*").asPredicate();

    /**
     * See {@link FeatureRolloutPlans#ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE}.
     */
    static FeatureCompliance<JobDescriptor<?>> environmentVariablesNames(JobAssertions jobAssertions) {
        return jobDescriptor -> {
            Map<String, String> context = jobAssertions.validateEnvironmentVariableNames(jobDescriptor.getContainer().getEnv());
            if (context.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(NonComplianceList.of(
                    ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE,
                    jobDescriptor,
                    context,
                    "Environment variable names may include only ASCII letters, digits and '_', and the first letter cannot be a digit."
            ));
        };
    }

    /**
     * See {@link FeatureRolloutPlans#IAM_ROLE_REQUIRED_FEATURE}.
     */
    static FeatureCompliance<JobDescriptor<?>> missingIamRole() {
        return jobDescriptor -> {
            if (!jobDescriptor.getContainer().getSecurityProfile().getIamRole().isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(NonComplianceList.of(
                    IAM_ROLE_REQUIRED_FEATURE,
                    jobDescriptor,
                    NO_IAM_ROLE_CONTEXT,
                    "IAM role not set in the job descriptor"
            ));
        };
    }

    /**
     * See {@link FeatureRolloutPlans#SECURITY_GROUPS_REQUIRED_FEATURE}.
     */
    static FeatureCompliance<JobDescriptor<?>> missingSecurityGroups() {
        return jobDescriptor -> {
            if (!jobDescriptor.getContainer().getSecurityProfile().getSecurityGroups().isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(NonComplianceList.of(
                    SECURITY_GROUPS_REQUIRED_FEATURE,
                    jobDescriptor,
                    NO_SECURITY_GROUPS_CONTEXT,
                    "At least one security group must be set in the job descriptor"
            ));
        };
    }

    /**
     * See {@link FeatureRolloutPlans#ENTRY_POINT_STRICT_VALIDATION_FEATURE}.
     */
    static FeatureCompliance<JobDescriptor<?>> entryPointViolations() {
        return jobDescriptor -> {
            List<String> entryPoint = jobDescriptor.getContainer().getEntryPoint();
            List<String> command = jobDescriptor.getContainer().getCommand();
            if (!CollectionsExt.isNullOrEmpty(entryPoint) && CollectionsExt.isNullOrEmpty(command) && CONTAINS_SPACES.test(entryPoint.get(0))) {
                return Optional.of(NonComplianceList.of(
                        ENTRY_POINT_STRICT_VALIDATION_FEATURE,
                        jobDescriptor,
                        ENTRY_POINT_WITH_SPACES_CONTEXT,
                        "First entry point value cannot contain spaces"
                ));
            }
            return Optional.empty();
        };
    }

    /**
     * See {@link FeatureRolloutPlans#MIN_DISK_SIZE_STRICT_VALIDATION_FEATURE}.
     */
    static FeatureCompliance<JobDescriptor<?>> minDiskSize(JobManagerConfiguration jobManagerConfiguration) {
        return jobDescriptor -> {

            ContainerResources containerResources = jobDescriptor.getContainer().getContainerResources();
            int minDiskSize = jobManagerConfiguration.getMinDiskSizeMB();
            if (containerResources.getDiskMB() >= minDiskSize) {
                return Optional.empty();
            }

            return Optional.of(NonComplianceList.of(
                    MIN_DISK_SIZE_STRICT_VALIDATION_FEATURE,
                    jobDescriptor,
                    Collections.singletonMap("diskSizeLessThanMin", String.format("Minimum disk size is %sMB, but is set %sMB", minDiskSize, containerResources.getDiskMB())),
                    String.format("Job descriptor must declare disk size that is no less than %sMB", minDiskSize)
            ));
        };
    }
}
