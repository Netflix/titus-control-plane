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

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.FeatureRolloutPlans;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.migration.MigrationPolicy;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobAssertions;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.feature.FeatureCompliance;
import com.netflix.titus.common.util.feature.FeatureCompliance.NonComplianceList;
import com.netflix.titus.grpc.protogen.NetworkConfiguration;
import com.netflix.titus.runtime.jobmanager.JobManagerConfiguration;

import static com.netflix.titus.api.FeatureRolloutPlans.CONTAINER_ACCOUNT_ID_AND_SUBNETS_REQUIRED_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.ENTRY_POINT_STRICT_VALIDATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.IAM_ROLE_REQUIRED_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.MIN_DISK_SIZE_STRICT_VALIDATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.SECURITY_GROUPS_REQUIRED_FEATURE;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_ACCOUNT_ID;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_CONTAINER_ATTRIBUTE_SUBNETS;

class JobFeatureComplianceChecks {

    @VisibleForTesting
    static final String DISRUPTION_BUDGET_FEATURE = "disruptionBudget";

    private static final Map<String, String> NO_ACCOUNT_ID_AND_SUBNETS_CONTAINER_ATTRIBUTES_CONTEXT = Collections.singletonMap("noContainerAccountIdAndSubnets", "Container accountId and/or subnet container attributes are empty/inconsistent");
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
            // On the HighScale network mode, it is important that the security group *not* be set,
            // as the purpose of the HighScale network mode is to enforce unified security groups
            // on the backend. Security groups are not configurable by the user with this mode.
            if (jobDescriptor.getNetworkConfiguration().getNetworkMode() == NetworkConfiguration.NetworkMode.HighScale.getNumber() ) {
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
     * A feature compliance is violated if there is a default accountId and subnets combination present in the {@link JobManagerConfiguration} for the deployment stack
     * but the container attributes in the {@link JobDescriptor} are missing one or both values.
     *
     * @param jobManagerConfiguration {@link JobManagerConfiguration}
     * @return feature compliance evaluation
     */
    static FeatureCompliance<JobDescriptor<?>> missingContainerAccountIdAndSubnets(JobManagerConfiguration jobManagerConfiguration) {
        return jobDescriptor -> {
            String defaultAccountId = jobManagerConfiguration.getDefaultContainerAccountId();
            String defaultSubnets = jobManagerConfiguration.getDefaultSubnets();
            // Ignore this compliance check unless both default accountId and subnet configuration properties are set implying our
            // intent to aid the job descriptor sanitization
            if (StringExt.isEmpty(defaultAccountId) || StringExt.isEmpty(defaultSubnets)) {
                return Optional.empty();
            }
            // On the HighScale network mode, it is important that the account/subents *not* be set.
            // Subnets/Accounts are not configurable by the user with this mode.
            if (jobDescriptor.getNetworkConfiguration().getNetworkMode() == NetworkConfiguration.NetworkMode.HighScale.getNumber() ) {
                return Optional.empty();
            }

            String accountIdContainerAttribute = jobDescriptor.getContainer().getAttributes().get(JOB_CONTAINER_ATTRIBUTE_ACCOUNT_ID);
            String subnetContainerAttribute = jobDescriptor.getContainer().getAttributes().get(JOB_CONTAINER_ATTRIBUTE_SUBNETS);

            // Feature compliance is violated if either (i) both attributes are not set, or (ii) accountId is set to the default value but the subnets value is not set.
            // In either case, we should take action in response to this violation.
            if ((StringExt.isEmpty(accountIdContainerAttribute) || defaultAccountId.equals(accountIdContainerAttribute)) && StringExt.isEmpty(subnetContainerAttribute)) {
                return Optional.of(NonComplianceList.of(
                        CONTAINER_ACCOUNT_ID_AND_SUBNETS_REQUIRED_FEATURE,
                        jobDescriptor,
                        NO_ACCOUNT_ID_AND_SUBNETS_CONTAINER_ATTRIBUTES_CONTEXT,
                        "accountId and/or subnets container attributes are not set"
                ));
            }
            return Optional.empty();
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

    /**
     * Disruption budget is not required to be set by clients.
     */
    static FeatureCompliance<JobDescriptor<?>> noDisruptionBudget() {
        return jobDescriptor -> {
            if (JobFunctions.hasDisruptionBudget(jobDescriptor)) {
                return Optional.empty();
            }

            String legacyMigrationPolicyInfo;
            if (JobFunctions.isBatchJob(jobDescriptor)) {
                legacyMigrationPolicyInfo = "no migration policy (batch job)";
            } else {
                MigrationPolicy migrationPolicy = ((ServiceJobExt) jobDescriptor.getExtensions()).getMigrationPolicy();
                legacyMigrationPolicyInfo = "service job with legacy migration policy: " + toString(migrationPolicy);
            }

            return Optional.of(NonComplianceList.of(
                    DISRUPTION_BUDGET_FEATURE,
                    jobDescriptor,
                    Collections.singletonMap("legacyMigration", legacyMigrationPolicyInfo),
                    "Job descriptor without disruption budget"
            ));
        };
    }

    private static String toString(MigrationPolicy migrationPolicy) {
        try {
            return (migrationPolicy == null ? "none" : ObjectMappers.storeMapper().writeValueAsString(migrationPolicy));
        } catch (Exception e) {
            return String.format("<%s>", e.getMessage());
        }
    }
}
