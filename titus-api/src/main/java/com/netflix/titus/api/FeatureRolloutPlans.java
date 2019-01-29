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

package com.netflix.titus.api;

import com.netflix.titus.common.util.feature.FeatureRollout;

public interface FeatureRolloutPlans {
    @FeatureRollout(
            featureId = "securityGroupsRequired",
            deadline = "03/30/2019",
            description = "Require all clients to provide the security group in the job descriptor."
    )
    String SECURITY_GROUPS_REQUIRED_FEATURE = "securityGroupsRequired";

    @FeatureRollout(
            featureId = "iamRoleRequired",
            deadline = "03/30/2019",
            description = "Require all clients to provide the IAM role in the job descriptor."
    )
    String IAM_ROLE_REQUIRED_FEATURE = "iamRoleRequired";

    @FeatureRollout(
            featureId = "entryPointStrictValidation",
            deadline = "06/30/2019",
            description = "Jobs with entry point binaries containing spaces are likely relying on the legacy shell parsing " +
                    "being done by titus-executor, and are submitting entry points as a flat string, instead of breaking it into a list of arguments. " +
                    "Jobs that have a command set will fall on the new code path that does not do any shell parsing, and does not need to be checked."
    )
    String ENTRY_POINT_STRICT_VALIDATION_FEATURE = "entryPointStrictValidation";

    @FeatureRollout(
            featureId = "disruptionBudget",
            deadline = "03/30/2019",
            description = "Controls the job disruption budget rollout process."
    )
    String DISRUPTION_BUDGET_FEATURE = "disruptionBudget";

    @FeatureRollout(
            featureId = "environmentVariableNamesStrictValidation",
            deadline = "06/30/2019",
            description = "Restricts the environment variables names to ASCII letters, digits and '_', according to the http://pubs.opengroup.org/onlinepubs/9699919799 spec"
    )
    String ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE = "environmentVariableNamesStrictValidation";

    @FeatureRollout(
            featureId = "minDiskSizeStrictValidationFeature",
            deadline = "03/30/2019",
            description = "Lower bound on the disk size."
    )
    String MIN_DISK_SIZE_STRICT_VALIDATION_FEATURE = "minDiskSizeStrictValidationFeature";

    @FeatureRollout(
            featureId = "moveTaskFeature",
            deadline = "03/30/2019",
            description = "Activation of the feature for moving tasks between jobs"
    )
    String MOVE_TASK_FEATURE = "moveTaskFeature";

    @FeatureRollout(
            featureId = "jobAuthorizationFeature",
            deadline = "06/30/2019",
            description = "Allowing job mutations for authorized users only "
    )
    String JOB_AUTHORIZATION_FEATURE = "jobAuthorizationFeature";
}
