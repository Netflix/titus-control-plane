/*
 * Copyright 2018 Netflix, Inc.
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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import javax.inject.Named;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.FeatureRolloutPlans;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobAssertions;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.feature.FeatureCompliance;
import com.netflix.titus.common.util.feature.FeatureCompliance.NonCompliance;
import com.netflix.titus.runtime.jobmanager.JobManagerConfiguration;

import static com.netflix.titus.api.FeatureRolloutPlans.ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE;
import static com.netflix.titus.api.FeatureRolloutPlans.SECURITY_GROUPS_REQUIRED_FEATURE;
import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static com.netflix.titus.common.util.feature.FeatureComplianceTypes.collectComplianceMetrics;
import static com.netflix.titus.common.util.feature.FeatureComplianceTypes.logNonCompliant;
import static com.netflix.titus.common.util.feature.FeatureComplianceTypes.mergeComplianceValidators;

/**
 * Extends the default job model sanitizer with extra checks.
 */
class ExtendedJobSanitizer implements EntitySanitizer {

    private static final String TITUS_NON_COMPLIANT_ROOT_NAME = "titus.noncompliant";

    private static final String TITUS_NON_COMPLIANT = TITUS_NON_COMPLIANT_ROOT_NAME + ".";

    @VisibleForTesting
    static final String TITUS_NON_COMPLIANT_FEATURES = TITUS_NON_COMPLIANT + "features";

    private final JobManagerConfiguration jobManagerConfiguration;
    private final EntitySanitizer entitySanitizer;
    private final DisruptionBudgetSanitizer disruptionBudgetSanitizer;
    private final Predicate<JobDescriptor> securityGroupsRequiredPredicate;
    private final Predicate<JobDescriptor> environmentVariableNamesStrictValidationPredicate;
    private final FeatureCompliance<JobDescriptor<?>> jobComplianceChecker;

    public ExtendedJobSanitizer(JobManagerConfiguration jobManagerConfiguration,
                                JobAssertions jobAssertions,
                                @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer,
                                DisruptionBudgetSanitizer disruptionBudgetSanitizer,
                                @Named(SECURITY_GROUPS_REQUIRED_FEATURE) Predicate<JobDescriptor> securityGroupsRequiredPredicate,
                                @Named(ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE) Predicate<JobDescriptor> environmentVariableNamesStrictValidationPredicate,
                                TitusRuntime titusRuntime) {
        this.jobManagerConfiguration = jobManagerConfiguration;
        this.entitySanitizer = entitySanitizer;
        this.disruptionBudgetSanitizer = disruptionBudgetSanitizer;
        this.securityGroupsRequiredPredicate = securityGroupsRequiredPredicate;
        this.environmentVariableNamesStrictValidationPredicate = environmentVariableNamesStrictValidationPredicate;

        this.jobComplianceChecker = logNonCompliant(collectComplianceMetrics(titusRuntime.getRegistry(),
                mergeComplianceValidators(
                        JobFeatureComplianceChecks.missingSecurityGroups(),
                        JobFeatureComplianceChecks.missingIamRole(),
                        JobFeatureComplianceChecks.environmentVariablesNames(jobAssertions),
                        JobFeatureComplianceChecks.entryPointViolations(),
                        JobFeatureComplianceChecks.minDiskSize(jobManagerConfiguration),
                        JobFeatureComplianceChecks.noDisruptionBudget()
                ))
        );
    }

    @Override
    public <T> Set<ValidationError> validate(T entity) {
        return entitySanitizer.validate(entity);
    }

    @Override
    public <T> Optional<T> sanitize(T entity) {
        T sanitized = entitySanitizer.sanitize(entity).orElse(entity);

        if (sanitized instanceof com.netflix.titus.api.jobmanager.model.job.JobDescriptor) {
            sanitized = (T) sanitizeJobDescriptor((JobDescriptor) sanitized);
        }

        return entity == sanitized ? Optional.empty() : Optional.of(sanitized);
    }

    private JobDescriptor<?> sanitizeJobDescriptor(JobDescriptor<?> providedJobDescriptor) {
        JobDescriptor<?> jobDescriptorWithAllowedAttributes = resetAttributes(providedJobDescriptor);

        return jobComplianceChecker.checkCompliance(jobDescriptorWithAllowedAttributes).map(violations -> {

            JobDescriptor sanitized = jobDescriptorWithAllowedAttributes;

            if (!securityGroupsRequiredPredicate.test(jobDescriptorWithAllowedAttributes)) {
                // Missing security groups
                SecurityProfile.Builder securityProfileBuilder = jobDescriptorWithAllowedAttributes.getContainer().getSecurityProfile().toBuilder();
                violations.findViolation(SECURITY_GROUPS_REQUIRED_FEATURE).ifPresent(report ->
                        securityProfileBuilder.withSecurityGroups(jobManagerConfiguration.getDefaultSecurityGroups())
                );

                // Missing IAM role
                violations.findViolation(FeatureRolloutPlans.IAM_ROLE_REQUIRED_FEATURE).ifPresent(report ->
                        securityProfileBuilder.withIamRole(jobManagerConfiguration.getDefaultIamRole())
                );

                sanitized = sanitized.toBuilder()
                        .withContainer(sanitized.getContainer().toBuilder()
                                .withSecurityProfile(securityProfileBuilder.build())
                                .build()
                        ).build();
            }

            // Min disk size
            NonCompliance<JobDescriptor<?>> diskSizeViolation = violations.findViolation(FeatureRolloutPlans.MIN_DISK_SIZE_STRICT_VALIDATION_FEATURE).orElse(null);
            if (diskSizeViolation != null) {
                ContainerResources containerResources = sanitized.getContainer().getContainerResources();
                sanitized = sanitized.toBuilder().withContainer(sanitized.getContainer().toBuilder()
                        .withContainerResources(
                                containerResources.toBuilder().withDiskMB(jobManagerConfiguration.getMinDiskSizeMB()).build()
                        ).build()

                ).build();
            }

            // TODO Once not needed, remove this code and add the field level validator which invokes method JobAssertions#validateEnvironmentVariableNames.
            // We have to throw the exception here, as we cannot conditionally check violations using annotations.
            violations.findViolation(ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE).ifPresent(nonCompliance -> {
                if (environmentVariableNamesStrictValidationPredicate.test(jobDescriptorWithAllowedAttributes)) {
                    throw TitusServiceException.invalidArgument(nonCompliance.toErrorMessage());
                }
            });

            // Set default disruption budget if not set
            sanitized = disruptionBudgetSanitizer.sanitize(sanitized);

            return sanitized.toBuilder()
                    .withAttributes(CollectionsExt.merge(jobDescriptorWithAllowedAttributes.getAttributes(), buildNonComplianceJobAttributeMap(violations)))
                    .build();
        }).orElse(jobDescriptorWithAllowedAttributes);
    }

    private JobDescriptor<?> resetAttributes(JobDescriptor<?> jobDescriptor) {
        Map<String, String> attributes = jobDescriptor.getAttributes();
        if (attributes.isEmpty()) {
            return jobDescriptor;
        }

        Map<String, String> allowedAttributes = new HashMap<>();
        attributes.forEach((key, value) -> {
            if (!key.startsWith(TITUS_NON_COMPLIANT_ROOT_NAME)) {
                allowedAttributes.put(key, value);
            }
        });

        return allowedAttributes.size() == attributes.size()
                ? jobDescriptor
                : jobDescriptor.toBuilder().withAttributes(allowedAttributes).build();
    }

    private Map<String, String> buildNonComplianceJobAttributeMap(FeatureCompliance.NonComplianceList<JobDescriptor<?>> violations) {
        StringBuilder violatedFeaturesBuilder = new StringBuilder();
        Map<String, String> violationJobAttributes = new HashMap<>();

        violations.getViolations().forEach(violation -> {
            violatedFeaturesBuilder.append(violation.getFeatureId()).append(',');

            String detailsPrefix = TITUS_NON_COMPLIANT + "details." + violation.getFeatureId() + '.';
            violation.getContext().forEach((key, value) -> {
                violationJobAttributes.put(detailsPrefix + key, value);
            });
        });
        violationJobAttributes.put(TITUS_NON_COMPLIANT_FEATURES, violatedFeaturesBuilder.substring(0, violatedFeaturesBuilder.length() - 1));

        return violationJobAttributes;
    }
}
