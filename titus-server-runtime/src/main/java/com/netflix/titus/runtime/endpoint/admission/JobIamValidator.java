/*
 *
 *  * Copyright 2019 Netflix, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netflix.titus.runtime.endpoint.admission;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.function.UnaryOperator;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.connector.cloud.IamConnector;
import com.netflix.titus.api.iam.model.IamRole;
import com.netflix.titus.api.iam.service.IamConnectorException;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import reactor.core.publisher.Mono;

/**
 * This {@link AdmissionValidator} implementation validates and sanitizes Job IAM information.
 */
@Singleton
public class JobIamValidator implements AdmissionValidator<JobDescriptor>, AdmissionSanitizer<JobDescriptor> {
    private final JobSecurityValidatorConfiguration configuration;
    private final IamConnector iamConnector;
    private final ValidatorMetrics validatorMetrics;

    @Inject
    public JobIamValidator(JobSecurityValidatorConfiguration configuration, IamConnector iamConnector, Registry registry) {
        this.configuration = configuration;
        this.iamConnector = iamConnector;

        validatorMetrics = new ValidatorMetrics(this.getClass().getSimpleName(), registry);
    }

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor jobDescriptor) {
        if (isDisabled()) {
            return Mono.just(Collections.emptySet());
        }

        String iamRoleName = jobDescriptor.getContainer().getSecurityProfile().getIamRole();

        // Skip validation if no IAM was provided because a valid default will be used.
        if (iamRoleName.isEmpty()) {
            validatorMetrics.incrementValidationSkipped("noIamProvided");
            return Mono.just(Collections.emptySet());
        }

        // Skip any IAM that is not in "friendly" format. A non-friendly format is
        // likely a cross-account IAM and would need cross-account access to get and validate.
        if (isIamArn(iamRoleName)) {
            validatorMetrics.incrementValidationSkipped(iamRoleName, "notFriendly");
            return Mono.just(Collections.emptySet());
        }

        return iamConnector.canIamAssume(iamRoleName, configuration.getAgentIamAssumeRole())
                .timeout(Duration.ofMillis(configuration.getIamValidationTimeoutMs()))
                // If role is found and is assumable return an empty ValidationError set, otherwise
                // populate the set with a specific error.
                .thenReturn(Collections.<ValidationError>emptySet())
                .doOnSuccess(result -> validatorMetrics.incrementValidationSuccess(iamRoleName))
                .onErrorResume(throwable -> {
                    String errorReason = throwable.getClass().getSimpleName();
                    if (throwable instanceof IamConnectorException) {
                        // Use a more specific error tag if available
                        errorReason = ((IamConnectorException) throwable).getErrorCode().name();
                    }
                    validatorMetrics.incrementValidationError(iamRoleName, errorReason);
                    return Mono.just(Collections.singleton(
                            new ValidationError(
                                    JobIamValidator.class.getSimpleName(),
                                    throwable.getMessage(),
                                    getErrorType())));
                });
    }

    @Override
    public ValidationError.Type getErrorType() {
        return configuration.toValidatorErrorType();
    }

    /**
     * @return a {@link UnaryOperator} that adds a sanitized role or job attributes when sanitization was skipped
     */
    @Override
    public Mono<UnaryOperator<JobDescriptor>> sanitize(JobDescriptor jobDescriptor) {
        if (isDisabled()) {
            return Mono.just(JobIamValidator::skipSanitization);
        }

        String iamRoleName = jobDescriptor.getContainer().getSecurityProfile().getIamRole();

        // If empty, it should be set to ARN value or rejected, but not in this place.
        if (iamRoleName.isEmpty()) {
            return Mono.empty();
        }
        if (isIamArn(iamRoleName)) {
            return Mono.empty();
        }

        return iamConnector.getIamRole(iamRoleName)
                .timeout(Duration.ofMillis(configuration.getIamValidationTimeoutMs()))
                .map(JobIamValidator::setIamRoleFunction)
                .onErrorReturn(JobIamValidator::skipSanitization);
    }

    private static UnaryOperator<JobDescriptor> setIamRoleFunction(IamRole iamRole) {
        return entity -> {
            Container container = entity.getContainer();
            return entity.toBuilder()
                    .withContainer(container.toBuilder()
                            .withSecurityProfile(container.getSecurityProfile().toBuilder()
                                    .withIamRole(iamRole.getResourceName())
                                    .build())
                            .build()
                    )
                    .build();
        };
    }

    /**
     * Check if this looks like an ARN
     */
    private boolean isIamArn(String iamRoleName) {
        return iamRoleName.startsWith("arn:aws:");
    }

    private boolean isDisabled() {
        return !configuration.isIamValidatorEnabled();
    }

    @SuppressWarnings("unchecked")
    private static JobDescriptor skipSanitization(JobDescriptor jobDescriptor) {
        return JobFunctions.appendJobDescriptorAttribute(jobDescriptor,
                JobAttributes.JOB_ATTRIBUTES_SANITIZATION_SKIPPED_IAM, true
        );
    }
}
