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

package com.netflix.titus.runtime.endpoint.validator;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.connector.cloud.IamConnector;
import com.netflix.titus.api.iam.model.IamRole;
import com.netflix.titus.api.iam.service.IamConnectorException;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * This {@link EntityValidator} implementation validates and sanitizes Job IAM information.
 */
@Singleton
public class JobIamValidator implements EntityValidator<JobDescriptor> {
    private static final Logger logger = LoggerFactory.getLogger(JobImageValidator.class);
    private static final String METRICS_ROOT = "titus.validation.iam";
    private static final String failedMetricIamTag = "iamrole";
    private static final String skippedMetricReasonTag = "reason";

    private final JobSecurityValidatorConfiguration configuration;
    private final IamConnector iamConnector;

    private final Registry registry;
    private final Id validationFailureId;
    private final Id validationSkippedId;

    @Inject
    public JobIamValidator(JobSecurityValidatorConfiguration configuration, IamConnector iamConnector, Registry registry) {
        this.configuration = configuration;
        this.iamConnector = iamConnector;
        this.registry = registry;
        this.validationFailureId = registry.createId(METRICS_ROOT + ".failed");
        this.validationSkippedId = registry.createId(METRICS_ROOT + ".skipped");
    }

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor jobDescriptor) {
        if (isDisabled()) {
            return Mono.just(Collections.emptySet());
        }

        String iamRoleName = jobDescriptor.getContainer().getSecurityProfile().getIamRole();

        // Skip validation if no IAM was provided because a valid default will be used.
        if (iamRoleName.isEmpty()) {
            registry.counter(validationSkippedId.withTag(skippedMetricReasonTag, "noneProvided")).increment();
            return Mono.just(Collections.emptySet());
        }

        // Skip any IAM that is not in "friendly" format. A non-friendly format is
        // likely a cross-account IAM and would need cross-account access to get and validate.
        if (!isFriendlyName(iamRoleName)) {
            registry.counter(validationSkippedId.withTag(skippedMetricReasonTag, "notFriendly")).increment();
            return Mono.just(Collections.emptySet());
        }

        return iamConnector.getIamRole(iamRoleName)
                .timeout(Duration.ofMillis(configuration.getIamValidationTimeoutMs()))
                .flatMap(iamRole -> validateAssumePolicy(iamRole, configuration.getAgentIamAssumeRole()))
                // If role is found and is assumable return an empty ValidationError set, otherwise
                // populate the set with a specific error.
                .thenReturn(Collections.<ValidationError>emptySet())
                .onErrorResume(throwable -> {
                    registry.counter(validationFailureId.withTag(failedMetricIamTag, iamRoleName)).increment();
                    return Mono.just(Collections.singleton(
                            new ValidationError(
                                    JobIamValidator.class.getSimpleName(),
                                    throwable.getMessage(),
                                    ValidationError.Type.SOFT)));
                });
    }

    /**
     * We do not expect to sanitize the IAM at the moment, this is a noop.
     */
    @Override
    public Mono<JobDescriptor> sanitize(JobDescriptor jobDescriptor) {
        return Mono.just(jobDescriptor);
    }

    /**
     * Validates that the provided assume role name can assume into the IAM role.
     * The Mono completes if assume is possible and emits error otherwise.
     */
    private Mono<Void> validateAssumePolicy(IamRole iamRole, String iamAssumeRoleName) {
        if (iamRole.canAssume(iamAssumeRoleName)) {
            return Mono.empty();
        }
        return Mono.error(new IamConnectorException(
                IamConnectorException.ErrorCode.INVALID,
                String.format("Titus cannot assume into role %s: %s unable to assumeRole",
                        iamRole.getRoleName(), iamAssumeRoleName)
        ));
    }

    private boolean isFriendlyName(String iamRoleName) {
        // Check if this looks like an ARN
        return !(iamRoleName.startsWith("arn:aws:") ||
                // Check if this looks like a path name
                iamRoleName.contains("/"));
    }

    private boolean isDisabled() { return !configuration.iamValidatorEnabled(); }
}
