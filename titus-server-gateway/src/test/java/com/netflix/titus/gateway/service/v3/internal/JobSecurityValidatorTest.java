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

package com.netflix.titus.gateway.service.v3.internal;

import java.util.Set;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.connector.cloud.IamConnector;
import com.netflix.titus.api.iam.service.IamConnectorException;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.common.model.validator.ValidationError;
import com.netflix.titus.runtime.endpoint.validator.JobIamValidator;
import com.netflix.titus.runtime.endpoint.validator.JobSecurityValidatorConfiguration;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobSecurityValidatorTest {
    private static final String VALID_IAM_ROLE_NAME = "myValidIamRole";
    private static final String INVALID_IAM_ROLE_NAME = "myInvalidIamRole";
    private static final String IAM_ASSUME_ROLE_NAME = "myIamAssumeRole";

    private static final String CANNOT_ASSUME_IAM_ERROR_MSG = "Titus cannot assume into role";
    private static final String MISSING_IAM_ERROR_MSG = "Could not find IAM";

    private final JobSecurityValidatorConfiguration configuration = mock(JobSecurityValidatorConfiguration.class);
    private final IamConnector iamConnector = mock(IamConnector.class);
    private JobIamValidator iamValidator;

    private final JobDescriptor<?> jobDescriptorWithValidIam = JobDescriptorGenerator.batchJobDescriptors()
            .map(jd -> jd.but(d -> d.getContainer().toBuilder()
            .withSecurityProfile(SecurityProfile.newBuilder()
                    .withIamRole(VALID_IAM_ROLE_NAME)
                    .build())
            ))
            .getValue();

    private final JobDescriptor<?> jobDescriptorWithInvalidIam = JobDescriptorGenerator.batchJobDescriptors()
            .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                    .withSecurityProfile(SecurityProfile.newBuilder()
                            .withIamRole(INVALID_IAM_ROLE_NAME)
                            .build())
            ))
            .getValue();

    @Before
    public void setUp() {
        when(configuration.isIamValidatorEnabled()).thenReturn(true);
        when(configuration.getAgentIamAssumeRole()).thenReturn(IAM_ASSUME_ROLE_NAME);
        when(configuration.getIamValidationTimeoutMs()).thenReturn(10000L);
        when(configuration.getErrorType()).thenReturn(ValidationError.Type.HARD.name());
        iamValidator = new JobIamValidator(configuration, iamConnector, new DefaultRegistry());
    }

    @Test
    public void testJobWithValidIam() {
        when(iamConnector.canIamAssume(VALID_IAM_ROLE_NAME, IAM_ASSUME_ROLE_NAME)).thenReturn(Mono.empty());

        StepVerifier.create(iamValidator.validate(jobDescriptorWithValidIam))
                .assertNext(validationErrors -> assertThat(validationErrors.isEmpty()).isTrue())
                .verifyComplete();
    }

    @Test
    public void testJobWithUnassumableIam() {
        String errorMsg = String.format("%s %s", CANNOT_ASSUME_IAM_ERROR_MSG, INVALID_IAM_ROLE_NAME);

        when(iamConnector.canIamAssume(INVALID_IAM_ROLE_NAME, IAM_ASSUME_ROLE_NAME))
                .thenReturn(Mono.error(IamConnectorException.iamRoleCannotAssume(INVALID_IAM_ROLE_NAME, IAM_ASSUME_ROLE_NAME)));

        StepVerifier.create(iamValidator.validate(jobDescriptorWithInvalidIam))
                .assertNext(validationErrors -> validationErrorsContainsJust(validationErrors, errorMsg))
                .verifyComplete();
    }

    @Test
    public void testJobWithNonexistentIam() {
        String errorMsg = String.format("%s %s", MISSING_IAM_ERROR_MSG, INVALID_IAM_ROLE_NAME);

        when(iamConnector.canIamAssume(INVALID_IAM_ROLE_NAME, IAM_ASSUME_ROLE_NAME))
                .thenReturn(Mono.error(IamConnectorException.iamRoleNotFound(INVALID_IAM_ROLE_NAME)));

        Mono<Set<ValidationError>> validationErrorsMono = iamValidator.validate(jobDescriptorWithInvalidIam);
        StepVerifier.create(validationErrorsMono)
                .assertNext(validationErrors -> validationErrorsContainsJust(validationErrors, errorMsg))
                .verifyComplete();
    }

    private void validationErrorsContainsJust(Set<ValidationError> validationErrors, String errorMessage) {
        assertThat(validationErrors).isNotNull();
        assertThat(validationErrors.size()).isEqualTo(1);
        assertThat(validationErrors).allMatch(validationError -> validationError.getDescription().startsWith(errorMessage));
    }
}
