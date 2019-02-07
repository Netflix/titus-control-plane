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
import com.netflix.titus.api.iam.model.IamRole;
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
    private static final String validIamRoleName = "myValidIamRole";
    private static final String invalidIamRoleName = "myInvalidIamRole";
    private static final String iamAssumeRoleName = "myIamAssumeRole";

    private static final String cannotAssumeIamErrorMsg = "Titus cannot assume into role";
    private static final String missingIamErrorMsg = "Could not find IAM";

    private final JobSecurityValidatorConfiguration configuration = mock(JobSecurityValidatorConfiguration.class);
    private final IamConnector iamConnector = mock(IamConnector.class);
    private JobIamValidator iamValidator;
    private final IamRole mockIamRole = mock(IamRole.class);

    private final JobDescriptor<?> jobDescriptorWithValidIam = JobDescriptorGenerator.batchJobDescriptors()
            .map(jd -> jd.but(d -> d.getContainer().toBuilder()
            .withSecurityProfile(SecurityProfile.newBuilder()
                    .withIamRole(validIamRoleName)
                    .build())
            ))
            .getValue();

    private final JobDescriptor<?> jobDescriptorWithInvalidIam = JobDescriptorGenerator.batchJobDescriptors()
            .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                    .withSecurityProfile(SecurityProfile.newBuilder()
                            .withIamRole(invalidIamRoleName)
                            .build())
            ))
            .getValue();

    @Before
    public void setUp() {
        when(configuration.iamValidatorEnabled()).thenReturn(true);
        when(configuration.getAgentIamAssumeRole()).thenReturn(iamAssumeRoleName);
        when(configuration.getIamValidationTimeoutMs()).thenReturn(10000L);
        iamValidator = new JobIamValidator(configuration, iamConnector, new DefaultRegistry());
    }

    @Test
    public void testJobWithValidIam() {
        when(mockIamRole.getRoleName()).thenReturn(validIamRoleName);
        when(mockIamRole.canAssume(iamAssumeRoleName)).thenReturn(true);
        when(iamConnector.getIamRole(validIamRoleName)).thenReturn(Mono.just(mockIamRole));

        StepVerifier.create(iamValidator.validate(jobDescriptorWithValidIam))
                .assertNext(validationErrors -> assertThat(validationErrors.isEmpty()).isTrue())
                .verifyComplete();
    }

    @Test
    public void testJobWithUnassumableIam() {
        String errorMsg = String.format("%s %s", cannotAssumeIamErrorMsg, invalidIamRoleName);

        when(mockIamRole.getRoleName()).thenReturn(invalidIamRoleName);
        when(iamConnector.getIamRole(invalidIamRoleName)).thenReturn(Mono.just(mockIamRole));
        when(mockIamRole.canAssume(iamAssumeRoleName)).thenReturn(false);

        StepVerifier.create(iamValidator.validate(jobDescriptorWithInvalidIam))
                .assertNext(validationErrors -> validationErrorsContainsJust(validationErrors, errorMsg))
                .verifyComplete();
    }

    @Test
    public void testJobWithNonexistentIam() {
        String errorMsg = String.format("%s %s", missingIamErrorMsg, invalidIamRoleName);

        when(mockIamRole.getRoleName()).thenReturn(invalidIamRoleName);
        when(iamConnector.getIamRole(invalidIamRoleName)).thenReturn(
                Mono.error(new IamConnectorException(IamConnectorException.ErrorCode.IAM_NOT_FOUND,
                        errorMsg))
        );

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
