/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.ext.jobvalidator.s3;

import java.util.Set;
import java.util.function.Function;

import com.netflix.compute.validator.protogen.ComputeValidator;
import com.netflix.compute.validator.protogen.ComputeValidator.S3BucketAccessValidationResponse;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Test;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobS3LogLocationValidatorTest {

    private static final JobDescriptor<?> JOB_WITH_DEFAULT_BUCKET = JobDescriptorGenerator.oneTaskBatchJobDescriptor();
    private static final JobDescriptor<?> JOB_WITH_CUSTOM_BUCKET = JobDescriptorGenerator.oneTaskBatchJobDescriptor().toBuilder()
            .withContainer(JOB_WITH_DEFAULT_BUCKET.getContainer().toBuilder()
                    .withAttributes(CollectionsExt.asMap(
                            JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_BUCKET_NAME, "junitBucket",
                            JobAttributes.JOB_CONTAINER_ATTRIBUTE_S3_PATH_PREFIX, "junitPrefix"
                    ))
                    .build()
            )
            .build();


    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final ReactorValidationServiceClient validationClient = mock(ReactorValidationServiceClient.class);

    private final JobS3LogLocationValidator validator = new JobS3LogLocationValidator(
            validationClient,
            "defaultBucket",
            "defaultPrefix",
            Function.identity(),
            () -> ValidationError.Type.SOFT,
            () -> true,
            titusRuntime
    );

    @Test
    public void testNoCustomBucket() {
        when(validationClient.validateS3BucketAccess(any())).thenReturn(
                Mono.just(S3BucketAccessValidationResponse.getDefaultInstance())
        );
        Set<ValidationError> result = validator.validate(JOB_WITH_DEFAULT_BUCKET).block();
        assertThat(result).isEmpty();
        verify(validationClient, times(0)).validateS3BucketAccess(any());
    }

    @Test
    public void testValidCustomBucket() {
        when(validationClient.validateS3BucketAccess(any())).thenReturn(
                Mono.just(S3BucketAccessValidationResponse.getDefaultInstance())
        );
        Set<ValidationError> result = validator.validate(JOB_WITH_CUSTOM_BUCKET).block();
        assertThat(result).isEmpty();
        verify(validationClient, times(1)).validateS3BucketAccess(any());
    }

    @Test
    public void testInValidCustomBucket() {
        when(validationClient.validateS3BucketAccess(any())).thenReturn(
                Mono.just(S3BucketAccessValidationResponse.newBuilder()
                        .setFailures(ComputeValidator.ValidationFailures.newBuilder()
                                .addFailures(ComputeValidator.ValidationFailure.getDefaultInstance())
                                .build()
                        )
                        .build()
                )
        );
        Set<ValidationError> result = validator.validate(JOB_WITH_CUSTOM_BUCKET).block();
        assertThat(result).hasSize(1);
        verify(validationClient, times(1)).validateS3BucketAccess(any());
    }
}