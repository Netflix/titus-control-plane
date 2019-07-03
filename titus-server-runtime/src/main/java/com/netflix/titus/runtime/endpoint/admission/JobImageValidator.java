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

package com.netflix.titus.runtime.endpoint.admission;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.runtime.connector.registry.RegistryClient;
import com.netflix.titus.runtime.connector.registry.TitusRegistryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * This {@link EntityValidator} implementation validates and sanitizes Job image information.
 */
@Singleton
public class JobImageValidator implements EntityValidator<JobDescriptor> {
    private static final Logger logger = LoggerFactory.getLogger(JobImageValidator.class);

    private final JobImageValidatorConfiguration configuration;
    private final RegistryClient registryClient;
    private final ValidatorMetrics validatorMetrics;

    @Inject
    public JobImageValidator(JobImageValidatorConfiguration configuration, RegistryClient registryClient, Registry spectatorRegistry) {
        this.configuration = configuration;
        this.registryClient = registryClient;
        this.validatorMetrics = new ValidatorMetrics(this.getClass().getSimpleName(), spectatorRegistry);
    }

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor jobDescriptor) {
        if (isDisabled()) {
            return Mono.just(Collections.emptySet());
        }

        Mono<Void> imageExistsMono;

        Image image = jobDescriptor.getContainer().getImage();
        if (hasDigest(image)) {
            imageExistsMono = checkImageDigestExist(image);
        } else {
            imageExistsMono = checkImageTagExists(image);
        }

        return imageExistsMono
                .then(Mono.just(Collections.<ValidationError>emptySet()))
                .doOnSuccess(j -> validatorMetrics.incrementValidationSuccess(image.getName()))
                .onErrorResume(throwable -> {
                    if (isValidationOK(throwable, image)) {
                        return Mono.just(Collections.emptySet());
                    }
                    return Mono.just(
                            Collections.singleton(
                                    new ValidationError(
                                            JobImageValidator.class.getSimpleName(),
                                            throwable.getMessage(),
                                            configuration.toValidatorErrorType())
                            ));
                });
    }

    @Override
    public Mono<JobDescriptor> sanitize(JobDescriptor jobDescriptor) {
        if (isDisabled()) {
            return Mono.just(withSanitizationSkipped(jobDescriptor));
        }

        Image image = jobDescriptor.getContainer().getImage();
        return sanitizeImage(jobDescriptor)
                .timeout(Duration.ofMillis(configuration.getJobImageValidationTimeoutMs()))
                .doOnSuccess(j -> validatorMetrics.incrementValidationSuccess(image.getName()))
                .onErrorReturn(throwable -> isValidationOK(throwable, image), withSanitizationSkipped(jobDescriptor));
    }

    @Override
    public ValidationError.Type getErrorType() {
        return configuration.toValidatorErrorType();
    }

    private Mono<JobDescriptor> sanitizeImage(JobDescriptor jobDescriptor) {
        // Check if a digest is provided
        // If so, check if it exists
        Image image = jobDescriptor.getContainer().getImage();
        if (hasDigest(image)) {
            return checkImageDigestExist(image)
                    .thenReturn(jobDescriptor);
        }

        // If not, resolve to digest and return error if not found
        return addMissingImageDigest(jobDescriptor);
    }

    private Mono<Void> checkImageTagExists(Image image) {
        return registryClient.getImageDigest(image.getName(), image.getTag()).then();
    }

    private Mono<Void> checkImageDigestExist(Image image) {
        return registryClient.getImageDigest(image.getName(), image.getDigest()).then();
    }

    private Mono<JobDescriptor> addMissingImageDigest(JobDescriptor jobDescriptor) {
        Image image = jobDescriptor.getContainer().getImage();
        if (hasDigest(image)) {
            return Mono.just(jobDescriptor);
        }

        return registryClient.getImageDigest(image.getName(), image.getTag())
                .map(digest -> jobDescriptor.toBuilder()
                        .withContainer(jobDescriptor.getContainer().toBuilder()
                                .withImage(Image.newBuilder()
                                        .withName(image.getName())
                                        .withTag(image.getTag())
                                        .withDigest(digest)
                                        .build()).build()
                        ).build());
    }

    private boolean hasDigest(Image image) {
        return null != image.getDigest() && !image.getDigest().isEmpty();
    }

    private boolean isDisabled() {
        return !configuration.isEnabled();
    }

    // Determines if this Exception should produce a validation failure
    private boolean isValidationOK(Throwable throwable, Image image) {
        logger.error("Exception while checking image digest", throwable);

        String imageName = image.getName();
        String imageVersion = image.getDigest().isEmpty() ? image.getTag() : image.getDigest();
        String imageResource = String.format("%s_%s", imageName, imageVersion);
        if (throwable instanceof TitusRegistryException) {
            TitusRegistryException tre = (TitusRegistryException) throwable;
            // Use a more specific error tag if available
            validatorMetrics.incrementValidationError(
                    imageResource,
                    tre.getErrorCode().name()
            );
            // We are ignoring most image validation errors. We will filter
            // fewer errors as we gain feature confidence.
            switch (tre.getErrorCode()) {
                case IMAGE_NOT_FOUND:
                    return false;
                default:
                    return true;
            }
        }
        validatorMetrics.incrementValidationError(
                imageResource,
                throwable.getClass().getSimpleName()
        );
        return true;
    }

    @SuppressWarnings("unchecked")
    private static JobDescriptor withSanitizationSkipped(JobDescriptor jobDescriptor) {
        return JobFunctions.appendJobDescriptorAttribute(jobDescriptor,
                JobAttributes.JOB_ATTRIBUTES_SANITIZATION_SKIPPED_IMAGE, true
        );
    }
}
