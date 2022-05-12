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
import java.util.function.UnaryOperator;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.common.model.admission.AdmissionSanitizer;
import com.netflix.titus.common.model.admission.AdmissionValidator;
import com.netflix.titus.common.model.admission.ValidatorMetrics;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.runtime.connector.registry.RegistryClient;
import com.netflix.titus.runtime.connector.registry.TitusRegistryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * This {@link AdmissionValidator} implementation validates and sanitizes Job image information.
 */
@Singleton
public class JobImageSanitizer implements AdmissionSanitizer<JobDescriptor> {
    private static final Logger logger = LoggerFactory.getLogger(JobImageSanitizer.class);

    private final JobImageSanitizerConfiguration configuration;
    private final RegistryClient registryClient;
    private final ValidatorMetrics validatorMetrics;

    @Inject
    public JobImageSanitizer(JobImageSanitizerConfiguration configuration, RegistryClient registryClient, Registry spectatorRegistry) {
        this.configuration = configuration;
        this.registryClient = registryClient;
        this.validatorMetrics = new ValidatorMetrics(this.getClass().getSimpleName(), spectatorRegistry);
    }

    /**
     * @return a {@link UnaryOperator} that adds a sanitized Image or job attributes when sanitization was skipped
     */
    @Override
    public Mono<UnaryOperator<JobDescriptor>> sanitize(JobDescriptor jobDescriptor) {
        if (isDisabled()) {
            return Mono.just(JobImageSanitizer::skipSanitization);
        }

        Image image = jobDescriptor.getContainer().getImage();
        return sanitizeImage(jobDescriptor)
                .map(JobImageSanitizer::setImageFunction)
                .timeout(Duration.ofMillis(configuration.getJobImageValidationTimeoutMs()))
                .doOnSuccess(j -> validatorMetrics.incrementValidationSuccess(image.getName()))
                .onErrorReturn(throwable -> isAllowedException(throwable, image), JobImageSanitizer::skipSanitization)
                .onErrorMap(Exception.class, error -> new IllegalArgumentException(String.format(
                        "Image validation error: image=%s, cause=%s",
                        image, error.getMessage()
                ), error));
    }

    private static UnaryOperator<JobDescriptor> setImageFunction(Image image) {
        return entity -> entity.toBuilder()
                .withContainer(entity.getContainer().toBuilder()
                        .withImage(image)
                        .build())
                .build();
    }

    private Mono<Image> sanitizeImage(JobDescriptor jobDescriptor) {
        Image image = jobDescriptor.getContainer().getImage();
        if (StringExt.isNotEmpty(image.getDigest())) {
            return Mono.empty();
        }
        return registryClient.getImageDigest(image.getName(), image.getTag())
                .map(digest -> image.toBuilder().withDigest(digest).build());
    }

    private Mono<String> checkImageDigestExist(Image image) {
        return registryClient.getImageDigest(image.getName(), image.getDigest());
    }

    private boolean isDisabled() {
        return !configuration.isEnabled();
    }

    /**
     * Determines if this Exception should fail open, or produce a sanitization failure
     */
    private boolean isAllowedException(Throwable throwable, Image image) {
        logger.error("Exception while checking image digest: {}", throwable.getMessage());
        logger.debug("Full stacktrace", throwable);

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
        // unknown errors should prevent job creation
        return false;
    }

    @SuppressWarnings("unchecked")
    private static JobDescriptor skipSanitization(JobDescriptor jobDescriptor) {
        return JobFunctions.appendJobDescriptorAttribute(jobDescriptor,
                JobAttributes.JOB_ATTRIBUTES_SANITIZATION_SKIPPED_IMAGE, true
        );
    }
}
