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

package com.netflix.titus.runtime.endpoint.validator;

import java.util.Collections;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
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

    @Inject
    public JobImageValidator(JobImageValidatorConfiguration configuration, RegistryClient registryClient) {
        this.configuration = configuration;
        this.registryClient = registryClient;
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
                .onErrorResume(throwable -> Mono.just(
                        Collections.singleton(
                                new ValidationError(
                                        JobImageValidator.class.getSimpleName(),
                                        throwable.getMessage(),
                                        ValidationError.Type.SOFT)
                        )));
    }

    @Override
    public Mono<JobDescriptor> sanitize(JobDescriptor jobDescriptor) {
        if (isDisabled()) {
            return Mono.just(jobDescriptor);
        }

        return sanitizeImage(jobDescriptor)
                // We are ignoring most image validation errors. We will propagate
                // more errors as we going feature confidence.
                .onErrorReturn(throwable -> {
                    if (throwable instanceof TitusRegistryException) {
                        TitusRegistryException tre = (TitusRegistryException) throwable;
                        switch (tre.getErrorCode()) {
                            case IMAGE_NOT_FOUND:
                                return false;
                            default:
                                return true;
                        }
                    }
                    return true;
                }, jobDescriptor);
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
}
