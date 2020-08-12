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

import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.common.model.admission.AdmissionSanitizer;
import com.netflix.titus.common.model.admission.ValidatorMetrics;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.runtime.connector.registry.RegistryClient;
import com.netflix.titus.runtime.connector.registry.TitusRegistryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.function.UnaryOperator;

/**
 * This {@link AdmissionSanitizer} implementation validates and sanitizes service mesh image attributes.
 */
@Singleton
public class ServiceMeshImageSanitizer implements AdmissionSanitizer<JobDescriptor> {
    private static final Logger logger = LoggerFactory.getLogger(ServiceMeshImageSanitizer.class);

    private final ServiceMeshImageSanitizerConfiguration configuration;
    private final RegistryClient registryClient;
    private final ValidatorMetrics validatorMetrics;

    @Inject
    public ServiceMeshImageSanitizer(ServiceMeshImageSanitizerConfiguration configuration, RegistryClient registryClient, Registry spectatorRegistry) {
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
            return Mono.just(ServiceMeshImageSanitizer::skipSanitization);
        }

        if (!serviceMeshIsEnabled(jobDescriptor)) {
            validatorMetrics.incrementValidationSkipped("serviceMeshNotEnabled");
            return Mono.just(UnaryOperator.identity());
        }

        if (!serviceMeshIsPinned(jobDescriptor)) {
            validatorMetrics.incrementValidationSkipped("serviceMeshNotPinned");
            return Mono.just(UnaryOperator.identity());
        }

        try {
            Image image = getServiceMeshImage(jobDescriptor);
            return sanitizeServiceMeshImage(image)
                    .map(ServiceMeshImageSanitizer::setMeshImageFunction)
                    .timeout(Duration.ofMillis(configuration.getServiceMeshImageValidationTimeoutMs()))
                    .doOnSuccess(j -> validatorMetrics.incrementValidationSuccess(image.getName()))
                    .onErrorReturn(throwable -> isAllowedException(throwable, image), ServiceMeshImageSanitizer::skipSanitization);
        } catch (Throwable t) {
            return Mono.error(t);
        }
    }

    private static UnaryOperator<JobDescriptor> setMeshImageFunction(Image image) {
        String imageName = toImageName(image);
        return jobDescriptor -> JobFunctions.appendJobDescriptorAttribute(jobDescriptor,
                JobAttributes.JOB_CONTAINER_ATTRIBUTE_SERVICEMESH_CONTAINER, imageName);
    }

    private Mono<Image> sanitizeServiceMeshImage(Image image) {
        if (StringExt.isNotEmpty(image.getDigest())) {
            return checkImageDigestExist(image).then(Mono.empty());
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

    private boolean serviceMeshIsEnabled(JobDescriptor<?> jobDescriptor) {
        String enabled = jobDescriptor
                .getAttributes()
                .get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_SERVICEMESH_ENABLED);

        if (enabled == null) {
            return false;
        }

        return Boolean.parseBoolean(enabled);
    }

    private boolean serviceMeshIsPinned(JobDescriptor<?> jobDescriptor) {
        return jobDescriptor
                .getAttributes()
                .containsKey(JobAttributes.JOB_CONTAINER_ATTRIBUTE_SERVICEMESH_CONTAINER);
    }

    private Image getServiceMeshImage(JobDescriptor<?> jobDescriptor) {
        String image = jobDescriptor
                .getAttributes()
                .get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_SERVICEMESH_CONTAINER);

        return parseImageName(image);
    }

    private static Image parseImageName(String imageName) {
        // Grammar
        //
        //  reference                       := name [ ":" tag ] [ "@" digest ]
        //  name                            := [hostname '/'] component ['/' component]*
        //  hostname                        := hostcomponent ['.' hostcomponent]* [':' port-number]
        //  hostcomponent                   := /([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])/
        //  port-number                     := /[0-9]+/
        //  component                       := alpha-numeric [separator alpha-numeric]*
        //  alpha-numeric                   := /[a-z0-9]+/
        //  separator                       := /[_.]|__|[-]*/
        //
        //  tag                             := /[\w][\w.-]{0,127}/
        //
        //  digest                          := digest-algorithm ":" digest-hex
        //  digest-algorithm                := digest-algorithm-component [ digest-algorithm-separator digest-algorithm-component ]
        //  digest-algorithm-separator      := /[+.-_]/
        //  digest-algorithm-component      := /[A-Za-z][A-Za-z0-9]*/
        //  digest-hex                      := /[0-9a-fA-F]{32,}/ ; At least 128 bit digest value


        int digestStart = imageName.lastIndexOf("@");
        if (digestStart < 0) {
            int tagStart = imageName.lastIndexOf(":");
            if (tagStart < 0) {
                throw new IllegalArgumentException("cannot parse " + imageName + " as versioned docker image name");
            }

            String name = imageName.substring(0, tagStart);
            String tag = imageName.substring(tagStart + 1);
            return Image.newBuilder().withName(name).withTag(tag).build();
        } else {
            String name = imageName.substring(0, digestStart);
            String digest = imageName.substring(digestStart + 1);
            return Image.newBuilder().withName(name).withDigest(digest).build();
        }
    }

    private static String toImageName(Image image) {
        return String.format("%s@%s", image.getName(), image.getDigest());
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

        // Use a more specific error tag if available
        if (throwable instanceof TitusRegistryException) {
            TitusRegistryException tre = (TitusRegistryException) throwable;
            validatorMetrics.incrementValidationError(
                    imageResource,
                    tre.getErrorCode().name());

            return tre.getErrorCode() != TitusRegistryException.ErrorCode.IMAGE_NOT_FOUND;
        } else {
            validatorMetrics.incrementValidationError(
                    imageResource,
                    throwable.getClass().getSimpleName());
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    private static JobDescriptor<?> skipSanitization(JobDescriptor<?> jobDescriptor) {
        return JobFunctions.appendJobDescriptorAttribute(jobDescriptor,
                JobAttributes.JOB_ATTRIBUTES_SANITIZATION_SKIPPED_SERVICEMESH_IMAGE, true
        );
    }
}
