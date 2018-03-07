/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.jobmanager.model.job.sanitizer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import io.netflix.titus.api.jobmanager.model.job.Container;
import io.netflix.titus.api.jobmanager.model.job.ContainerResources;
import io.netflix.titus.api.jobmanager.model.job.Image;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.util.StringExt;

/**
 */
public class JobAssertions {

    private static final Pattern SG_PATTERN = Pattern.compile("sg-.*");
    private static final Pattern IMAGE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9\\.\\\\/_-]+");
    private static final Pattern IMAGE_DIGEST_PATTERN = Pattern.compile("^sha256:[0-9].+");
    private static final Pattern IMAGE_TAG_PATTERN = Pattern.compile("[a-zA-Z0-9\\._-]+");

    private final Function<String, ResourceDimension> maxContainerSizeResolver;

    public JobAssertions(Function<String, ResourceDimension> maxContainerSizeResolver) {
        this.maxContainerSizeResolver = maxContainerSizeResolver;
    }

    public boolean isValidSyntax(List<String> securityGroups) {
        return securityGroups.stream().allMatch(sg -> SG_PATTERN.matcher(sg).matches());
    }

    public boolean isValidIamRole(String iamRole) {
        // TODO We should make full ARN validation
        return !StringExt.safeTrim(iamRole).isEmpty();
    }

    public Map<String, String> validateImage(Image image) {
        // As class-level constraints are evaluated after field-level constraints we have to check for null value here.
        if(image == null) {
            return Collections.emptyMap();
        }

        Map<String, String> violations = new HashMap<>();
        if (!IMAGE_NAME_PATTERN.matcher(image.getName()).matches()) {
            violations.put("name", "image name is not valid");
        }

        boolean validDigest = !Strings.isNullOrEmpty(image.getDigest()) && IMAGE_DIGEST_PATTERN.matcher(image.getDigest()).matches();
        boolean validTag = !Strings.isNullOrEmpty(image.getTag()) && IMAGE_TAG_PATTERN.matcher(image.getTag()).matches();

        if (!validDigest && !validTag) {
            violations.put("", "must specify a valid digest or tag");
        }

        if (validDigest && validTag) {
            violations.put("", "must specify only a valid digest or tag and not both");
        }

        return violations;
    }

    public Map<String, String> notExceedsComputeResources(String capacityGroup, Container container) {
        // As class-level constraints are evaluated after field-level constraints we have to check for null value here.
        if(container == null) {
            return Collections.emptyMap();
        }
        ResourceDimension maxContainerSize = maxContainerSizeResolver.apply(capacityGroup);
        ContainerResources resources = container.getContainerResources();

        Map<String, String> violations = new HashMap<>();
        check(resources::getCpu, maxContainerSize::getCpu).ifPresent(v -> violations.put("container.containerResources.cpu", v));
        check(resources::getGpu, maxContainerSize::getGpu).ifPresent(v -> violations.put("container.containerResources.gpu", v));
        check(resources::getMemoryMB, maxContainerSize::getMemoryMB).ifPresent(v -> violations.put("container.containerResources.memoryMB", v));
        check(resources::getDiskMB, maxContainerSize::getDiskMB).ifPresent(v -> violations.put("container.containerResources.diskMB", v));
        check(resources::getNetworkMbps, maxContainerSize::getNetworkMbs).ifPresent(v -> violations.put("container.containerResources.networkMbps", v));

        return violations;
    }

    private <N extends Number> Optional<String> check(Supplier<N> jobResource, Supplier<N> maxAllowed) {
        if (jobResource.get().doubleValue() > maxAllowed.get().doubleValue()) {
            return Optional.of("Above maximum allowed value " + maxAllowed.get());
        }
        return Optional.empty();
    }
}