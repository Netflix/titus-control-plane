/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.jobmanager.model.job.sanitizer;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddressAllocation;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;

import static com.netflix.titus.common.util.StringExt.isAsciiDigit;
import static com.netflix.titus.common.util.StringExt.isAsciiLetter;

/**
 *
 */
public class JobAssertions {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    public static final int MAX_ENTRY_POINT_SIZE_SIZE_KB = 16;
    public static final int MAX_ENTRY_POINT_SIZE_SIZE_BYTES = MAX_ENTRY_POINT_SIZE_SIZE_KB * 1024;

    private static final Pattern SG_PATTERN = Pattern.compile("sg-.*");
    private static final Pattern IMAGE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9\\.\\\\/_-]+");
    private static final Pattern IMAGE_TAG_PATTERN = Pattern.compile("[a-zA-Z0-9\\._-]+");

    // Based on https://github.com/docker/distribution/blob/master/reference/reference.go
    private static final String DIGEST_ALGORITHM_SEPARATOR = "[+.-_]";
    private static final String DIGEST_ALGORITHM_COMPONENT = "[A-Za-z][A-Za-z0-9]*";
    private static final String DIGEST_ALGORITHM = String.format("%s[%s%s]*", DIGEST_ALGORITHM_COMPONENT, DIGEST_ALGORITHM_SEPARATOR, DIGEST_ALGORITHM_COMPONENT);
    private static final String DIGEST_HEX = "[0-9a-fA-F]{32,}";
    private static final String DIGEST = String.format("%s:%s", DIGEST_ALGORITHM, DIGEST_HEX);

    private static final Pattern IMAGE_DIGEST_PATTERN = Pattern.compile(DIGEST);

    private final JobConfiguration configuration;
    private final Function<String, ResourceDimension> maxContainerSizeResolver;

    public JobAssertions(JobConfiguration configuration,
                         Function<String, ResourceDimension> maxContainerSizeResolver) {
        this.configuration = configuration;
        this.maxContainerSizeResolver = maxContainerSizeResolver;
    }

    public boolean isValidSyntax(List<String> securityGroups) {
        return securityGroups.stream().allMatch(sg -> SG_PATTERN.matcher(sg).matches());
    }

    public boolean isValidIamRole(String iamRole) {
        // TODO We should make full ARN validation
        String trimmed = StringExt.safeTrim(iamRole);
        return !trimmed.isEmpty() && !trimmed.contains(" ");
    }

    public boolean isEntryPointNotTooLarge(List<String> entryPoint) {
        if (!configuration.isEntryPointSizeLimitEnabled()) {
            return true;
        }
        if (CollectionsExt.isNullOrEmpty(entryPoint)) {
            return true;
        }

        int totalSize = entryPoint.stream().mapToInt(e -> StringExt.isEmpty(e) ? 0 : e.getBytes(UTF_8).length).sum();

        return totalSize <= MAX_ENTRY_POINT_SIZE_SIZE_BYTES;
    }

    public Map<String, String> validateEnvironmentVariableNames(Map<String, String> environment) {
        if (CollectionsExt.isNullOrEmpty(environment)) {
            return Collections.emptyMap();
        }

        Map<String, String> violations = new HashMap<>();
        environment.forEach((key, value) -> {
            if (key.isEmpty()) {
                violations.put("empty", "the environment name cannot be an empty string");
                return;
            }

            char first = key.charAt(0);
            if (!isAsciiUpperCase(first) && first != '_') {
                violations.put("invalidFirstCharacter", "the environment name must start with an upper case ASCII letter or '_'");
            }

            if (key.length() == 1) {
                return;
            }

            for (int i = 1; i < key.length(); i++) {
                char c = key.charAt(i);
                if (!isAsciiUpperCase(c) && !isAsciiDigit(c) && c != '_') {
                    violations.put("invalidCharacter", "the environment name characters may be an upper case ASCII letter, a digit or '_'");
                    break;
                }
            }
        });

        return violations;
    }

    private boolean isAsciiUpperCase(char c) {
        return isAsciiLetter(c) && Character.isUpperCase(c);
    }

    public boolean areEnvironmentVariablesNotTooLarge(Map<String, String> environment) {
        if (CollectionsExt.isNullOrEmpty(environment)) {
            return true;
        }

        int totalSize = environment.entrySet().stream().mapToInt(entry -> {
            int keySize = StringExt.isEmpty(entry.getKey()) ? 0 : entry.getKey().getBytes(UTF_8).length;
            int valueSize = StringExt.isEmpty(entry.getValue()) ? 0 : entry.getValue().getBytes(UTF_8).length;

            // The 2 additional bytes are for the equal sign and the NUL terminator.
            return keySize + valueSize + 2;
        }).sum();

        return totalSize <= configuration.getMaxTotalEnvironmentVariableSizeKB() * 1024;
    }

    public boolean isValidContainerHealthServiceName(String name) {
        String[] validNames = configuration.getContainerHealthProviders().split(",");
        if (CollectionsExt.isNullOrEmpty(validNames)) {
            return false;
        }
        for (String validName : validNames) {
            if (validName.equals(name)) {
                return true;
            }
        }
        return false;
    }

    public Map<String, String> validateImage(Image image) {
        // As class-level constraints are evaluated after field-level constraints we have to check for null value here.
        if (image == null) {
            return Collections.emptyMap();
        }

        Map<String, String> violations = new HashMap<>();
        if (!IMAGE_NAME_PATTERN.matcher(image.getName()).matches()) {
            violations.put("name", "image name is not valid");
        }

        boolean validDigest = !Strings.isNullOrEmpty(image.getDigest()) && IMAGE_DIGEST_PATTERN.matcher(image.getDigest()).matches();
        boolean validTag = !Strings.isNullOrEmpty(image.getTag()) && IMAGE_TAG_PATTERN.matcher(image.getTag()).matches();

        if (!validDigest && !validTag) {
            violations.put("noValidImageDigestOrTag", "must specify a valid digest or tag");
        }

        return violations;
    }

    public Map<String, String> notExceedsComputeResources(String capacityGroup, Container container) {
        // As class-level constraints are evaluated after field-level constraints we have to check for null value here.
        if (container == null) {
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

    public Map<String, String> notExceedsIpAllocations(Container container, JobDescriptor.JobDescriptorExt extension) {
        // As class-level constraints are evaluated after field-level constraints we have to check for null value here.
        if (container == null) {
            return Collections.emptyMap();
        }

        int numIpAllocations = container.getContainerResources().getSignedIpAddressAllocations().size();
        int numInstances = extension instanceof ServiceJobExt
                ? ((ServiceJobExt) extension).getCapacity().getMax()
                : ((BatchJobExt) extension).getSize();
        if (numIpAllocations > 0 &&
                numInstances > numIpAllocations) {
            return Collections.singletonMap("container.containerResources.signedIpAllocations", "Above number of max task instances " + numInstances);
        }
        return Collections.emptyMap();
    }

    public Map<String, String> notExceedsEbsVolumes(Container container, JobDescriptor.JobDescriptorExt extension) {
        // As class-level constraints are evaluated after field-level constraints we have to check for null value here.
        if (container == null) {
            return Collections.emptyMap();
        }

        int numEbsVolumes = container.getContainerResources().getEbsVolumes().size();
        int numInstances = extension instanceof ServiceJobExt
                ? ((ServiceJobExt) extension).getCapacity().getMax()
                : ((BatchJobExt) extension).getSize();
        if (numEbsVolumes > 0 && numInstances > numEbsVolumes) {
            return Collections.singletonMap("container.containerResources.ebsVolumes", "Above number of max task instances " + numInstances);
        }

        return Collections.emptyMap();
    }

    public Map<String, String> matchingEbsAndIpZones(List<EbsVolume> ebsVolumes, List<SignedIpAddressAllocation> ipSignedAddressAllocations) {
        return validateMatchingEbsAndIpZones(ebsVolumes, ipSignedAddressAllocations).stream()
                .collect(Collectors.toMap(ValidationError::getField, ValidationError::getDescription));
    }

    public static Set<ValidationError> validateMatchingEbsAndIpZones(List<EbsVolume> ebsVolumes, List<SignedIpAddressAllocation> ipSignedAddressAllocations) {
        if (ebsVolumes == null || ipSignedAddressAllocations == null) {
            return Collections.emptySet();
        }

        if (ebsVolumes.isEmpty() || ipSignedAddressAllocations.isEmpty()) {
            return Collections.emptySet();
        }

        int numElements = Math.min(ebsVolumes.size(), ipSignedAddressAllocations.size());
        for (int i = 0; i < numElements; i++) {
            EbsVolume ebsVolume = ebsVolumes.get(i);
            IpAddressAllocation ipAddressAllocation = ipSignedAddressAllocations.get(i).getIpAddressAllocation();
            if (!ebsVolume.getVolumeAvailabilityZone().equals(ipAddressAllocation.getIpAddressLocation().getAvailabilityZone())) {
                return Collections.singleton(new ValidationError(
                        "containerResources.ebsVolumes",
                        String.format(
                                "EBS volume %s zone %s conflicts with Static IP %s zone %s and index %d",
                                ebsVolume.getVolumeId(), ebsVolume.getVolumeAvailabilityZone(), ipAddressAllocation.getAllocationId(), ipAddressAllocation.getIpAddressLocation().getAvailabilityZone(), i
                        )
                ));
            }
        }

        return Collections.emptySet();
    }

    private <N extends Number> Optional<String> check(Supplier<N> jobResource, Supplier<N> maxAllowed) {
        if (jobResource.get().doubleValue() > maxAllowed.get().doubleValue()) {
            return Optional.of("Above maximum allowed value " + maxAllowed.get());
        }
        return Optional.empty();
    }
}