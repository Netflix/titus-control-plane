package com.netflix.titus.gateway.service.v3.internal;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Named;
import javax.validation.ConstraintViolation;

import com.google.common.base.CharMatcher;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.gateway.service.v3.JobManagerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;

/**
 * Extends the default job model sanitizer with extra checks.
 */
class ExtendedJobSanitizer implements EntitySanitizer {

    private static final Logger logger = LoggerFactory.getLogger(ExtendedJobSanitizer.class);

    private static final String TITUS_NON_COMPLIANT = "titus.noncompliant";
    private static final Predicate<String> CONTAINS_SPACES = Pattern.compile(".*\\s+.*").asPredicate();

    private final JobManagerConfiguration jobManagerConfiguration;
    private final EntitySanitizer entitySanitizer;
    private final Function<String, Matcher> uncompliantClientMatcher;

    public ExtendedJobSanitizer(JobManagerConfiguration jobManagerConfiguration,
                                @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer) {
        this.jobManagerConfiguration = jobManagerConfiguration;
        this.entitySanitizer = entitySanitizer;
        this.uncompliantClientMatcher = RegExpExt.dynamicMatcher(
                jobManagerConfiguration::getNoncompliantClientWhiteList, "noncompliantClientWhiteList", 0, logger
        );
    }

    @Override
    public <T> Set<ConstraintViolation<T>> validate(T entity) {
        return entitySanitizer.validate(entity);
    }

    @Override
    public <T> Optional<T> sanitize(T entity) {
        T sanitized = entitySanitizer.sanitize(entity).orElse(entity);
        if (sanitized instanceof com.netflix.titus.api.jobmanager.model.job.JobDescriptor) {
            com.netflix.titus.api.jobmanager.model.job.JobDescriptor jobDescriptor = (com.netflix.titus.api.jobmanager.model.job.JobDescriptor) sanitized;

            // TODO Remove this code section once all clients are compliant and they set explicitly security group(s) and IAM role.
            if (isInNonCompliantWhiteList(jobDescriptor)) {
                jobDescriptor = addMissingSecurityGroupAndIamRole(jobDescriptor);
            }

            // TODO Remove once all clients are compliant.
            jobDescriptor = checkEntryPointViolations(jobDescriptor);
            jobDescriptor = checkResourceViolations(jobDescriptor);
            sanitized = (T) checkEnvironmentViolations(jobDescriptor);
        }
        return entity == sanitized ? Optional.empty() : Optional.of(sanitized);
    }

    private boolean isInNonCompliantWhiteList(com.netflix.titus.api.jobmanager.model.job.JobDescriptor jobDescriptor) {
        com.netflix.titus.api.jobmanager.model.job.JobGroupInfo jobGroupInfo = jobDescriptor.getJobGroupInfo();
        String jobClusterId = jobDescriptor.getApplicationName() + '-' + jobGroupInfo.getStack() + '-' + jobGroupInfo.getDetail() + '-' + jobGroupInfo.getSequence();
        return uncompliantClientMatcher.apply(jobClusterId).matches();
    }

    private com.netflix.titus.api.jobmanager.model.job.JobDescriptor addMissingSecurityGroupAndIamRole(com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> jobDescriptor) {
        SecurityProfile securityProfile = jobDescriptor.getContainer().getSecurityProfile();
        if (!securityProfile.getSecurityGroups().isEmpty() && !securityProfile.getIamRole().isEmpty()) {
            return jobDescriptor;
        }
        SecurityProfile.Builder builder = securityProfile.toBuilder();
        String nonCompliant = null;
        if (securityProfile.getSecurityGroups().isEmpty()) {
            builder.withSecurityGroups(jobManagerConfiguration.getDefaultSecurityGroups());
            nonCompliant = "noSecurityGroups";
        }
        if (securityProfile.getIamRole().isEmpty()) {
            builder.withIamRole(jobManagerConfiguration.getDefaultIamRole());
            nonCompliant = nonCompliant == null ? "noIamRole" : nonCompliant + ",noIamRole";
        }
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> sanitizedJobDescriptor = jobDescriptor.toBuilder()
                .withContainer(jobDescriptor.getContainer().toBuilder()
                        .withSecurityProfile(builder.build()).build()
                ).build();
        return markNonCompliant(sanitizedJobDescriptor, nonCompliant);
    }

    /**
     * Jobs with entry point binaries containing spaces are likely relying on the legacy shell parsing being done by
     * titus-executor, and are submitting entry points as a flat string, instead of breaking it onto a list of
     * arguments.
     * <p>
     * Jobs that have a <tt>command</tt> set will fall on the "new" code path that does not do any shell parsing, and do
     * not need to be checked.
     */
    private JobDescriptor checkEntryPointViolations(JobDescriptor jobDescriptor) {
        List<String> entryPoint = jobDescriptor.getContainer().getEntryPoint();
        List<String> command = jobDescriptor.getContainer().getCommand();
        if (!CollectionsExt.isNullOrEmpty(entryPoint) && CollectionsExt.isNullOrEmpty(command) &&
                CONTAINS_SPACES.test(entryPoint.get(0))) {
            return markNonCompliant(jobDescriptor, "entryPointBinaryWithSpaces");
        }
        return jobDescriptor;
    }

    private com.netflix.titus.api.jobmanager.model.job.JobDescriptor checkEnvironmentViolations(com.netflix.titus.api.jobmanager.model.job.JobDescriptor jobDescriptor) {
        Map<String, String> env = jobDescriptor.getContainer().getEnv();
        if (CollectionsExt.isNullOrEmpty(env)) {
            return jobDescriptor;
        }

        boolean allAsciiCharacters = env.entrySet().stream().allMatch(entry -> isAscii(entry.getKey()) && isAscii(entry.getValue()));
        boolean noDotInKeyName = env.keySet().stream().allMatch(key -> key == null || !key.contains("."));

        if (allAsciiCharacters && noDotInKeyName) {
            return jobDescriptor;
        }

        String nonCompliant = allAsciiCharacters
                ? "environmentVariableNameWithDot"
                : (noDotInKeyName ? "nonAsciiCharactersInEnvironmentVariable" : "environmentVariableNameWithDot,nonAsciiCharactersInEnvironmentVariable");

        return markNonCompliant(jobDescriptor, nonCompliant);
    }

    private com.netflix.titus.api.jobmanager.model.job.JobDescriptor checkResourceViolations(com.netflix.titus.api.jobmanager.model.job.JobDescriptor jobDescriptor) {
        ContainerResources containerResources = jobDescriptor.getContainer().getContainerResources();
        int minDiskSize = jobManagerConfiguration.getMinDiskSizeMB();
        if (containerResources.getDiskMB() >= minDiskSize) {
            return jobDescriptor;
        }

        com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> sanitizedJobDescriptor = jobDescriptor.toBuilder()
                .withContainer(jobDescriptor.getContainer().toBuilder()
                        .withContainerResources(
                                containerResources.toBuilder().withDiskMB(minDiskSize).build()
                        ).build()
                ).build();
        return markNonCompliant(sanitizedJobDescriptor, "diskSizeLessThanMin");
    }

    private com.netflix.titus.api.jobmanager.model.job.JobDescriptor markNonCompliant(com.netflix.titus.api.jobmanager.model.job.JobDescriptor<?> jobDescriptor, String nonCompliant) {
        Map<String, String> attributes = jobDescriptor.getAttributes();
        String previousNonCompliant = attributes.get(TITUS_NON_COMPLIANT);
        String newNonCompliant = previousNonCompliant == null ? nonCompliant : previousNonCompliant + ',' + nonCompliant;
        return jobDescriptor.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(jobDescriptor.getAttributes(), TITUS_NON_COMPLIANT, newNonCompliant))
                .build();
    }

    private boolean isAscii(String value) {
        return value == null || CharMatcher.ascii().matchesAllOf(value);
    }
}
