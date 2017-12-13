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

import java.util.Optional;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.common.model.sanitizer.EntitySanitizerBuilder;

/**
 */
public class JobSanitizerBuilder {

    public static final String DEFAULT_CAPACITY_GROUP = "DEFAULT";

    private static final String MODEL_ROOT_PACKAGE = Job.class.getPackage().getName();

    private final EntitySanitizerBuilder sanitizerBuilder = EntitySanitizerBuilder.stdBuilder();

    private JobConfiguration jobConfiguration;
    private Function<String, ResourceDimension> maxContainerSizeResolver;

    public JobSanitizerBuilder withJobConstrainstConfiguration(JobConfiguration jobConfiguration) {
        this.jobConfiguration = jobConfiguration;
        return this;
    }

    public JobSanitizerBuilder withMaxContainerSizeResolver(Function<String, ResourceDimension> maxContainerSizeResolver) {
        this.maxContainerSizeResolver = maxContainerSizeResolver;
        return this;
    }

    public EntitySanitizer build() {
        Preconditions.checkNotNull(jobConfiguration, "JobConfiguration not set");
        Preconditions.checkNotNull(maxContainerSizeResolver, "Max container size resolver not set");

        sanitizerBuilder
                .processEntities(type -> type.getPackage().getName().startsWith(MODEL_ROOT_PACKAGE))
                .addTemplateResolver(path -> {
                    if (path.endsWith("capacityGroup")) {
                        return Optional.of(DEFAULT_CAPACITY_GROUP);
                    }
                    return Optional.empty();
                })
                .addValidatorFactory(type -> {
                    if (type.equals(SchedulingConstraintValidator.SchedulingConstraint.class)) {
                        return Optional.of(new SchedulingConstraintValidator());
                    }
                    if (type.equals(SchedulingConstraintSetValidator.SchedulingConstraintSet.class)) {
                        return Optional.of(new SchedulingConstraintSetValidator());
                    }
                    return Optional.empty();
                })
                .registerBean("constraints", jobConfiguration)
                .registerBean("asserts", new JobAssertions(maxContainerSizeResolver));

        return sanitizerBuilder.build();
    }
}
