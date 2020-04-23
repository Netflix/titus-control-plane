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

package com.netflix.titus.api.model;

import java.util.Objects;

import com.google.common.base.Strings;

/**
 * Application SLA definition.
 */
public class ApplicationSLA {

    public static final String DEFAULT_SCHEDULER_NAME = "fenzo";

    private final String appName;

    /**
     * If service tier is not defined it is defaulted to {@link Tier#Flex}.
     */
    private final Tier tier;

    private final ResourceDimension resourceDimension;

    private final String schedulerName;

    /**
     * Total number of instances required by this application. Titus will keep pre-allocated resources to always
     * fulfill this requirement.
     */
    private int instanceCount;

    public ApplicationSLA(String appName,
                          Tier tier,
                          ResourceDimension resourceDimension,
                          int instanceCount,
                          String schedulerName) {
        this.appName = appName;
        this.tier = tier;
        this.resourceDimension = resourceDimension;
        this.instanceCount = instanceCount;
        if (Strings.isNullOrEmpty(schedulerName)) {
            this.schedulerName = DEFAULT_SCHEDULER_NAME;
        } else {
            this.schedulerName = schedulerName;
        }
    }

    public String getAppName() {
        return appName;
    }

    public Tier getTier() {
        return tier;
    }

    public ResourceDimension getResourceDimension() {
        return resourceDimension;
    }

    public int getInstanceCount() {
        return instanceCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ApplicationSLA that = (ApplicationSLA) o;

        return instanceCount == that.instanceCount &&
                Objects.equals(appName, that.appName) &&
                tier == that.tier &&
                Objects.equals(resourceDimension, that.resourceDimension) &&
                Objects.equals(schedulerName, that.schedulerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(appName, tier, resourceDimension, schedulerName, instanceCount);
    }

    public String getSchedulerName() {
        return schedulerName;
    }

    @Override
    public String toString() {
        return "ApplicationSLA{" +
                "appName='" + appName + '\'' +
                ", tier=" + tier +
                ", resourceDimension=" + resourceDimension +
                ", schedulerName='" + schedulerName + '\'' +
                ", instanceCount=" + instanceCount +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(ApplicationSLA original) {
        return newBuilder().withAppName(original.getAppName()).withTier(original.getTier())
                .withResourceDimension(original.getResourceDimension()).withInstanceCount(original.getInstanceCount())
                .withSchedulerName(original.getSchedulerName());
    }

    public static final class Builder {
        private String appName;
        private Tier tier;
        private ResourceDimension resourceDimension;
        private int instanceCount;
        private String schedulerName;

        private Builder() {
        }

        public Builder withAppName(String appName) {
            this.appName = appName;
            return this;
        }

        public Builder withTier(Tier tier) {
            this.tier = tier;
            return this;
        }

        public Builder withResourceDimension(ResourceDimension resourceDimension) {
            this.resourceDimension = resourceDimension;
            return this;
        }

        public Builder withInstanceCount(int instanceCount) {
            this.instanceCount = instanceCount;
            return this;
        }

        public Builder but() {
            return newBuilder().withAppName(appName).withTier(tier)
                    .withResourceDimension(resourceDimension).withInstanceCount(instanceCount);
        }

        public Builder withSchedulerName(String schedulerName) {
            this.schedulerName = schedulerName;
            return this;
        }

        public ApplicationSLA build() {
            return new ApplicationSLA(appName, tier, resourceDimension, instanceCount, schedulerName);
        }
    }
}
