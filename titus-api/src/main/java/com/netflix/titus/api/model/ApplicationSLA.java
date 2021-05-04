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
import com.netflix.titus.common.util.StringExt;

/**
 * Application SLA definition.
 */
public class ApplicationSLA {

    public static final String DEFAULT_SCHEDULER_NAME = "fenzo";

    public static final String DEFAULT_CRITICAL_TIER_RESOURCE_POOL = "reserved";

    public static final String DEFAULT_FLEX_TIER_RESOURCE_POOL = "elastic";

    private final String appName;

    /**
     * If service tier is not defined it is defaulted to {@link Tier#Flex}.
     */
    private final Tier tier;

    private final ResourceDimension resourceDimension;

    private final String schedulerName;

    private final String resourcePool;

    /**
     * Total number of instances required by this application. Titus will keep pre-allocated resources to always
     * fulfill this requirement.
     */
    private int instanceCount;

    public ApplicationSLA(String appName,
                          Tier tier,
                          ResourceDimension resourceDimension,
                          int instanceCount,
                          String schedulerName,
                          String resourcePool) {
        this.appName = appName;
        this.tier = tier;
        this.resourceDimension = resourceDimension;
        this.instanceCount = instanceCount;
        if (Strings.isNullOrEmpty(schedulerName)) {
            this.schedulerName = DEFAULT_SCHEDULER_NAME;
        } else {
            this.schedulerName = schedulerName;
        }
        // Resource pools are only used with Kube Scheduler.
        // Unless a non-empty value is given, we populate a default value of resource pool for non-GPU capacity groups
        // when associated with kubeScheduler such that
        // critical tier capacity groups (ApplicationSLAs) are mapped to reserved resource pool
        // and flex tier capacity groups are mapped to elastic.
        if (StringExt.isNotEmpty(resourcePool)) {
            this.resourcePool = resourcePool;
        } else {
            if (Objects.equals(this.schedulerName, "kubeScheduler") && this.resourceDimension.getGpu() == 0L) {
                if (tier == Tier.Critical) {
                    this.resourcePool = DEFAULT_CRITICAL_TIER_RESOURCE_POOL;
                } else {
                    this.resourcePool = DEFAULT_FLEX_TIER_RESOURCE_POOL;
                }
            } else {
                this.resourcePool = "";
            }
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

    public String getResourcePool() {
        return resourcePool;
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
                Objects.equals(schedulerName, that.schedulerName) &&
                Objects.equals(resourcePool, that.resourcePool);
    }

    @Override
    public int hashCode() {
        return Objects.hash(appName, tier, resourceDimension, schedulerName, resourcePool, instanceCount);
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
                ", instanceCount=" + instanceCount + '\'' +
                ", resourcePool=" + resourcePool +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(ApplicationSLA original) {
        return newBuilder().withAppName(original.getAppName()).withTier(original.getTier())
                .withResourceDimension(original.getResourceDimension()).withInstanceCount(original.getInstanceCount())
                .withSchedulerName(original.getSchedulerName())
                .withResourcePool(original.getResourcePool());
    }

    public static final class Builder {
        private String appName;
        private Tier tier;
        private ResourceDimension resourceDimension;
        private int instanceCount;
        private String schedulerName;
        private String resourcePool;

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

        public Builder withResourcePool(String resourcePool) {
            this.resourcePool = resourcePool;
            return this;
        }

        public ApplicationSLA build() {
            return new ApplicationSLA(appName, tier, resourceDimension, instanceCount, schedulerName, resourcePool);
        }
    }
}
