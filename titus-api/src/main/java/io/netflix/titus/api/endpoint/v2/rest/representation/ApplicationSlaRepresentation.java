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

package io.netflix.titus.api.endpoint.v2.rest.representation;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Application SLA model for v2 REST API. Each job type (service, batch) can be scheduled in any tier.
 * <p>
 * <h1>Capacity guarantee</h1>
 * An application has to define resources required by their tasks. It is expected (but not verified yet), that
 * all application's jobs will include the resource allocations identical to those requested in SLA.
 * Together with resource dimensions, the application SLA defines a total number of instances it expects to
 * require for peek allocation. This is not a constraint, and the application can ask for more resources, but
 * these may not be readily available, and may require scale up action. If scale-up is not possible, as maximum
 * size of the agent server group is reached, application's excessive tasks will be queued.
 * <p>
 * <h1>Fitness</h1>
 * Agent cluster allocation is based on job fitness criteria (how well its resources match a given instance type).
 * As service job is expected always to run on an instance available within its tier, it may be a mismatch between fitness
 * calculated best-agent, and the SLA-level restriction. To resolve this, a global constraint is provided that
 * restricts the application to run on instance types from within its tier.
 * <p>
 * <h1>Agent cluster assignment</h1>
 * It is expected that only one agent server group per instance type exists. This can be not true however under
 * certain circumstances, like Titus agent redeployment. If there are multiple clusters deployed with the same
 * instance type, the sum of their resources is checked against expected SLA resource level. If not sufficient
 * resources are present, any of those clusters (one or many) may be chosen for scale-up action
 * (setting new min-ASG level).
 */
public class ApplicationSlaRepresentation {

    @NotNull(message = "'appName' is a required property")
    private final String appName;

    /**
     * If service tier is not defined it is defaulted to {@link TierRepresentation#Flex}.
     */
    private final TierRepresentation tier;

    @NotNull(message = "'instanceCPU' is a required property")
    @Min(value = 1, message = "'instanceCPU' must be at least 1")
    private final Double instanceCPU;

    @NotNull(message = "'instanceMemoryMB' is a required property")
    @Min(value = 1, message = "'instanceMemoryMB' must be at least 1MB")
    private final Integer instanceMemoryMB;

    @NotNull(message = "'instanceDiskMB' is a required property")
    @Min(value = 1, message = "'instanceDiskMB' must be at least 1MB")
    private final Integer instanceDiskMB;

    @NotNull(message = "'instanceNetworkMbs' is a required property")
    @Min(value = 1, message = "'instanceNetworkMbs' must be at least 1Mbs")
    private final Integer instanceNetworkMbs;

    /**
     * Total number of instances required by this application. Titus will keep pre-allocated resources to always
     * fulfill this requirement.
     */
    @NotNull(message = "'instanceCount' is a required property")
    @Min(value = 1, message = "'instanceCount' must be at least 1")
    private Integer instanceCount;

    @JsonCreator
    public ApplicationSlaRepresentation(@JsonProperty("appName") String appName,
                                        @JsonProperty("tier") TierRepresentation tier,
                                        @JsonProperty("instanceCPU") Double instanceCPU,
                                        @JsonProperty("instanceMemoryMB") Integer instanceMemoryMB,
                                        @JsonProperty("instanceDiskMB") Integer instanceDiskMB,
                                        @JsonProperty("instanceNetworkMbs") Integer instanceNetworkMbs,
                                        @JsonProperty("instanceCount") Integer instanceCount) {
        this.appName = appName;
        this.tier = tier;
        this.instanceCPU = instanceCPU;
        this.instanceMemoryMB = instanceMemoryMB;
        this.instanceDiskMB = instanceDiskMB;
        this.instanceNetworkMbs = instanceNetworkMbs;
        this.instanceCount = instanceCount;
    }

    public String getAppName() {
        return appName;
    }

    public TierRepresentation getTier() {
        return tier;
    }

    public Double getInstanceCPU() {
        return instanceCPU;
    }

    public Integer getInstanceMemoryMB() {
        return instanceMemoryMB;
    }

    public Integer getInstanceDiskMB() {
        return instanceDiskMB;
    }

    public Integer getInstanceNetworkMbs() {
        return instanceNetworkMbs;
    }

    public Integer getInstanceCount() {
        return instanceCount;
    }
}
