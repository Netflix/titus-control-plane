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

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import io.netflix.titus.api.model.MigrationPolicy;
import io.netflix.titus.api.model.v2.JobConstraints;

public class TitusJobInfo {

    private final String id;
    private final String name;
    private final TitusJobType type;
    private final Map<String, String> labels;
    private final String applicationName;
    private final String appName;
    private final String user;
    private final String version;
    private final String entryPoint;
    private final boolean inService;
    private final TitusJobState state;
    private final int instances;
    private final int instancesMin;
    private final int instancesMax;
    private final int instancesDesired;
    private final double cpu;
    private final double memory;
    private final double networkMbps;
    private final double disk;
    private final int[] ports;
    private final int gpu;
    private final String jobGroupStack;
    private final String jobGroupDetail;
    private final String jobGroupSequence;
    private final String capacityGroup;
    private final MigrationPolicy migrationPolicy;
    private final Map<String, String> environment;
    private final Map<String, Object> titusContext;
    private final int retries;
    private long runtimeLimitSecs;
    private final boolean allocateIpAddress;
    private final String iamProfile;
    private final List<String> securityGroups;
    private final EfsMountRepresentation efs;
    private final List<EfsMountRepresentation> efsMounts;
    private final String submittedAt;
    private final List<JobConstraints> softConstraints;
    private final List<JobConstraints> hardConstraints;
    private final List<TaskInfo> tasks;
    private final List<AuditLog> auditLogs;
    private ServiceJobProcesses jobProcesses;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public TitusJobInfo(@JsonProperty("id") String id,
                        @JsonProperty("name") String name,
                        @JsonProperty("type") TitusJobType type,
                        @JsonProperty("labels") Map<String, String> labels,
                        @JsonProperty("applicationName") String applicationName,
                        @JsonProperty("appName") String appName,
                        @JsonProperty("user") String user,
                        @JsonProperty("version") String version,
                        @JsonProperty("entryPoint") String entryPoint,
                        @JsonProperty("inService") Boolean inService,
                        @JsonProperty("state") TitusJobState state,
                        @JsonProperty("instances") int instances,
                        @JsonProperty("instancesMin") int instancesMin,
                        @JsonProperty("instancesMax") int instancesMax,
                        @JsonProperty("instancesDesired") int instancesDesired,
                        @JsonProperty("cpu") double cpu,
                        @JsonProperty("memory") double memory,
                        @JsonProperty("networkMbps") double networkMbps,
                        @JsonProperty("disk") double disk,
                        @JsonProperty("ports") int[] ports,
                        @JsonProperty("gpu") int gpu,
                        @JsonProperty("jobGroupStack") String jobGroupStack,
                        @JsonProperty("jobGroupDetail") String jobGroupDetail,
                        @JsonProperty("jobGroupSequence") String jobGroupSequence,
                        @JsonProperty("capacityGroup") String capacityGroup,
                        @JsonProperty("migrationPolicy") MigrationPolicy migrationPolicy,
                        @JsonProperty("environment") Map<String, String> environment,
                        @JsonProperty("titusContext") Map<String, Object> titusContext,
                        @JsonProperty("retries") int retries,
                        @JsonProperty("runtimeLimitSecs") Long runtimeLimitSecs,
                        @JsonProperty("allocateIpAddress") boolean allocateIpAddress,
                        @JsonProperty("iamProfile") String iamProfile,
                        @JsonProperty("securityGroups") List<String> securityGroups,
                        @JsonProperty("efs") EfsMountRepresentation efs,
                        @JsonProperty("efsMounts") List<EfsMountRepresentation> efsMounts,
                        @JsonProperty("submittedAt") String submittedAt,
                        @JsonProperty("softConstraints") List<JobConstraints> softConstraints,
                        @JsonProperty("hardConstraints") List<JobConstraints> hardConstraints,
                        @JsonProperty("tasks") List<TaskInfo> tasks,
                        @JsonProperty("auditLogs") List<AuditLog> auditLogs,
                        @JsonProperty("jobProcesses") ServiceJobProcesses jobProcesses
    ) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.labels = labels;
        this.applicationName = applicationName;
        this.appName = appName;
        this.user = user;
        this.version = version;
        this.entryPoint = entryPoint;
        this.inService = inService == null ? true : inService;
        this.state = state;
        this.instances = instances;
        this.instancesMin = instancesMin;
        this.instancesMax = instancesMax;
        this.instancesDesired = instancesDesired;
        this.cpu = cpu;
        this.memory = memory;
        this.networkMbps = networkMbps;
        this.disk = disk;
        this.ports = ports;
        this.gpu = gpu;
        this.jobGroupStack = jobGroupStack;
        this.jobGroupDetail = jobGroupDetail;
        this.jobGroupSequence = jobGroupSequence;
        this.capacityGroup = capacityGroup;
        this.migrationPolicy = migrationPolicy;
        this.environment = environment;
        this.titusContext = titusContext;
        this.retries = retries;
        this.runtimeLimitSecs = runtimeLimitSecs != null ? runtimeLimitSecs : 0L;
        this.allocateIpAddress = allocateIpAddress;
        this.iamProfile = iamProfile;
        this.securityGroups = securityGroups;
        this.efs = efs;
        this.efsMounts = efsMounts;
        this.submittedAt = submittedAt;
        this.softConstraints = softConstraints;
        this.hardConstraints = hardConstraints;
        this.tasks = tasks;
        this.auditLogs = auditLogs;
        this.jobProcesses = jobProcesses;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public TitusJobType getType() {
        return type;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getAppName() {
        return appName;
    }

    public String getUser() {
        return user;
    }

    public String getVersion() {
        return version;
    }

    public String getEntryPoint() {
        return entryPoint;
    }

    public boolean isInService() {
        return inService;
    }

    public TitusJobState getState() {
        return state;
    }

    public int getInstances() {
        return instances;
    }

    public int getInstancesMin() {
        return instancesMin;
    }

    public int getInstancesMax() {
        return instancesMax;
    }

    public int getInstancesDesired() {
        return instancesDesired;
    }

    public double getCpu() {
        return cpu;
    }

    public double getMemory() {
        return memory;
    }

    public double getNetworkMbps() {
        return networkMbps;
    }

    public double getDisk() {
        return disk;
    }

    public int[] getPorts() {
        return ports;
    }

    public int getGpu() {
        return gpu;
    }

    public String getJobGroupStack() {
        return jobGroupStack;
    }

    public String getJobGroupDetail() {
        return jobGroupDetail;
    }

    public String getJobGroupSequence() {
        return jobGroupSequence;
    }

    public String getCapacityGroup() {
        return capacityGroup;
    }

    public MigrationPolicy getMigrationPolicy() {
        return migrationPolicy;
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }

    public Map<String, Object> getTitusContext() {
        return titusContext;
    }

    public int getRetries() {
        return retries;
    }

    public long getRuntimeLimitSecs() {
        return runtimeLimitSecs;
    }

    public boolean getAllocateIpAddress() {
        return allocateIpAddress;
    }

    public String getIamProfile() {
        return iamProfile;
    }

    public List<String> getSecurityGroups() {
        return securityGroups;
    }

    public EfsMountRepresentation getEfs() {
        return efs;
    }

    public List<EfsMountRepresentation> getEfsMounts() {
        return efsMounts;
    }

    public String getSubmittedAt() {
        return submittedAt;
    }

    public List<JobConstraints> getSoftConstraints() {
        return softConstraints;
    }

    public List<JobConstraints> getHardConstraints() {
        return hardConstraints;
    }

    public List<TaskInfo> getTasks() {
        return tasks;
    }

    public List<AuditLog> getAuditLogs() {
        return auditLogs;
    }

    public ServiceJobProcesses getJobProcesses() {
        return jobProcesses;
    }
}
