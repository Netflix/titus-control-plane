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

package com.netflix.titus.api.model.v2;

import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import com.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import com.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import com.netflix.titus.api.model.v2.parameter.Parameter;

public class V2JobDefinition {

    private static final long serialVersionUID = 1L;

    private String name;
    private String user;
    private URL jobJarFileLocation;
    private String version;
    private List<Parameter> parameters;
    private JobSla jobSla;
    private long subscriptionTimeoutSecs = 0L;
    private SchedulingInfo schedulingInfo;
    private int slaMin = 0;
    private int slaMax = 0;
    private String cronSpec = "";
    private NamedJobDefinition.CronPolicy cronPolicy = null;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public V2JobDefinition(@JsonProperty("name") String name,
                           @JsonProperty("user") String user,
                           @JsonProperty("url") URL jobJarFileLocation,
                           @JsonProperty("version") String version,
                           @JsonProperty("parameters") List<Parameter> parameters,
                           @JsonProperty("jobSla") JobSla jobSla,
                           @JsonProperty("subscriptionTimeoutSecs") long subscriptionTimeoutSecs,
                           @JsonProperty("schedulingInfo") SchedulingInfo schedulingInfo,
                           @JsonProperty("slaMin") int slaMin, @JsonProperty("slaMax") int slaMax,
                           @JsonProperty("cronSpec") String cronSpec,
                           @JsonProperty("cronPolicy") NamedJobDefinition.CronPolicy cronPolicy
    ) {
        this.name = name;
        this.user = user;
        this.jobJarFileLocation = jobJarFileLocation;
        this.version = version;
        if (parameters != null) {
            this.parameters = parameters;
        } else {
            this.parameters = new LinkedList<>();
        }
        this.jobSla = jobSla;
        if (subscriptionTimeoutSecs > 0) {
            this.subscriptionTimeoutSecs = subscriptionTimeoutSecs;
        }
        this.schedulingInfo = schedulingInfo;
        this.slaMin = slaMin;
        this.slaMax = slaMax;
        this.cronSpec = cronSpec;
        this.cronPolicy = cronPolicy;
    }

    public void validate(boolean schedulingInfoOptional) throws InvalidJobException {
        validateSla();
        validateSchedulingInfo(schedulingInfoOptional);
    }

    private void validateSla() throws InvalidJobException {
        if (jobSla == null) {
            throw new InvalidJobException("No Job SLA provided (likely incorrect job submit request)");
        }
        if (jobSla.getDurationType() == null) {
            throw new InvalidJobException("Invalid null duration type in job sla (likely incorrect job submit request");
        }
    }

    public void validateSchedulingInfo() throws InvalidJobException {
        validateSchedulingInfo(false);
    }

    public void validateSchedulingInfo(boolean schedulingInfoOptional) throws InvalidJobException {
        if (schedulingInfoOptional && schedulingInfo == null) {
            return;
        }
        if (schedulingInfo == null) {
            throw new InvalidJobException("No scheduling info provided");
        }
        if (schedulingInfo.getStages() == null) {
            throw new InvalidJobException("No stages defined in scheduling info");
        }
        int numStages = schedulingInfo.getStages().size();
        for (int i = 1; i <= numStages; i++) {
            StageSchedulingInfo stage = schedulingInfo.getStages().get(i);
            if (stage == null) {
                throw new InvalidJobException("No definition for stage " + i + " in scheduling info for " + numStages + " stage job");
            }
            if (stage.getNumberOfInstances() < 1) {
                throw new InvalidJobException("Number of instance for stage " + i + " must be >0, not " + stage.getNumberOfInstances());
            }
            MachineDefinition machineDefinition = stage.getMachineDefinition();
            if (machineDefinition.getCpuCores() <= 0) {
                throw new InvalidJobException("cpuCores must be >0.0, not " + machineDefinition.getCpuCores());
            }
            if (machineDefinition.getMemoryMB() <= 0) {
                throw new InvalidJobException("memory must be <0.0, not " + machineDefinition.getMemoryMB());
            }
            if (machineDefinition.getDiskMB() < 0) {
                throw new InvalidJobException("disk must be >=0, not " + machineDefinition.getDiskMB());
            }
            if (machineDefinition.getNumPorts() < 0) {
                throw new InvalidJobException("numPorts must be >=0, not " + machineDefinition.getNumPorts());
            }
        }
    }

    public String getName() {
        return name;
    }

    public String getUser() {
        return user;
    }

    public String getVersion() {
        return version;
    }

    public URL getJobJarFileLocation() {
        return jobJarFileLocation;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public JobSla getJobSla() {
        return jobSla;
    }

    public long getSubscriptionTimeoutSecs() {
        return subscriptionTimeoutSecs;
    }

    public SchedulingInfo getSchedulingInfo() {
        return schedulingInfo;
    }

    public void setSchedulingInfo(SchedulingInfo schedulingInfo) {
        this.schedulingInfo = schedulingInfo;
    }

    public int getSlaMin() {
        return slaMin;
    }

    public int getSlaMax() {
        return slaMax;
    }

    public String getCronSpec() {
        return cronSpec;
    }

    public NamedJobDefinition.CronPolicy getCronPolicy() {
        return cronPolicy;
    }

    @Override
    public String toString() {
        return "V2JobDefinition{" +
                "name='" + name + '\'' +
                ", user='" + user + '\'' +
                ", parameters=" + parameters +
                ", schedulingInfo=" + schedulingInfo +
                '}';
    }
}
