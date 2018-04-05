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

package com.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.fenzo.VirtualMachineCurrentState;
import rx.functions.Func0;

public interface VMOperations {

    void setAgentInfos(List<VirtualMachineCurrentState> agentInfos);

    void setJobsOnVMsGetter(Func0<List<JobsOnVMStatus>> func0);

    List<AgentInfo> getAgentInfos();

    List<AgentInfo> getDetachedAgentInfos();

    /**
     * Get all current tasks assigned to VMs based on provided arguments for showing idle VMs. This produces a map with
     * key of VM attribute used to set active VMs. The values of the map are the list of jobs on the VMs.
     *
     * @param idleOnly    Only include VMs that are idle, that is, not assigned any tasks.
     * @param excludeIdle Do not include VMs that are idle.
     * @return current tasks assigned to VMs.
     */
    Map<String, List<JobsOnVMStatus>> getJobsOnVMs(boolean idleOnly, boolean excludeIdle);

    class JobOnVMInfo {
        private final String jobId;
        private final String taskId;

        @JsonCreator
        public JobOnVMInfo(@JsonProperty("jobId") String jobId,
                           @JsonProperty("taskId") String taskId) {
            this.jobId = jobId;
            this.taskId = taskId;
        }

        public String getJobId() {
            return jobId;
        }

        public String getTaskId() {
            return taskId;
        }
    }

    class JobsOnVMStatus {
        private final String hostname;
        private final String attributeValue;
        private final List<JobOnVMInfo> jobs;

        @JsonCreator
        public JobsOnVMStatus(@JsonProperty("hostname") String hostname,
                              @JsonProperty("attributeValue") String attributeValue) {
            this.hostname = hostname;
            this.attributeValue = attributeValue;
            this.jobs = new ArrayList<>();
        }

        @JsonIgnore
        void addJob(JobOnVMInfo job) {
            jobs.add(job);
        }

        public String getHostname() {
            return hostname;
        }

        public String getAttributeValue() {
            return attributeValue;
        }

        public List<JobOnVMInfo> getJobs() {
            return jobs;
        }
    }

    class AgentInfo {
        private final String name;
        private final double availableCpus;
        private final double availableMemory;
        private final double availableDisk;
        private final int availableNumPorts;
        private final Map<String, Double> scalars;
        private final Map<String, String> attributes;
        private final Set<String> resourceSets;
        private final String disabledUntil;
        private final List<String> offerIds;

        @JsonCreator
        public AgentInfo(@JsonProperty("name") String name,
                         @JsonProperty("availableCpus") double availableCpus,
                         @JsonProperty("availableMemory") double availableMemory,
                         @JsonProperty("availableDisk") double availableDisk,
                         @JsonProperty("availableNumPorts") int availableNumPorts,
                         @JsonProperty("scalars") Map<String, Double> scalars,
                         @JsonProperty("attributes") Map<String, String> attributes,
                         @JsonProperty("resourceSets") Set<String> resourceSets,
                         @JsonProperty("disabledUntil") String disabledUntil,
                         @JsonProperty("offerIds") List<String> offerIds) {
            this.name = name;
            this.availableCpus = availableCpus;
            this.availableMemory = availableMemory;
            this.availableDisk = availableDisk;
            this.availableNumPorts = availableNumPorts;
            this.scalars = scalars;
            this.attributes = attributes;
            this.resourceSets = resourceSets;
            this.disabledUntil = disabledUntil;
            this.offerIds = offerIds;
        }

        public String getName() {
            return name;
        }

        public double getAvailableCpus() {
            return availableCpus;
        }

        public double getAvailableMemory() {
            return availableMemory;
        }

        public double getAvailableDisk() {
            return availableDisk;
        }

        public int getAvailableNumPorts() {
            return availableNumPorts;
        }

        public Map<String, Double> getScalars() {
            return scalars;
        }

        public Map<String, String> getAttributes() {
            return attributes;
        }

        public Set<String> getResourceSets() {
            return resourceSets;
        }

        public String getDisabledUntil() {
            return disabledUntil;
        }

        public List<String> getOfferIds() {
            return offerIds;
        }
    }
}
