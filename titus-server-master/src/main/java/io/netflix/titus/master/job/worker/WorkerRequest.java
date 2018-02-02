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

package io.netflix.titus.master.job.worker;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.api.model.v2.JobSla;
import io.netflix.titus.api.model.v2.MachineDefinition;
import io.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;

public class WorkerRequest {

    public static final String V2_NETFLIX_APP_METADATA = "NETFLIX_APP_METADATA";
    public static final String V2_NETFLIX_APP_METADATA_SIG = "NETFLIX_APP_METADATA_SIG";

    private String jobName;
    private String jobId;
    private int workerIndex;
    private int workerNumber;
    private final String workerInstanceId;
    private URL jobJarUrl;
    private int workerStage;
    private int totalStages;
    private MachineDefinition definition;
    private int numInstancesAtStage;
    private int numPortsPerInstance;
    private int metricsPort = -1;
    private int debugPort = -1;
    private List<Integer> ports;
    private List<Parameter> parameters;
    private JobSla jobSla;
    private List<JobConstraints> hardConstraints;
    private List<JobConstraints> softConstraints;
    private List<String> securityGroups;
    private List<V2WorkerMetadata.TwoLevelResource> twoLevelResources = Collections.emptyList();
    private boolean allocateIP;
    private SchedulingInfo schedulingInfo;
    private final boolean isStreamJob;
    private String metatronAppMetadata;
    private String metatronAppSignature;

    public WorkerRequest(MachineDefinition definition, String jobId,
                         int workerIndex, int workerNumber, String workerInstanceId,
                         URL jobJarUrl, int workerStage, int totalStages,
                         int numInstancesAtStage,
                         String jobName, int numPortsPerInstance,
                         List<Parameter> parameters, JobSla jobSla,
                         List<JobConstraints> hardConstraints, List<JobConstraints> softConstraints,
                         List<String> securityGroups, boolean allocateIP, SchedulingInfo schedulingInfo, boolean isStreamJob) {
        this.definition = definition;
        this.jobId = jobId;
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.workerInstanceId = workerInstanceId;
        this.jobJarUrl = jobJarUrl;
        this.workerStage = workerStage;
        this.totalStages = totalStages;
        this.numInstancesAtStage = numInstancesAtStage;
        this.jobName = jobName;
        // add additional ports for metricsPort and debugPort for stream job
        this.numPortsPerInstance = isStreamJob ? numPortsPerInstance + 2 : numPortsPerInstance;
        ports = new ArrayList<>();
        this.parameters = parameters;
        this.jobSla = jobSla;
        this.hardConstraints = hardConstraints;
        this.softConstraints = softConstraints;
        this.securityGroups = securityGroups;
        this.allocateIP = allocateIP;
        this.schedulingInfo = schedulingInfo;
        this.isStreamJob = isStreamJob;

        // Populate Metatron info from labels in v2
        Map<String, String> labels = Parameters.getLabels(parameters);
        this.metatronAppMetadata = labels.get(V2_NETFLIX_APP_METADATA);
        this.metatronAppSignature = labels.get(V2_NETFLIX_APP_METADATA_SIG);
    }

    public SchedulingInfo getSchedulingInfo() {
        return schedulingInfo;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public MachineDefinition getDefinition() {
        return definition;
    }

    public String getJobId() {
        return jobId;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public int getWorkerNumber() {
        return workerNumber;
    }

    public String getWorkerInstanceId() {
        return workerInstanceId;
    }

    public URL getJobJarUrl() {
        return jobJarUrl;
    }

    public int getWorkerStage() {
        return workerStage;
    }

    public int getTotalStages() {
        return totalStages;
    }

    public int getNumInstancesAtStage() {
        return numInstancesAtStage;
    }

    public String getJobName() {
        return jobName;
    }

    public int getNumPortsPerInstance() {
        return numPortsPerInstance;
    }

    public static int getNumPortsPerInstance(MachineDefinition machineDefinition) {
        return machineDefinition.getNumPorts() + 1;
    }

    public int getMetricsPort() {
        return metricsPort;
    }

    public int getDebugPort() {
        return debugPort;
    }

    public void addPort(int port) {
        if (isStreamJob && metricsPort == -1) {
            metricsPort = port; // fill metricsPort first
        } else if (isStreamJob && debugPort == -1) {
            debugPort = port; // fill debug port next
        } else {
            ports.add(port);
        }
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public List<Integer> getAllPortsUsed() {
        List<Integer> allPorts = new ArrayList<>(ports);
        if (isStreamJob) {
            allPorts.add(metricsPort);
            allPorts.add(debugPort);
        }
        return allPorts;
    }

    public JobSla getJobSla() {
        return jobSla;
    }

    public List<JobConstraints> getHardConstraints() {
        return hardConstraints;
    }

    public List<JobConstraints> getSoftConstraints() {
        return softConstraints;
    }

    public List<String> getSecurityGroups() {
        return securityGroups;
    }

    public void setTwoLevelResource(List<V2WorkerMetadata.TwoLevelResource> twoLevelResources) {
        this.twoLevelResources = twoLevelResources;
    }

    public List<V2WorkerMetadata.TwoLevelResource> getTwoLevelResource() {
        return twoLevelResources;
    }

    public boolean getAllocateIP() {
        return allocateIP;
    }

    @Override
    public String toString() {
        return jobId + "-Stage-" + workerStage + "-Worker-" + workerIndex;
    }

    public static String generateWorkerInstanceId() {
        return UUID.randomUUID().toString();
    }

    public String getMetatronAppMetadata() {
        return metatronAppMetadata;
    }

    public String getMetatronAppSignature() {
        return metatronAppSignature;
    }
}
