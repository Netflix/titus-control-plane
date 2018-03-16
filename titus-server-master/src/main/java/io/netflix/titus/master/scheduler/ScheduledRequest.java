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

package io.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.queues.QAttributes;
import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.api.model.v2.MachineDefinition;
import io.netflix.titus.api.model.v2.V2JobDurationType;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.job.V2JobMgrIntf;
import io.netflix.titus.master.job.worker.WorkerRequest;
import io.netflix.titus.master.model.job.TitusQueuableTask;
import io.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import io.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import io.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;

/**
 */
public class ScheduledRequest implements TitusQueuableTask<V2JobMetadata, V2WorkerMetadata> {

    private static final String defaultGrpName = "defaultGrp";
    private static final String SecurityGroupsResName = "ENIs";
    private final V2JobMgrIntf jobMgr;
    private final V2JobMetadata job;
    private final V2WorkerMetadata task;
    private final MasterConfiguration config;
    private final WorkerRequest request;
    private final String requestId;
    private final ConstraintEvaluatorTransformer<JobConstraints> v2ConstraintEvaluatorTransformer;
    private final V2JobDurationType durationType;
    private final int workerIndex;
    private final int workerNumber;
    private final int stageNum;
    private final MachineDefinition machineDefinition;
    private List<ConstraintEvaluator> hardConstraints;
    private List<VMTaskFitnessCalculator> softConstraints;
    private final Map<String, NamedResourceSetRequest> namedResources = new HashMap<>();
    private AssignedResources assignedResources = null;
    private final QAttributes qAttributes;

    private static String getConcatenatedString(List<String> strings) {
        return String.join(":", strings);
    }

    public static QAttributes getQAttributes(V2JobMgrIntf jobMgrIntf, ApplicationSlaManagementService applicationSlaManagementService) {
        final V2JobMetadata jobMetadata = jobMgrIntf.getJobMetadata(true);
        List<Parameter> parameters = jobMetadata.getParameters();
        String capacityGroup = Parameters.getCapacityGroup(parameters);
        if (capacityGroup == null) {
            capacityGroup = Parameters.getAppName(parameters);
        }
        if (capacityGroup == null) {
            capacityGroup = ApplicationSlaManagementService.DEFAULT_APPLICATION;
        }
        ApplicationSLA appSla = applicationSlaManagementService.getApplicationSLA(capacityGroup);
        final ApplicationSLA sla = appSla == null ?
                applicationSlaManagementService.getApplicationSLA(ApplicationSlaManagementService.DEFAULT_APPLICATION) :
                appSla;
        if (sla == null) {
            throw new RuntimeException("Can't get default application sla");
        }
        return new QAttributes() {
            @Override
            public String getBucketName() {
                return sla.getAppName();
            }

            @Override
            public int getTierNumber() {
                return sla.getTier().ordinal();
            }
        };
    }

    public ScheduledRequest(V2JobMgrIntf jobMgr, V2WorkerMetadata task, WorkerRequest request, MasterConfiguration config,
                            ConstraintEvaluatorTransformer<JobConstraints> v2ConstraintEvaluatorTransformer,
                            SystemSoftConstraint systemSoftConstraint,
                            SystemHardConstraint systemHardConstraint,
                            ApplicationSlaManagementService applicationSlaManagementService) {
        this.config = config;
        this.jobMgr = jobMgr;
        this.request = request;
        this.workerIndex = request.getWorkerIndex();
        this.workerNumber = request.getWorkerNumber();
        this.machineDefinition = request.getDefinition();
        this.stageNum = request.getWorkerStage();
        this.durationType = jobMgr.getJobMetadata().getSla().getDurationType();
        this.v2ConstraintEvaluatorTransformer = v2ConstraintEvaluatorTransformer;
        requestId = WorkerNaming.getWorkerName(jobMgr.getJobId(), workerIndex, workerNumber);
        final List<V2WorkerMetadata.TwoLevelResource> twoLevelResources = request.getTwoLevelResource();
        if (twoLevelResources != null && !twoLevelResources.isEmpty()) {
            assignedResources = new AssignedResources();
            List<PreferentialNamedConsumableResourceSet.ConsumeResult> consumeResults = new ArrayList<>();
            for (V2WorkerMetadata.TwoLevelResource resource : twoLevelResources) {
                consumeResults.add(
                        new PreferentialNamedConsumableResourceSet.ConsumeResult(
                                Integer.parseInt(resource.getLabel()), resource.getAttrName(),
                                resource.getResName(), 1.0)
                );
            }
            assignedResources.setConsumedNamedResources(consumeResults);
        }
        setupConstraints(jobMgr, systemSoftConstraint, systemHardConstraint);
        setupCustomNamedResources(request);
        qAttributes = getQAttributes(jobMgr, applicationSlaManagementService);

        this.job = jobMgr.getJobMetadata();
        this.task = task;
    }

    private void setupCustomNamedResources(WorkerRequest request) {
        if (config.getDisableSecurityGroupsAssignments()) {
            return;
        }
        List<String> securityGroups = request.getSecurityGroups();
        if (securityGroups != null && !securityGroups.isEmpty()) {
            NamedResourceSetRequest resourceSetRequest = new NamedResourceSetRequest(
                    SecurityGroupsResName, getConcatenatedString(securityGroups),
                    1, 1
            );
            namedResources.put(resourceSetRequest.getResName(), resourceSetRequest);
        }
    }

    private void setupConstraints(V2JobMgrIntf jobMgr, SystemSoftConstraint systemSoftConstraint, SystemHardConstraint systemHardConstraint) {
        V2StageMetadata stageMetadata = jobMgr.getJobMetadata().getStageMetadata(stageNum);
        List<JobConstraints> stageHC = stageMetadata.getHardConstraints();
        List<JobConstraints> stageSC = stageMetadata.getSoftConstraints();
        softConstraints = new ArrayList<>();
        softConstraints.add(systemSoftConstraint);
        hardConstraints = new ArrayList<>();
        hardConstraints.add(systemHardConstraint);

        if ((stageHC == null || stageHC.isEmpty()) && (stageSC == null || stageSC.isEmpty())) {
            return;
        }
        final Set<String> coTasks = new HashSet<>();
        for (V2WorkerMetadata mwmd : stageMetadata.getWorkerByIndexMetadataSet()) {
            if (mwmd.getWorkerNumber() != workerNumber) {
                coTasks.add(WorkerNaming.getWorkerName(jobMgr.getJobId(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber()));
            }
        }
        if (stageHC != null && !stageHC.isEmpty()) {
            for (JobConstraints c : stageHC) {
                v2ConstraintEvaluatorTransformer.hardConstraint(c, () -> coTasks).ifPresent(hardConstraints::add);
            }
        }
        if (stageSC != null && !stageSC.isEmpty()) {
            for (JobConstraints c : stageSC) {
                v2ConstraintEvaluatorTransformer.softConstraint(c, () -> coTasks).ifPresent(softConstraints::add);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        return o != null && o instanceof ScheduledRequest && getId().equals(((ScheduledRequest) o).getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String getId() {
        return requestId;
    }

    @Override
    public String taskGroupName() {
        return defaultGrpName;
    }

    @Override
    public double getCPUs() {
        return machineDefinition.getCpuCores();
    }

    @Override
    public double getMemory() {
        return machineDefinition.getMemoryMB();
    }

    @Override
    public double getNetworkMbps() {
        return machineDefinition.getNetworkMbps();
    }

    @Override
    public double getDisk() {
        return machineDefinition.getDiskMB();
    }

    @Override
    public int getPorts() {
        // this gets called for scheduling purposes, only when request is non-null. But, for a worker that was
        // already running from before master restarted, this object is created with request=null. To provide
        // an answer to the caller, we defer to WorkerRequest. Note that this
        // call isn't expected to happen for workers that are already running, so there is no confusion here.
        return request == null ? WorkerRequest.getNumPortsPerInstance(machineDefinition) : request.getNumPortsPerInstance();
    }

    @Override
    public Map<String, Double> getScalarRequests() {
        return machineDefinition.getScalars();
    }

    @Override
    public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
        return namedResources;
    }

    @Override
    public List<ConstraintEvaluator> getHardConstraints() {
        return hardConstraints;
    }

    @Override
    public List<VMTaskFitnessCalculator> getSoftConstraints() {
        return softConstraints;
    }

    @Override
    public QAttributes getQAttributes() {
        return qAttributes;
    }

    public V2JobDurationType getDurationType() {
        return durationType;
    }

    public V2JobMgrIntf getJobMgr() {
        return jobMgr;
    }

    public int getWorkerNumber() {
        return workerNumber;
    }

    public int getStageNum() {
        return stageNum;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public WorkerRequest getRequest() {
        return request;
    }

    @Override
    public void setAssignedResources(AssignedResources assignedResources) {
        this.assignedResources = assignedResources;
    }

    @Override
    public AssignedResources getAssignedResources() {
        return assignedResources;
    }

    @Override
    public V2JobMetadata getJob() {
        return job;
    }

    @Override
    public V2WorkerMetadata getTask() {
        return task;
    }
}
