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

package io.netflix.titus.testkit.model.runtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.netflix.titus.api.model.EfsMount;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.JobSla;
import io.netflix.titus.api.model.v2.MachineDefinition;
import io.netflix.titus.api.model.v2.ServiceJobProcesses;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.V2JobDurationType;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import io.netflix.titus.api.model.v2.descriptor.StageScalingPolicy;
import io.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.model.v2.parameter.Parameters.JobType;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.master.store.InvalidJobStateChangeException;
import io.netflix.titus.master.store.V2JobMetadataWritable;
import io.netflix.titus.master.store.V2StageMetadataWritable;
import io.netflix.titus.master.store.V2WorkerMetadataWritable;
import io.netflix.titus.testkit.util.TitusTaskIdParser;

import static io.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_CELL;
import static io.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_STACK;
import static io.netflix.titus.common.util.CollectionsExt.asMap;
import static java.util.Arrays.asList;

/**
 * Data generator for v2 runtime and storage layers. The runtime/storage model has a complex set of
 * inter-dependencies which makes creating correctly initialized objects difficult.
 * {@link RuntimeModelGenerator} aims at simplifying this process by creating the entities in their initial state
 * and updating them over their lifecycle according to a user requests. The data generation flow covers:
 * <ul>
 * <li>{@link V2JobDefinition} generation</li>
 * <li>{@link V2JobMetadata} creation from an existing {@link V2JobDefinition}. The job is in its initial unscheduled state</li>
 * <li>scheduling of {@link V2JobMetadata} which results in creation of {@link V2WorkerMetadata} instances. The workers are set to STARTING state initially.</li>
 * <li>{@link V2WorkerMetadata} state manipulation (RUNNING, CRASHED, STOPPED, etc)</li>
 * <li>{@link V2WorkerMetadata} workers rescheduling</li>
 * <li>service job scale up/scale down</li>
 * <li>job termination and archiving</li>
 * </ul>
 */
public class RuntimeModelGenerator {

    private static final Map<V2JobState, V2JobState> NEXT_STATES = new ImmutableMap.Builder<V2JobState, V2JobState>()
            .put(V2JobState.Accepted, V2JobState.Launched)
            .put(V2JobState.Launched, V2JobState.StartInitiated)
            .put(V2JobState.StartInitiated, V2JobState.Started)
            .put(V2JobState.Started, V2JobState.Completed)
            .build();

    private int jobIdx;

    private final String cell;
    private final Map<String, V2JobDefinition> jobDefinitions = new HashMap<>();
    private final Map<String, V2JobMetadata> jobsMetadata = new HashMap<>();
    private final Map<String, V2JobMetadata> archivedJobsMetadata = new HashMap<>();

    public RuntimeModelGenerator(String cell) {
        this.cell = cell;
    }

    public V2JobDefinition newJobDefinition(JobType jobType, String name) {
        return newJobDefinition(jobType, name, null, null, 0.0);
    }

    public V2JobDefinition newJobDefinition(JobType jobType, String name, String capacityGroup, Long runtimeLimitSecs, double gpu) {
        List<String> securityGroups = asList("sg-0001", "sg-0002");

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(Parameters.newCmdTypeParameter("titus"));
        parameters.add(Parameters.newJobTypeParameter(jobType));
        parameters.add(Parameters.newNameParameter(name));
        parameters.add(Parameters.newAppNameParameter(name));
        if (capacityGroup != null) {
            parameters.add(Parameters.newCapacityGroup(capacityGroup));
        }
        parameters.add(Parameters.newImageNameParameter("testImage"));
        parameters.add(Parameters.newRestartOnSuccessParameter(false));
        parameters.add(Parameters.newLabelsParameter(asMap(
                JOB_ATTRIBUTES_CELL, cell,
                JOB_ATTRIBUTES_STACK, cell,
                "labelA", "valueA"
        )));
        parameters.add(Parameters.newEntryPointParameter("./testApp.sh"));
        parameters.add(Parameters.newEnvParameter(asMap("EVN_PARAM_A", "envValueA")));
        parameters.add(Parameters.newIamProfileParameter("testIamProfile"));
        parameters.add(Parameters.newInServiceParameter(true));
        parameters.add(Parameters.newJobGroupStack("testStack"));
        parameters.add(Parameters.newJobGroupSequence("001"));
        parameters.add(Parameters.newJobGroupDetail("testDetail"));
        parameters.add(Parameters.newPortsParameter(new int[]{8080, 8081}));
        parameters.add(Parameters.newSecurityGroupsParameter(securityGroups));
        parameters.add(Parameters.newVersionParameter("latest"));
        List<EfsMount> efsMounts = new ArrayList<>();
        efsMounts.add(new EfsMount("id1", "/mount1", EfsMount.MountPerm.RW, null));
        efsMounts.add(new EfsMount("id2", "/mount2", EfsMount.MountPerm.RW, "/relative"));
        parameters.add(Parameters.newEfsMountsParameter(efsMounts));

        JobSla jobSla = new JobSla(
                0,
                runtimeLimitSecs,
                600,
                JobSla.StreamSLAType.Lossy,
                jobType == JobType.Batch ? V2JobDurationType.Transient : V2JobDurationType.Perpetual,
                null
        );

        MachineDefinition machineDefinition = new MachineDefinition(
                1, 1024, 100, 512, jobType == JobType.Service ? 1 : 0, gpu > 0.0 ? Collections.singletonMap("gpu", 1.0) : Collections.emptyMap()
        );

        StageScalingPolicy scalingPolicy;
        if (jobType == JobType.Batch) {
            scalingPolicy = new StageScalingPolicy(
                    1,
                    1, 1, 1, 1, 0, 0, null
            );
        } else {
            scalingPolicy = new StageScalingPolicy(
                    1,
                    1, 5, 2, 1, 1, 60, null
            );
        }

        StageSchedulingInfo stage = new StageSchedulingInfo(
                1,
                machineDefinition,
                Collections.emptyList(),
                Collections.emptyList(),
                securityGroups,
                jobType == JobType.Service,
                scalingPolicy,
                false
        );

        SchedulingInfo schedulingInfo = new SchedulingInfo(Collections.singletonMap(1, stage));

        return new V2JobDefinition(
                name,
                "testUser",
                null,
                "latest",
                parameters,
                jobSla,
                3600,
                schedulingInfo,
                0,
                0,
                null,
                null
        );
    }

    public V2JobMetadata newJobMetadata(JobType jobType, String name) {
        return newJobMetadata(newJobDefinition(jobType, name));
    }

    public V2JobMetadata newJobMetadata(JobType jobType, String name, double gpu) {
        return newJobMetadata(newJobDefinition(jobType, name, null, null, gpu));
    }

    public V2JobMetadata newJobMetadata(JobType jobType, String name, String capacityGroup) {
        return newJobMetadata(newJobDefinition(jobType, name, capacityGroup, null, 0.0));
    }

    public V2JobMetadata newJobMetadata(JobType jobType, String name, String capacityGroup, Long runtimeLimitSeconds) {
        return newJobMetadata(newJobDefinition(jobType, name, capacityGroup, runtimeLimitSeconds, 0.0));
    }


    public V2JobMetadata newJobMetadata(V2JobDefinition jobDefinition) {
        String jobId = createJobId();

        V2JobMetadataWritable jobMetadata = new V2JobMetadataWritable(
                jobId,
                jobDefinition.getName(),
                jobDefinition.getUser(),
                System.currentTimeMillis(),
                null,
                1,
                jobDefinition.getJobSla(),
                V2JobState.Accepted,
                jobDefinition.getSubscriptionTimeoutSecs(),
                jobDefinition.getParameters(),
                1
        );

        StageSchedulingInfo schedulingInfo = jobDefinition.getSchedulingInfo().getStages().get(1);
        V2StageMetadataWritable stage = new V2StageMetadataWritable(
                jobId,
                1,
                1,
                schedulingInfo.getMachineDefinition(),
                schedulingInfo.getNumberOfInstances(),
                schedulingInfo.getHardConstraints(),
                schedulingInfo.getSoftConstraints(),
                schedulingInfo.getSecurityGroups(),
                schedulingInfo.getAllocateIP(),
                schedulingInfo.getScalingPolicy(),
                schedulingInfo.getScalable(),
                ServiceJobProcesses.newBuilder().build()
        );
        jobMetadata.addJobStageIfAbsent(stage);

        jobDefinitions.put(jobId, jobDefinition);
        jobsMetadata.put(jobId, jobMetadata);

        return jobMetadata;
    }

    /**
     * Create tasks for a job.
     *
     * @throws IllegalStateException if the job has tasks already scheduled
     */
    public V2JobMetadata scheduleJob(String jobId) {
        V2JobMetadataWritable job = (V2JobMetadataWritable) jobsMetadata.get(jobId);
        Preconditions.checkNotNull(job, "No job with id " + jobId);

        V2StageMetadataWritable jobStage = (V2StageMetadataWritable) job.getStageMetadata(1);
        if (!jobStage.getAllWorkers().isEmpty()) {
            throw new IllegalStateException("Job " + jobId + " has workers already assigned");
        }

        for (int i = 0; i < jobStage.getScalingPolicy().getDesired(); i++) {
            V2WorkerMetadataWritable worker = new V2WorkerMetadataWritable(
                    i,
                    i,
                    jobId,
                    "tid:uuid-" + i,
                    1,
                    jobStage.getMachineDefinition().getNumPorts(),
                    cell
            );
            try {
                jobStage.replaceWorkerIndex(worker, null);
            } catch (InvalidJobException e) {
                throw new IllegalStateException("Unexpected problem when adding new worker for the job " + jobId);
            }
        }
        job.setNextWorkerNumberToUse(jobStage.getScalingPolicy().getDesired() + 1);
        try {
            job.setJobState(V2JobState.Launched);
        } catch (InvalidJobStateChangeException e) {
            throw new IllegalStateException("Invalid job  state transition", e);
        }

        return job;
    }

    public void moveWorkerToState(String jobId, String taskId, V2JobState state) {
        for (V2WorkerMetadata worker : getAllWorkers(jobId)) {
            String workerId = WorkerNaming.getWorkerName(worker.getJobId(), worker.getWorkerIndex(), worker.getWorkerNumber());
            if (workerId.equals(taskId)) {
                moveToWorkerState(taskId, state, (V2WorkerMetadataWritable) worker);
                return;
            }
        }
    }

    public void moveWorkerToState(String jobId, int workerIndex, V2JobState state) {
        for (V2WorkerMetadata worker : getAllWorkers(jobId)) {
            if (worker.getWorkerIndex() == workerIndex) {
                String workerId = WorkerNaming.getWorkerName(worker.getJobId(), worker.getWorkerIndex(), worker.getWorkerNumber());
                moveToWorkerState(workerId, state, (V2WorkerMetadataWritable) worker);
                return;
            }
        }
        throw new IllegalArgumentException(jobId + " has no worker with index " + workerIndex);
    }

    private void moveToWorkerState(String workerId, V2JobState state, V2WorkerMetadataWritable worker) {
        try {
            while (worker.getState() != state) {
                V2JobState next = NEXT_STATES.get(worker.getState());
                if (next == null) {
                    throw new IllegalStateException("Worker " + workerId + " has no valid state transition to " + state);
                }
                worker.setState(next, System.currentTimeMillis(), null);
                if (next == V2JobState.StartInitiated) {
                    worker.setStartingAt(System.currentTimeMillis());
                } else if (next == V2JobState.Started) {
                    worker.setStartedAt(System.currentTimeMillis());
                    worker.setSlave("slave.host");
                    worker.setSlaveID("slaveId");
                }
            }
        } catch (InvalidJobStateChangeException e) {
            throw new IllegalStateException("Worker " + workerId + " cannot move to state " + state, e);
        }
    }

    public void moveJobToState(String jobId, V2JobState state) {
        getAllWorkers(jobId).forEach(t -> {
            if (!V2JobState.isTerminalState(t.getState())) {
                moveWorkerToState(jobId, t.getWorkerIndex(), state);
            }
        });
        V2JobMetadataWritable job = (V2JobMetadataWritable) getJob(jobId);
        try {
            job.setJobState(state);
        } catch (InvalidJobStateChangeException e) {
            throw new IllegalStateException("Invalid job state transition", e);
        }
    }

    public V2WorkerMetadata replaceTask(String jobId, String taskId, V2JobState finalState) {
        Preconditions.checkArgument(V2JobState.isTerminalState(finalState));
        moveWorkerToState(jobId, taskId, finalState);

        V2WorkerMetadata replacedTask = getTask(jobId, taskId);
        int taskIdx = TitusTaskIdParser.getTaskIndexFromTaskId(taskId);

        V2JobMetadataWritable job = (V2JobMetadataWritable) jobsMetadata.get(jobId);
        V2StageMetadataWritable jobStage = (V2StageMetadataWritable) job.getStageMetadata(1);
        V2WorkerMetadataWritable worker = new V2WorkerMetadataWritable(
                taskIdx,
                job.getNextWorkerNumberToUse(),
                jobId,
                "tid:uuid-" + job.getNextWorkerNumberToUse(),
                1,
                jobStage.getMachineDefinition().getNumPorts(),
                replacedTask.getCell()
        );
        try {
            jobStage.replaceWorkerIndex(worker, replacedTask);
        } catch (InvalidJobException e) {
            throw new IllegalStateException("Unexpected problem when adding new worker for the job " + jobId);
        }

        return worker;
    }

    public V2JobMetadata getJob(String jobId) {
        V2JobMetadata job = jobsMetadata.get(jobId);
        if (job == null) {
            job = archivedJobsMetadata.get(jobId);
        }
        return job;
    }

    public List<V2WorkerMetadata> getAllWorkers(String jobId) {
        return new ArrayList<>(getJob(jobId).getStageMetadata(1).getAllWorkers());
    }

    public List<V2WorkerMetadata> getRunningWorkers(String jobId) {
        return getAllWorkers(jobId).stream().filter(w -> V2JobState.isRunningState(w.getState())).collect(Collectors.toList());
    }

    public V2WorkerMetadata getTask(String jobId, String taskId) {
        return getAllWorkers(jobId).stream().filter(t -> {
            String tid = WorkerNaming.getWorkerName(jobId, t.getWorkerIndex(), t.getWorkerNumber());
            return tid.equals(taskId);
        }).findFirst().orElse(null);
    }

    public Set<V2JobDefinition> getAllJobsDefinition() {
        return new HashSet<>(jobDefinitions.values());
    }

    public List<V2JobMetadata> getAllJobsMetadata(boolean includeArchived) {
        List<V2JobMetadata> result = new ArrayList<>(jobsMetadata.values());
        if (includeArchived) {
            result.addAll(archivedJobsMetadata.values());
        }
        return result;
    }

    public void killJob(String jobId) {
        V2JobMetadataWritable job = (V2JobMetadataWritable) jobsMetadata.get(jobId);
        Preconditions.checkNotNull(job, "No job with id " + jobId);

        for (V2WorkerMetadata worker : job.getStageMetadata(1).getAllWorkers()) {
            try {
                ((V2WorkerMetadataWritable) worker).setState(
                        V2JobState.Completed,
                        System.currentTimeMillis(),
                        JobCompletedReason.Killed
                );
            } catch (InvalidJobStateChangeException e) {
                throw new IllegalStateException(e);
            }
        }
        jobsMetadata.remove(jobId);
        archivedJobsMetadata.put(jobId, job);
    }

    private String createJobId() {
        return "Titus-" + jobIdx++;
    }
}
