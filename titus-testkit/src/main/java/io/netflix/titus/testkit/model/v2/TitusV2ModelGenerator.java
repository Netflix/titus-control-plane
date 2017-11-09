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

package io.netflix.titus.testkit.model.v2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobState;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import io.netflix.titus.api.model.v2.JobSla;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.V2JobDurationType;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.DateTimeExt;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.master.store.NamedJobs;
import io.netflix.titus.testkit.model.runtime.RuntimeModelGenerator;

import static io.netflix.titus.api.model.v2.parameter.Parameters.JobType;

/**
 * A REST layer wrapper for {@link RuntimeModelGenerator}.
 */
public class TitusV2ModelGenerator {

    private final RuntimeModelGenerator runtimeGenerator;

    public TitusV2ModelGenerator() {
        this.runtimeGenerator = new RuntimeModelGenerator();
    }

    public TitusJobSpec newJobSpec(TitusJobType type, String name) {
        JobType jobType = type == TitusJobType.batch ? JobType.Batch : JobType.Service;
        return TitusJobSpec.getSpec(runtimeGenerator.newJobMetadata(jobType, name));
    }

    public TitusJobInfo newJobInfo(TitusJobType type, String name) {
        return newJobInfo(newJobSpec(type, name));
    }

    public TitusJobInfo newJobInfo(TitusJobSpec jobSpec) {
        V2JobMetadata jobMetadata = runtimeGenerator.newJobMetadata(toV2JobDefinition(jobSpec));
        return getJob(jobMetadata.getJobId());
    }

    public TitusJobInfo scheduleJob(String jobId) {
        runtimeGenerator.scheduleJob(jobId);
        return getJob(jobId);
    }

    public void moveWorkerToState(String jobId, String taskId, TitusTaskState state) {
        runtimeGenerator.moveWorkerToState(jobId, taskId, TitusTaskState.getV2State(state));
    }


    public void moveJobToState(String jobId, TitusJobState state) {
        V2JobState v2State;
        switch (state) {
            case QUEUED:
                v2State = V2JobState.Accepted;
                break;
            case DISPATCHED:
                v2State = V2JobState.Launched;
                break;
            case FINISHED:
                v2State = V2JobState.Completed;
                break;
            case FAILED:
                v2State = V2JobState.Failed;
                break;
            default:
                throw new IllegalArgumentException("Cannot map " + state + " to V2JobState");
        }
        runtimeGenerator.moveJobToState(jobId, v2State);
    }

    public TaskInfo replaceTask(String jobId, String id, TitusTaskState finalState) {
        return toTaskInfo(runtimeGenerator.replaceTask(jobId, id, TitusTaskState.getV2State(finalState)));
    }

    public TitusJobInfo getJob(String jobId) {
        V2JobMetadata jobMetadata = runtimeGenerator.getJob(jobId);
        TitusJobSpec jobSpec = TitusJobSpec.getSpec(jobMetadata);
        Map<String, Object> titusContext = Collections.singletonMap("stack", "desktop");

        List<V2WorkerMetadata> allWorkers = new ArrayList<>(jobMetadata.getStageMetadata(1).getAllWorkers());
        List<TaskInfo> tasks = new ArrayList<>();
        allWorkers.forEach(w -> tasks.add(toTaskInfo(w)));

        return new TitusJobInfo(jobMetadata.getJobId(), jobSpec.getName(), jobSpec.getType(), jobSpec.getLabels(),
                jobSpec.getApplicationName(), jobSpec.getAppName(), jobSpec.getUser(), jobSpec.getVersion(),
                jobSpec.getEntryPoint(), jobSpec.isInService(), TitusJobState.getTitusState(jobMetadata.getState()),
                jobSpec.getInstances(), jobSpec.getInstancesMin(),
                jobSpec.getInstancesMax(), jobSpec.getInstancesDesired(), jobSpec.getCpu(), jobSpec.getMemory(),
                jobSpec.getNetworkMbps(), jobSpec.getDisk(), jobSpec.getPorts(), jobSpec.getGpu(),
                jobSpec.getJobGroupStack(), jobSpec.getJobGroupDetail(), jobSpec.getJobGroupSequence(),
                jobSpec.getCapacityGroup(), jobSpec.getMigrationPolicy(), jobSpec.getEnv(),
                titusContext, jobSpec.getRetries(), jobSpec.getRuntimeLimitSecs(), jobSpec.isAllocateIpAddress(),
                jobSpec.getIamProfile(), jobSpec.getSecurityGroups(), jobSpec.getEfs(), jobSpec.getEfsMounts(),
                DateTimeExt.toUtcDateTimeString(jobMetadata.getSubmittedAt()),
                jobSpec.getSoftConstraints(), jobSpec.getHardConstraints(), tasks, Collections.emptyList(), ServiceJobProcesses.newBuilder().build());
    }

    public List<TitusTaskInfo> getTitusTaskInfos(String jobId) {
        V2JobMetadata job = runtimeGenerator.getJob(jobId);
        TitusJobSpec jobSpec = TitusJobSpec.getSpec(job);

        List<TitusTaskInfo> result = new ArrayList<>();
        for (V2WorkerMetadata mwmd : job.getStageMetadata(1).getAllWorkers()) {
            Map<Integer, Integer> portMapping = new HashMap<>();
            final int[] portsRequested = jobSpec.getPorts();
            if (V2JobState.isRunningState(mwmd.getState()) && portsRequested != null && portsRequested.length > 0) {
                final List<Integer> portsAssigned = mwmd.getPorts();
                int idx = 0;
                for (int p : portsRequested) {
                    if (idx < portsAssigned.size()) {
                        portMapping.put(p, portsAssigned.get(idx));
                    }
                    idx++;
                }
            }
            TitusTaskState titusState = TitusTaskState.getTitusState(mwmd.getState(), mwmd.getReason());
            TitusTaskInfo taskInfo = new TitusTaskInfo(
                    WorkerNaming.getWorkerName(jobId, mwmd.getWorkerIndex(), mwmd.getWorkerNumber()),
                    mwmd.getWorkerInstanceId(),
                    jobId,
                    titusState,
                    jobSpec.getApplicationName(), jobSpec.getCpu(), jobSpec.getMemory(), jobSpec.getNetworkMbps(), jobSpec.getDisk(),
                    portMapping, jobSpec.getGpu(), jobSpec.getEnv(), jobSpec.getVersion(), jobSpec.getEntryPoint(), mwmd.getSlave(),
                    DateTimeExt.toUtcDateTimeString(mwmd.getAcceptedAt()),
                    DateTimeExt.toUtcDateTimeString(mwmd.getLaunchedAt()),
                    DateTimeExt.toUtcDateTimeString(mwmd.getStartedAt()),
                    DateTimeExt.toUtcDateTimeString(mwmd.getCompletedAt()),
                    DateTimeExt.toUtcDateTimeString(mwmd.getMigrationDeadline()),
                    mwmd.getCompletionMessage(),
                    mwmd.getStatusData(),
                    "s3://stdout.live",
                    "s3://logs",
                    "s3://snapshots",
                    mwmd.getSlaveAttributes().get("id"),
                    mwmd.getSlaveAttributes().get("itype")
            );

            result.add(taskInfo);
        }

        return result;
    }

    private TaskInfo toTaskInfo(V2WorkerMetadata mwmd) {
        String id = WorkerNaming.getWorkerName(mwmd.getJobId(), mwmd.getWorkerIndex(), mwmd.getWorkerNumber());
        String host = mwmd.getSlave();
        String hostInstanceId = mwmd.getSlaveAttributes().get("id");
        String itype = mwmd.getSlaveAttributes().get("itype");
        TitusTaskState state = TitusTaskState.getTitusState(mwmd.getState(), mwmd.getReason());
        return new TaskInfo(
                id,
                mwmd.getWorkerInstanceId(),
                state,
                host,
                hostInstanceId,
                itype,
                "us-east-1",
                "us-east-1c",
                DateTimeExt.toUtcDateTimeString(mwmd.getAcceptedAt()),
                DateTimeExt.toUtcDateTimeString(mwmd.getLaunchedAt()),
                DateTimeExt.toUtcDateTimeString(mwmd.getStartedAt()),
                DateTimeExt.toUtcDateTimeString(mwmd.getCompletedAt()),
                DateTimeExt.toUtcDateTimeString(mwmd.getMigrationDeadline()),
                mwmd.getCompletionMessage(), mwmd.getStatusData(),
                "s3://stdout.live",
                "s3://logs",
                "s3://snapshots"
        );
    }

    private static V2JobDefinition toV2JobDefinition(TitusJobSpec jobSpec) {
        final List<Parameter> parameters = TitusJobSpec.getParameters(jobSpec);
        return new V2JobDefinition(NamedJobs.TitusJobName, jobSpec.getUser(), null,
                jobSpec.getVersion(), parameters,
                new JobSla(jobSpec.getRetries(), jobSpec.getRuntimeLimitSecs(), 0L, JobSla.StreamSLAType.Lossy,
                        jobSpec.getType() == TitusJobType.service ?
                                V2JobDurationType.Perpetual : V2JobDurationType.Transient,
                        null),
                0L, jobSpec.getSchedulingInfo(), 0, 0, null, null
        );
    }
}
