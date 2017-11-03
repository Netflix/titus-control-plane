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

package io.netflix.titus.api.store.v2;

import java.util.Collection;
import java.util.List;

import io.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.api.model.v2.MachineDefinition;
import io.netflix.titus.api.model.v2.descriptor.StageScalingPolicy;

public interface V2StageMetadata {
    String getJobId();

    int getStageNum();

    int getNumStages();

    MachineDefinition getMachineDefinition();

    int getNumWorkers();

    List<JobConstraints> getHardConstraints();

    List<JobConstraints> getSoftConstraints();

    List<String> getSecurityGroups();

    boolean getAllocateIP();

    StageScalingPolicy getScalingPolicy();

    boolean getScalable();

    AutoCloseable obtainLock();

    Collection<V2WorkerMetadata> getWorkerByIndexMetadataSet();

    Collection<V2WorkerMetadata> getAllWorkers();

    V2WorkerMetadata getWorkerByIndex(int workerIndex) throws InvalidJobException;

    V2WorkerMetadata getWorkerByWorkerNumber(int workerNumber) throws InvalidJobException;

    ServiceJobProcesses getJobProcesses();
}
