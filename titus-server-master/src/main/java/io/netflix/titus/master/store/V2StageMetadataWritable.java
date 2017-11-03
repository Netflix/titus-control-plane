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

package io.netflix.titus.master.store;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.api.model.v2.MachineDefinition;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.descriptor.StageScalingPolicy;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V2StageMetadataWritable implements V2StageMetadata {
    private String jobId;
    private int stageNum;
    private int numStages;
    private MachineDefinition machineDefinition;
    private int numWorkers;
    private List<JobConstraints> hardConstraints;
    private List<JobConstraints> softConstraints;
    private final List<String> securityGroups;
    private final boolean allocateIP;
    private StageScalingPolicy scalingPolicy;
    private boolean scalable;
    private ServiceJobProcesses jobProcesses;
    @JsonIgnore
    private final ConcurrentMap<Integer, V2WorkerMetadata> workerByIndexMetadataSet;
    @JsonIgnore
    private final ConcurrentMap<Integer, V2WorkerMetadata> workerByNumberMetadataSet;
    @JsonIgnore
    private final ReentrantLock lock = new ReentrantLock();
    private static final Logger logger = LoggerFactory.getLogger(V2StageMetadataWritable.class);

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public V2StageMetadataWritable(@JsonProperty("jobId") String jobId,
                                   @JsonProperty("stageNum") int stageNum,
                                   @JsonProperty("numStages") int numStages,
                                   @JsonProperty("machineDefinition") MachineDefinition machineDefinition,
                                   @JsonProperty("numWorkers") int numWorkers,
                                   @JsonProperty("hardConstraints") List<JobConstraints> hardConstraints,
                                   @JsonProperty("softConstraints") List<JobConstraints> softConstraints,
                                   @JsonProperty("securityGroups") List<String> securityGroups,
                                   @JsonProperty("allocateIP") boolean allocateIP,
                                   @JsonProperty("scalingPolicy") StageScalingPolicy scalingPolicy,
                                   @JsonProperty("scalable") boolean scalable,
                                   @JsonProperty("jobProcesses") ServiceJobProcesses jobProcesses) {
        this.jobId = jobId;
        this.stageNum = stageNum;
        this.numStages = numStages;
        this.machineDefinition = machineDefinition;
        this.numWorkers = numWorkers;
        this.hardConstraints = hardConstraints;
        this.softConstraints = softConstraints;
        this.securityGroups = securityGroups;
        this.allocateIP = allocateIP;
        this.scalingPolicy = scalingPolicy;
        this.scalable = scalable;
        this.jobProcesses = jobProcesses;
        workerByIndexMetadataSet = new ConcurrentHashMap<>();
        workerByNumberMetadataSet = new ConcurrentHashMap<>();
    }
    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public int getStageNum() {
        return stageNum;
    }

    @Override
    public int getNumStages() {
        return numStages;
    }

    @Override
    public int getNumWorkers() {
        return numWorkers;
    }

    // This call is unsafe to be called by itself. Typically this is called from within a block that
    // locks this object and also does the right things for reflecting upon the change. E.g., if increasing
    // the number, then create the new workers. When decrementing, call the unsafeRemoveWorker() to remove
    // the additional workers.
    public void unsafeSetNumWorkers(int numWorkers) {
        this.numWorkers = numWorkers;
    }

    public boolean unsafeRemoveWorker(int index, int number) {
        final V2WorkerMetadata removedIdx = workerByIndexMetadataSet.remove(index);
        final V2WorkerMetadata removedNum = workerByNumberMetadataSet.remove(number);
        return removedIdx != null && removedNum != null && removedIdx.getWorkerNumber() == number &&
                removedNum.getWorkerIndex() == index;
    }

    @Override
    public List<JobConstraints> getHardConstraints() {
        return hardConstraints;
    }

    @Override
    public List<JobConstraints> getSoftConstraints() {
        return softConstraints;
    }

    @Override
    public List<String> getSecurityGroups() {
        return securityGroups;
    }

    @Override
    public boolean getAllocateIP() {
        return allocateIP;
    }

    @Override
    public StageScalingPolicy getScalingPolicy() {
        return scalingPolicy;
    }

    public void setScalingPolicy(StageScalingPolicy scalingPolicy) {
        this.scalingPolicy = scalingPolicy;
    }

    public void setJobProcesses(ServiceJobProcesses jobProcesses) {
        this.jobProcesses = jobProcesses;
    }

    @Override
    public boolean getScalable() {
        return scalable;
    }

    public void setScalable(boolean scalable) {
        this.scalable = scalable;
    }

    @Override
    public MachineDefinition getMachineDefinition() {
        return machineDefinition;
    }

    @JsonIgnore
    @Override
    public Collection<V2WorkerMetadata> getWorkerByIndexMetadataSet() {
        return workerByIndexMetadataSet.values();
    }

    @JsonIgnore
    @Override
    public Collection<V2WorkerMetadata> getAllWorkers() {
        return workerByNumberMetadataSet.values();
    }

    @JsonIgnore
    @Override
    public V2WorkerMetadata getWorkerByIndex(int workerId) throws InvalidJobException {
        V2WorkerMetadata mwmd = workerByIndexMetadataSet.get(workerId);
        if (mwmd == null) {
            throw new InvalidJobException(jobId, -1, workerId);
        }
        return mwmd;
    }

    @JsonIgnore
    @Override
    public V2WorkerMetadata getWorkerByWorkerNumber(int workerNumber) throws InvalidJobException {
        V2WorkerMetadata mwmd = workerByNumberMetadataSet.get(workerNumber);
        if (mwmd == null) {
            throw new InvalidJobException(jobId, -1, workerNumber);
        }
        return mwmd;
    }

    @Override
    public ServiceJobProcesses getJobProcesses() {
        return jobProcesses;
    }

    @Override
    public AutoCloseable obtainLock() {
        lock.lock();
        return new AutoCloseable() {
            @Override
            public void close() throws IllegalMonitorStateException {
                lock.unlock();
            }
        };
    }

    V2WorkerMetadataWritable removeWorkerInTerminalState(int workerNumber) {
        V2WorkerMetadataWritable mwmd = (V2WorkerMetadataWritable) workerByNumberMetadataSet.get(workerNumber);
        if (mwmd != null && V2JobState.isTerminalState(mwmd.getState())) {
            workerByNumberMetadataSet.remove(workerNumber);
            return mwmd;
        }
        return null;
    }

    public boolean replaceWorkerIndex(V2WorkerMetadata newWorker, V2WorkerMetadata oldWorker)
            throws InvalidJobException {
        int index = newWorker.getWorkerIndex();
        boolean result = true;
        try (AutoCloseable ac = obtainLock()) {
            if (!V2JobState.isErrorState(newWorker.getState())) {
                if (oldWorker == null) {
                    if (workerByIndexMetadataSet.putIfAbsent(index, newWorker) != null) {
                        result = false;
                    }
                } else {
                    if (oldWorker.getWorkerIndex() != index) {
                        throw new InvalidJobException(newWorker.getJobId(), stageNum, oldWorker.getWorkerIndex());
                    }
                    V2WorkerMetadata mwmd = workerByIndexMetadataSet.put(index, newWorker);
                    if (mwmd.getWorkerNumber() != oldWorker.getWorkerNumber()) {
                        workerByIndexMetadataSet.put(index, mwmd);
                        result = false;
                        logger.info("Did not replace worker " + oldWorker.getWorkerNumber() + " with " +
                                newWorker.getWorkerNumber() + " for index " + newWorker.getWorkerIndex() + " of job " +
                                jobId + ", different worker " + mwmd.getWorkerNumber() + " exists already");
                    }
//                    else
//                        logger.info("Replaced worker " + oldWorker.getWorkerNumber() + " with " + newWorker.getWorkerNumber() +
//                                " for index " + newWorker.getWorkerIndex() + " of job " + jobId);
                }
            } else if (oldWorker != null) {
                result = false;
            } else {
                // oldWorker == null, newWorker is error state
                // oldWorker is null while loading archived data
                if (workerByIndexMetadataSet.containsKey(index)) {
                    final V2WorkerMetadata existingWorkerMetadata = workerByIndexMetadataSet.get(index);
                    if (existingWorkerMetadata.getWorkerNumber() < newWorker.getWorkerNumber()) {
                        // remove existingWorkerMetadata from both maps
                        workerByIndexMetadataSet.put(index, newWorker);
                        workerByNumberMetadataSet.remove(existingWorkerMetadata.getWorkerNumber());
                    } else {
                        result = false;
                    }
                } else {
                    workerByIndexMetadataSet.put(index, newWorker);
                }
            }
            // This condition hits only during storage initialization phase.
            if (newWorker.getState() == V2JobState.Failed && newWorker.getReason() == JobCompletedReason.TombStone) {
                workerByIndexMetadataSet.put(index, newWorker);
            }
            if (result) {
                workerByNumberMetadataSet.put(newWorker.getWorkerNumber(), newWorker);
            }
            return result;
        } catch (Exception e) {
            // shouldn't happen
        }
        return false;
    }
}
