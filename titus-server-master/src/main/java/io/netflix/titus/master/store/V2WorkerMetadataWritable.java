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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;

public class V2WorkerMetadataWritable implements V2WorkerMetadata {

    private int workerIndex;
    private int workerNumber;
    private String jobId;
    private int stageNum;
    private String workerInstanceId;
    private int numberOfPorts;
    private int metricsPort;
    private int debugPort = -1;
    private List<Integer> ports;
    private volatile V2JobState state;
    private String slave;
    private String slaveID;
    private List<TwoLevelResource> twoLevelResources = null;
    private Map<String, String> slaveAttributes;
    private long acceptedAt = 0;
    private long launchedAt = 0;
    private long startingAt = 0;
    private long startedAt = 0;
    private long completedAt = 0;
    private JobCompletedReason reason;
    private String completionMessage;
    private Object statusData = null;
    private int resubmitOf = -1;
    private int totalResubmitCount = 0;
    private long migrationDeadline = 0;

    @JsonIgnore
    private final ReentrantLock lock = new ReentrantLock();

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public V2WorkerMetadataWritable(@JsonProperty("workerIndex") int workerIndex,
                                    @JsonProperty("workerNumber") int workerNumber,
                                    @JsonProperty("jobId") String jobId,
                                    @JsonProperty("workerInstanceId") String workerIndexId,
                                    @JsonProperty("stageNum") int stageNum,
                                    @JsonProperty("numberOfPorts") int numberOfPorts) {
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.jobId = jobId;
        this.workerInstanceId = workerIndexId;
        this.stageNum = stageNum;
        this.numberOfPorts = numberOfPorts;
        this.state = V2JobState.Accepted;
        this.acceptedAt = System.currentTimeMillis();
        this.ports = new ArrayList<>();
        slaveAttributes = new HashMap<>();
    }

    @Override
    public int getWorkerIndex() {
        return workerIndex;
    }

    @Override
    public int getWorkerNumber() {
        return workerNumber;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public String getWorkerInstanceId() {
        return workerInstanceId;
    }

    @Override
    public int getStageNum() {
        return stageNum;
    }

    @Override
    public int getNumberOfPorts() {
        return numberOfPorts;
    }

    @Override
    public List<Integer> getPorts() {
        return ports;
    }

    @Override
    public void addPorts(List<Integer> ports) {
        this.ports.addAll(ports);
    }

    @Override
    public int getTotalResubmitCount() {
        return totalResubmitCount;
    }

    @Override
    public int getMetricsPort() {
        return metricsPort;
    }

    @Override
    public int getDebugPort() {
        return debugPort;
    }

    public void setMetricsPort(int metricsPort) {
        this.metricsPort = metricsPort;
    }

    public void setDebugPort(int debugPort) {
        this.debugPort = debugPort;
    }

    @Override
    public int getResubmitOf() {
        return resubmitOf;
    }

    @JsonIgnore
    public void setResubmitInfo(int resubmitOf, int totalCount) {
        this.resubmitOf = resubmitOf;
        this.totalResubmitCount = totalCount;
    }

    /**
     * Obtain a lock on this object, All operations on this object work without checking for concurrent updates. Callers
     * are expected call this method to lock this object for safe modifications and unlock after use. The return object
     * can be used in try with resources for reliable unlocking.
     *
     * @return {@link java.lang.AutoCloseable} lock object.
     */
    @Override
    public AutoCloseable obtainLock() {
        lock.lock();
        return lock::unlock;
    }

    private void validateStateChange(V2JobState newState) throws InvalidJobStateChangeException {
        if (!state.isValidStateChgTo(newState)) {
            throw new InvalidJobStateChangeException(jobId, state, newState);
        }
    }

    public void setState(V2JobState state, long when, JobCompletedReason reason) throws InvalidJobStateChangeException {
        validateStateChange(state);
        this.state = state;
        switch (state) {
            case Accepted:
                this.acceptedAt = when;
                break;
            case Launched:
                this.launchedAt = when;
                break;
            case StartInitiated:
                this.startingAt = when;
                break;
            case Started:
                this.startedAt = when;
                break;
            case Failed:
                this.completedAt = when;
                this.reason = reason == null ? JobCompletedReason.Lost : reason;
                break;
            case Completed:
                this.completedAt = when;
                this.reason = reason == null ? JobCompletedReason.Normal : reason;
                break;
            default:
                assert false : "Unexpected job state to set";
        }
    }

    @Override
    public V2JobState getState() {
        return state;
    }

    public void setSlave(String slave) {
        this.slave = slave;
    }

    @Override
    public String getSlave() {
        return slave;
    }

    public void setSlaveID(String slaveID) {
        this.slaveID = slaveID;
    }

    @Override
    public String getSlaveID() {
        return slaveID;
    }

    @Override
    public List<TwoLevelResource> getTwoLevelResources() {
        return twoLevelResources;
    }

    public void setTwoLevelResources(List<TwoLevelResource> resources) {
        this.twoLevelResources = resources;
    }

    public void setSlaveAttributes(Map<String, String> slaveAttributes) {
        this.slaveAttributes.clear();
        this.slaveAttributes.putAll(slaveAttributes);
    }

    @Override
    public Map<String, String> getSlaveAttributes() {
        return slaveAttributes;
    }

    @Override
    public long getAcceptedAt() {
        return acceptedAt;
    }

    public void setAcceptedAt(long when) {
        this.acceptedAt = when;
    }

    @Override
    public long getLaunchedAt() {
        return launchedAt;
    }

    public void setLaunchedAt(long when) {
        this.launchedAt = when;
    }

    @Override
    public long getStartingAt() {
        return startingAt;
    }

    public void setStartingAt(long when) {
        this.startingAt = when;
    }

    @Override
    public long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(long when) {
        this.startedAt = when;
    }

    @Override
    public long getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(long when) {
        this.completedAt = when;
    }

    public void setReason(JobCompletedReason reason) {
        this.reason = reason;
    }

    @Override
    public JobCompletedReason getReason() {
        return reason;
    }

    @Override
    public String getCompletionMessage() {
        return completionMessage;
    }

    @Override
    public Object getStatusData() {
        return statusData;
    }

    public void setMigrationDeadline(long migrationDeadline) {
        this.migrationDeadline = migrationDeadline;
    }

    @Override
    public long getMigrationDeadline() {
        return migrationDeadline;
    }

    public void setCompletionMessage(String completionMessage) {
        this.completionMessage = completionMessage;
    }

    public void setStatusData(Object data) {
        this.statusData = data;
    }

    @Override
    public String toString() {
        return "Worker " + workerNumber + " state=" + state + ", acceptedAt=" + acceptedAt +
                ((launchedAt == 0) ? "" : ", launchedAt=" + launchedAt) +
                ((startingAt == 0) ? "" : ", startingAt=" + startingAt) +
                ((startedAt == 0) ? "" : ", startedAt=" + startedAt) +
                ((completedAt == 0) ? "" : ", completedAt=" + completedAt) +
                ", #ports=" + ports.size() + ", ports=" + ports;
    }
}
