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

import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netflix.titus.api.audit.model.AuditLogEvent;
import io.netflix.titus.api.model.v2.JobSla;
import io.netflix.titus.api.model.v2.V2JobDurationType;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V2JobMetadataWritable implements V2JobMetadata {
    class Locker implements AutoCloseable {

        public Locker() {
            lock.lock();
        }

        @Override
        public void close() throws Exception {
            lock.unlock();
        }
    }

    public static class OldJobMetadataImpl {
        public enum SlaType {
            Lossy
        }

        private final String jobId;
        private final String name;
        private final long submittedAt;
        private final URL jarUrl;
        private final int numStages;
        private final SlaType slaType;
        private final V2JobDurationType durationType;
        private final String userProvidedType;
        private final List<Parameter> parameters;
        private final V2JobState state;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public OldJobMetadataImpl(@JsonProperty("jobId") String jobId, @JsonProperty("name") String name,
                                  @JsonProperty("submittedAt") long submittedAt,
                                  @JsonProperty("jarUrl") URL jarUrl,
                                  @JsonProperty("numStages") int numStages,
                                  @JsonProperty("slaType") SlaType slaType,
                                  @JsonProperty("durationType") V2JobDurationType durationType,
                                  @JsonProperty("userProvidedType") String userProvidedType,
                                  @JsonProperty("parameters") List<Parameter> parameters,
                                  @JsonProperty("state") V2JobState state) {
            this.jobId = jobId;
            this.name = name;
            this.submittedAt = submittedAt;
            this.jarUrl = jarUrl;
            this.numStages = numStages;
            this.slaType = slaType;
            this.durationType = durationType;
            this.userProvidedType = userProvidedType;
            this.parameters = parameters;
            this.state = state;
        }

        public String getJobId() {
            return jobId;
        }

        public String getName() {
            return name;
        }

        public long getSubmittedAt() {
            return submittedAt;
        }

        public URL getJarUrl() {
            return jarUrl;
        }

        public int getNumStages() {
            return numStages;
        }

        public SlaType getSlaType() {
            return slaType;
        }

        public V2JobDurationType getDurationType() {
            return durationType;
        }

        public String getUserProvidedType() {
            return userProvidedType;
        }

        public List<Parameter> getParameters() {
            return parameters;
        }

        public V2JobState getState() {
            return state;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(V2JobMetadataWritable.class);
    private String jobId;
    private String name;
    private final String user;
    private long submittedAt;
    private URL jarUrl;
    private final JobSla sla;
    private final long subscriptionTimeoutSecs;
    private volatile V2JobState state;
    private int numStages;
    private List<Parameter> parameters;
    private List<AuditLogEvent> latestAuditLogs;

    private int nextWorkerNumberToUse = 1;
    @JsonIgnore
    private final ConcurrentMap<Integer, V2StageMetadataWritable> stageMetadataMap;
    @JsonIgnore
    private final ConcurrentMap<Integer, Integer> workerNumberToStageMap;
    @JsonIgnore
    private Object sink; // ToDo need to figure out what object we store for sink
    @JsonIgnore
    private final ReentrantLock lock = new ReentrantLock();

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public V2JobMetadataWritable(@JsonProperty("jobId") String jobId,
                                 @JsonProperty("name") String name,
                                 @JsonProperty("user") String user,
                                 @JsonProperty("submittedAt") long submittedAt,
                                 @JsonProperty("jarUrl") URL jarUrl,
                                 @JsonProperty("numStages") int numStages,
                                 @JsonProperty("sla") JobSla sla,
                                 @JsonProperty("state") V2JobState state,
                                 @JsonProperty("subscriptionTimeoutSecs") long subscriptionTimeoutSecs,
                                 @JsonProperty("parameters") List<Parameter> parameters,
                                 @JsonProperty("nextWorkerNumberToUse") int nextWorkerNumberToUse) {
        this.jobId = jobId;
        this.name = name;
        this.user = user;
        this.submittedAt = submittedAt;
        this.jarUrl = jarUrl;
        this.numStages = numStages;
        this.sla = sla;
        this.state = state == null ? V2JobState.Accepted : state;
        this.subscriptionTimeoutSecs = subscriptionTimeoutSecs;
        this.stageMetadataMap = new ConcurrentHashMap<>();
        this.workerNumberToStageMap = new ConcurrentHashMap<>();
        if (parameters == null) {
            this.parameters = new LinkedList<Parameter>();
        } else {
            this.parameters = parameters;
        }
        this.nextWorkerNumberToUse = nextWorkerNumberToUse;
        this.latestAuditLogs = new LinkedList<>();
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

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getUser() {
        return user;
    }


    @Override
    public long getSubmittedAt() {
        return submittedAt;
    }

    @Override
    public URL getJarUrl() {
        return jarUrl;
    }

    @Override
    public List<AuditLogEvent> getLatestAuditLogEvents() {
        return latestAuditLogs;
    }

    @Override
    public JobSla getSla() {
        return sla;
    }

    @Override
    public List<Parameter> getParameters() {
        return parameters;
    }

    @Override
    public long getSubscriptionTimeoutSecs() {
        return subscriptionTimeoutSecs;
    }

    @Override
    public int getNextWorkerNumberToUse() {
        return nextWorkerNumberToUse;
    }

    public void setNextWorkerNumberToUse(int n) {
        this.nextWorkerNumberToUse = n;
    }

    public void setJobState(V2JobState state) throws InvalidJobStateChangeException {
        if (!this.state.isValidStateChgTo(state)) {
            throw new InvalidJobStateChangeException(jobId, this.state, state);
        }
        this.state = state;
    }

    @Override
    public V2JobState getState() {
        return state;
    }

    @JsonIgnore
    @Override
    public Collection<? extends V2StageMetadata> getStageMetadata() {
        return stageMetadataMap.values();
    }

    @Override
    public int getNumStages() {
        return numStages;
    }

    @JsonIgnore
    @Override
    public V2StageMetadata getStageMetadata(int stageNum) {
        return stageMetadataMap.get(stageNum);
    }

    /**
     * Add job stage if absent, returning true if it was actually added.
     *
     * @param msmd The stage's metadata object.
     * @return true if actually added, false otherwise.
     */
    public boolean addJobStageIfAbsent(V2StageMetadataWritable msmd) {
        return stageMetadataMap.putIfAbsent(msmd.getStageNum(), msmd) == null;
    }

    public boolean addWorkerMedata(int stageNum, V2WorkerMetadata workerMetadata, V2WorkerMetadata replacedWorker)
            throws InvalidJobException {
        boolean result = true;
        if (!stageMetadataMap.get(stageNum).replaceWorkerIndex(workerMetadata, replacedWorker)) {
            result = false;
        }
        Integer integer = workerNumberToStageMap.put(workerMetadata.getWorkerNumber(), stageNum);
        if (integer != null && integer != stageNum) {
            logger.error(String.format("Unexpected to put worker number mapping from %d to stage %d for job %s, prev mapping to stage %d",
                    workerMetadata.getWorkerNumber(), stageNum, workerMetadata.getJobId(), integer));
        }
        return result;
    }

    @JsonIgnore
    @Override
    public V2WorkerMetadata getWorkerByIndex(int stageNumber, int workerIndex) throws InvalidJobException {
        V2StageMetadata stage = stageMetadataMap.get(stageNumber);
        if (stage == null) {
            throw new InvalidJobException(jobId, stageNumber, workerIndex);
        }
        return stage.getWorkerByIndex(workerIndex);
    }

    @JsonIgnore
    @Override
    public V2WorkerMetadata getWorkerByNumber(int workerNumber) throws InvalidJobException {
        Integer stageNumber = workerNumberToStageMap.get(workerNumber);
        if (stageNumber == null) {
            throw new InvalidJobException(jobId, -1, workerNumber);
        }
        V2StageMetadata stage = stageMetadataMap.get(stageNumber);
        if (stage == null) {
            throw new InvalidJobException(jobId, stageNumber, workerNumber);
        }
        return stage.getWorkerByWorkerNumber(workerNumber);
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters == null ? new LinkedList<>() : parameters;
    }
}
