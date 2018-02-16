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

package io.netflix.titus.master;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.master.mesos.ContainerEvent;

public class Status implements ContainerEvent {

    public static class Payload {
        private final String type;
        private final String data;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public Payload(@JsonProperty("type") String type, @JsonProperty("data") String data) {
            this.type = type;
            this.data = data;
        }

        public String getType() {
            return type;
        }

        public String getData() {
            return data;
        }
    }

    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String getDataStringFromObj(Object statusObj) {
        try {
            return mapper.writeValueAsString(statusObj);
        } catch (JsonProcessingException e) {
            return statusObj == null ? null : statusObj.toString();
        }
    }

    public enum TYPE {
        ERROR, WARN, INFO, DEBUG
    }

    private String jobId;
    private final String taskId;
    private int stageNum;
    private int workerIndex;
    private int workerNumber;
    private String hostname;
    private String instanceId;
    private TYPE type;
    private String message;
    private String data;
    private long timestamp;
    private V2JobState state;
    private JobCompletedReason reason = JobCompletedReason.Normal;
    private List<Payload> payloads;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public Status(@JsonProperty("jobId") String jobId,
                  @JsonProperty("taskId") String taskId,
                  @JsonProperty("stageNum") int stageNum,
                  @JsonProperty("workerIndex") int workerIndex,
                  @JsonProperty("workerNumber") int workerNumber,
                  @JsonProperty("type") TYPE type,
                  @JsonProperty("message") String message,
                  @JsonProperty("data") String data,
                  @JsonProperty("state") V2JobState state) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.stageNum = stageNum;
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.type = type;
        this.message = message;
        this.data = data;
        this.state = state;
        timestamp = System.currentTimeMillis();
        this.payloads = new ArrayList<>();
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getStageNum() {
        return stageNum;
    }

    public void setStageNum(int stageNum) {
        this.stageNum = stageNum;
    }

    public void setWorkerIndex(int workerIndex) {
        this.workerIndex = workerIndex;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public int getWorkerNumber() {
        return workerNumber;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public TYPE getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    public String getData() {
        return data;
    }

    public V2JobState getState() {
        return state;
    }

    public JobCompletedReason getReason() {
        return reason;
    }

    public void setReason(JobCompletedReason reason) {
        this.reason = reason;
    }

    public List<Payload> getPayloads() {
        return payloads;
    }

    public void setPayloads(List<Payload> payloads) {
        this.payloads = payloads;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "Error getting string for status on job " + jobId;
        }
    }
}
