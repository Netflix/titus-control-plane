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

package com.netflix.titus.api.store.v2;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.titus.api.model.v2.JobCompletedReason;
import com.netflix.titus.api.model.v2.V2JobState;

/**
 * Metadata object for a Titus worker. Modification operations do not perform locking. Instead, a lock can be
 * obtained via the <code>obtainLock()</code> method which is an instance of {@link java.lang.AutoCloseable}.
 */
public interface V2WorkerMetadata {

    class TwoLevelResource {
        private final String attrName;
        private final String resName;
        private final String label;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public TwoLevelResource(
                @JsonProperty("attrName") String attrName,
                @JsonProperty("resName") String resName,
                @JsonProperty("label") String label) {
            this.attrName = attrName == null ? "ENIs" : attrName;
            this.resName = resName;
            this.label = label;
        }

        public String getAttrName() {
            return attrName;
        }

        public String getResName() {
            return resName;
        }

        public String getLabel() {
            return label;
        }
    }

    int getWorkerIndex();

    int getWorkerNumber();

    String getJobId();

    String getWorkerInstanceId();

    int getStageNum();

    int getMetricsPort();

    int getDebugPort();

    /**
     * Get number of ports for this worker, including the metrics port
     *
     * @return The number of ports
     */
    int getNumberOfPorts();

    List<Integer> getPorts();

    void addPorts(List<Integer> ports);

    int getTotalResubmitCount();

    /**
     * Get the worker number (not index) of which this is a resubmission of.
     *
     * @return
     */
    int getResubmitOf();

    /**
     * Obtain a lock on this object, All operations on this object work without checking for concurrent updates. Callers
     * are expected call this method to lock this object for safe modifications and unlock after use. The return object
     * can be used in try with resources for reliable unlocking.
     *
     * @return {@link }java.lang.AutoCloseable} lock object.
     */
    AutoCloseable obtainLock();

    V2JobState getState();

    String getCell();

    String getSlave();

    String getSlaveID();

    List<TwoLevelResource> getTwoLevelResources();

    Map<String, String> getSlaveAttributes();

    long getAcceptedAt();

    long getLaunchedAt();

    long getStartingAt();

    long getStartedAt();

    long getCompletedAt();

    JobCompletedReason getReason();

    String getCompletionMessage();

    Object getStatusData();

    long getMigrationDeadline();

}
