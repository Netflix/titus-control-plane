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

package io.netflix.titus.api.model.v2;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class WorkerHost {

    private V2JobState state;
    private int workerNumber;
    private int workerIndex;
    private String host;
    private List<Integer> port;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public WorkerHost(@JsonProperty("host") String host, @JsonProperty("workerIndex") int workerIndex,
                      @JsonProperty("port") List<Integer> port, @JsonProperty("state") V2JobState state,
                      @JsonProperty("workerNumber") int workerNumber) {
        this.host = host;
        this.workerIndex = workerIndex;
        this.port = port;
        this.state = state;
        this.workerNumber = workerNumber;
    }

    public int getWorkerNumber() {
        return workerNumber;
    }

    public V2JobState getState() {
        return state;
    }

    public String getHost() {
        return host;
    }

    public List<Integer> getPort() {
        return port;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    @Override
    public String toString() {
        return "WorkerHost [state=" + state + ", workerIndex=" + workerIndex
                + ", host=" + host + ", port=" + port + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        for (int p : port) {
            result = prime * result + p;
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        WorkerHost other = (WorkerHost) obj;
        if (host == null) {
            if (other.host != null) {
                return false;
            }
        } else if (!host.equals(other.host)) {
            return false;
        }
        if (port == null) {
            if (other.port != null) {
                return false;
            }
        } else {
            if (other.port == null) {
                return false;
            }
            if (port.size() != other.port.size()) {
                return false;
            }
            for (int p = 0; p < port.size(); p++) {
                if (port.get(p) != other.port.get(p)) {
                    return false;
                }
            }
        }
        return true;
    }
}
