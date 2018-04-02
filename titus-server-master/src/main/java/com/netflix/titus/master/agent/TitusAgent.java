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

package com.netflix.titus.master.agent;

import java.util.Map;

/**
 * An entity containing information about a Titus agent.
 */
public class TitusAgent {

    private final String id;
    private final String ipAddress;
    private final Map<String, String> attributes;

    public TitusAgent(String id, String ipAddress, Map<String, String> attributes) {
        this.id = id;
        this.ipAddress = ipAddress;
        this.attributes = attributes;
    }

    public String getId() {
        return id;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TitusAgent that = (TitusAgent) o;

        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        if (ipAddress != null ? !ipAddress.equals(that.ipAddress) : that.ipAddress != null) {
            return false;
        }
        return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (ipAddress != null ? ipAddress.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TitusAgent{" +
                "id='" + id + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", attributes=" + attributes +
                '}';
    }
}
