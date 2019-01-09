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

package com.netflix.titus.api.connector.cloud;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class LoadBalancer {
    public enum State {
        ACTIVE,
        REMOVED
    }

    private final String id;
    private final State state;
    private final Set<String> registeredIps;

    public LoadBalancer(String id, State state, Set<String> registeredIps) {
        this.id = id;
        this.state = state;
        this.registeredIps = Collections.unmodifiableSet(registeredIps);
    }

    public String getId() {
        return id;
    }

    public State getState() {
        return state;
    }

    public Set<String> getRegisteredIps() {
        return registeredIps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LoadBalancer)) {
            return false;
        }
        LoadBalancer that = (LoadBalancer) o;
        return id.equals(that.id) &&
                state == that.state &&
                registeredIps.equals(that.registeredIps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, state, registeredIps);
    }

    @Override
    public String toString() {
        return "LoadBalancer{" +
                "id='" + id + '\'' +
                ", state=" + state +
                ", registeredIps=" + registeredIps +
                '}';
    }
}
