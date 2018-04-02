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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Entity representing agent server group.
 */
@Deprecated
public class TitusAgentServerGroup {

    public enum LifecycleState {
        Active,
        Inactive,
        Removable,
        Unknown
    }

    private final String id;
    private final String name;
    private final LifecycleState lifecycleState;
    private final Map<String, String> tags;
    private final Optional<List<TitusAgent>> agents;

    public TitusAgentServerGroup(String id,
                                 String name,
                                 LifecycleState lifecycleState,
                                 Map<String, String> tags,
                                 Optional<List<TitusAgent>> agents) {
        this.id = id;
        this.name = name;
        this.lifecycleState = lifecycleState;
        this.tags = tags;
        this.agents = agents;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Optional<List<TitusAgent>> getAgents() {
        return agents;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TitusAgentServerGroup that = (TitusAgentServerGroup) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (lifecycleState != that.lifecycleState) {
            return false;
        }
        if (tags != null ? !tags.equals(that.tags) : that.tags != null) {
            return false;
        }
        return agents != null ? agents.equals(that.agents) : that.agents == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (lifecycleState != null ? lifecycleState.hashCode() : 0);
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        result = 31 * result + (agents != null ? agents.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TitusAgentServerGroup{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", lifecycleState=" + lifecycleState +
                ", tags=" + tags +
                ", agents=" + agents +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(TitusAgentServerGroup serverGroup) {
        return new Builder()
                .withName(serverGroup.getName())
                .withLifecycleState(serverGroup.getLifecycleState())
                .withTags(serverGroup.getTags())
                .withAgents(serverGroup.getAgents());
    }

    public Builder but() {
        return newBuilder().withName(name).withLifecycleState(lifecycleState).withTags(tags).withAgents(agents);
    }

    public static final class Builder {
        private String id;
        private String name;
        private LifecycleState lifecycleState;
        private Map<String, String> tags;
        private Optional<List<TitusAgent>> agents;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withLifecycleState(LifecycleState lifecycleState) {
            this.lifecycleState = lifecycleState;
            return this;
        }

        public Builder withTags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        public Builder withAgents(Optional<List<TitusAgent>> agents) {
            this.agents = agents;
            return this;
        }

        public TitusAgentServerGroup build() {
            TitusAgentServerGroup titusAgentServerGroup = new TitusAgentServerGroup(
                    id,
                    name,
                    lifecycleState,
                    tags == null ? Collections.emptyMap() : tags,
                    agents == null ? Optional.empty() : agents
            );
            return titusAgentServerGroup;
        }
    }
}
