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

package io.netflix.titus.testkit.embedded.cloud.endpoint.representation;

import java.util.List;

public class SimulatedAgentRepresentation {

    private final String id;
    private final String hostName;
    private final long launchTime;
    private final List<SimulatedContainerRepresentation> containers;

    public SimulatedAgentRepresentation(String id, String hostName, long launchTime, List<SimulatedContainerRepresentation> containers) {

        this.id = id;
        this.hostName = hostName;
        this.launchTime = launchTime;
        this.containers = containers;
    }

    public String getId() {
        return id;
    }

    public String getHostName() {
        return hostName;
    }

    public long getLaunchTime() {
        return launchTime;
    }

    public List<SimulatedContainerRepresentation> getContainers() {
        return containers;
    }
}
