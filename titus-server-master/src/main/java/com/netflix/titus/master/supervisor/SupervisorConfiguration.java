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

package com.netflix.titus.master.supervisor;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.supervisor")
public interface SupervisorConfiguration {

    @DefaultValue("TitusMasterInstanceId")
    String getTitusMasterInstanceId();

    /**
     * @return whether or not the instance is forced to join into the leader election process instead of going through the
     * lifecycle states. This property is useful in case the system used to resolve the other instances is not functioning.
     */
    @DefaultValue("false")
    boolean isForceLeaderElectionEnabled();
}