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

package com.netflix.titus.federation.startup;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.federation")
public interface TitusFederationConfiguration {
    @DefaultValue("federation=hostname:7501")
    String getFederationEndpoint();

    @DefaultValue("cell1=hostName1:7001;cell2=hostName2:7002")
    String getCells();

    @DefaultValue("dev")
    String getStack();

    @DefaultValue("cell1=(app1.*|app2.*);cell2=(.*)")
    String getRoutingRules();

    @DefaultValue("cell1=(gpu.*)")
    String getInstanceTypeRoutingRules();
}
