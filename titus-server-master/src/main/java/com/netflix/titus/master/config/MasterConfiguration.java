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

package com.netflix.titus.master.config;

import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;

public interface MasterConfiguration {

    @PropertyName(name = "titus.localmode")
    @DefaultValue("false")
    boolean isLocalMode();

    @PropertyName(name = "titus.master.cellName")
    @DefaultValue("dev")
    String getCellName();

    @PropertyName(name = "titus.master.apiProxyPort")
    @DefaultValue("7001")
    int getApiProxyPort();

    @PropertyName(name = "titus.master.api.status.path")
    @DefaultValue("/api/v2/jobs/heartbeat")
    String getApiStatusUri();

    @PropertyName(name = "titus.master.host")
    @DefaultValue("")
    String getMasterHost();

    @PropertyName(name = "titus.master.ip")
    @DefaultValue("")
    String getMasterIP();
}