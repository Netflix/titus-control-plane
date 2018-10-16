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

package com.netflix.titus.ext.zookeeper;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titusMaster.ext.zookeeper")
public interface ZookeeperConfiguration {

    @DefaultValue("localhost:2181")
    String getZkConnectionString();

    @DefaultValue("10000")
    int getZkConnectionTimeoutMs();

    @DefaultValue("500")
    int getZkConnectionRetrySleepMs();

    @DefaultValue("5")
    int getZkConnectionMaxRetries();

    @DefaultValue("/titus")
    String getZkRoot();
}
