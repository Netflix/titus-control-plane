/*
 * Copyright 2020 Netflix, Inc.
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
package com.netflix.titus.ext.elasticsearch;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.es")
public interface EsClientConfiguration {
    int DEFAULT_READ_TIMEOUT_SECONDS = 20;
    int DEFAULT_CONNECT_TIMEOUT_MILLIS = 1000;

    @DefaultValue("" + DEFAULT_READ_TIMEOUT_SECONDS)
    int getReadTimeoutSeconds();

    @DefaultValue("" + DEFAULT_CONNECT_TIMEOUT_MILLIS)
    int getConnectTimeoutMillis();

    String getHost();

    int getPort();
}
