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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsClientConfiguration {
    @Value("${titus.es.host}")
    private String esHostName;

    @Value("${titus.es.port}")
    private int esPort;

    @Value("${titus.es.client.readTimeoutSeconds:20}")
    private int readTimeoutSeconds;

    @Value("${titus.es.client.connectTimeoutMillis:1000}")
    private int connectTimeoutMillis;

    public int getReadTimeoutSeconds() {
        return readTimeoutSeconds;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public String getEsHostName() {
        return esHostName;
    }

    public int getEsPort() {
        return esPort;
    }

}
