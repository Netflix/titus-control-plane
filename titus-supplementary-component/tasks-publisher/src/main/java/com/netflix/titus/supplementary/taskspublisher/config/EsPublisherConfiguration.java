/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.titus.supplementary.taskspublisher.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsPublisherConfiguration {

    @Value("${netflix.environment}")
    private String env;

    @Value("${netflix.account}")
    private String account;

    @Value("${netflix.region}")
    private String region;

    @Value("${titus.es.taskDocumentEsIndexDateSuffixPattern}")
    private String taskDocumentEsIndexDateSuffixPattern;

    @Value("${titus.es.taskDocumentEsIndexName}")
    private String taskDocumentEsIndexName;

    @Value("${titus.es.publish.enabled}")
    private boolean enabled;

    public String getTaskDocumentEsIndexDateSuffixPattern() {
        return taskDocumentEsIndexDateSuffixPattern;
    }

    public String getTaskDocumentEsIndexName() {
        return taskDocumentEsIndexName;
    }

    public String getEnv() {
        return env;
    }

    public String getAccount() {
        return account;
    }

    public String getRegion() {
        return region;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
