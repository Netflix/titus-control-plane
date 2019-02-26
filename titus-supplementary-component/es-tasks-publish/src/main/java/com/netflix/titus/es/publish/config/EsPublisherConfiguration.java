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
package com.netflix.titus.es.publish.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class EsPublisherConfiguration {
    private String env;
    private String account;
    private String region;

    private final String getTaskDocumentEsHostName;
    private final int getTaskDocumentEsPort;
    private String taskDocumentEsIndexDateSuffixPattern;
    private String taskDocumentEsIndexName;
    private boolean isLocalMode;

    @Autowired
    public EsPublisherConfiguration(
            @Value("#{ @environment['EC2_REGION'] }") String region,
            @Value("#{ @environment['NETFLIX_ENVIRONMENT'] }") String env,
            @Value("#{ @environment['NETFLIX_ACCOUNT'] }") String account,
            @Value("#{ @environment['titus.es.host'] }") String getTaskDocumentEsHostName,
            @Value("#{ @environment['titus.es.port'] }") int getTaskDocumentEsPort,
            @Value("#{ @environment['titus.es.taskDocumentEsIndexDateSuffixPattern'] ?: 'yyyyMM'}") String esIndexDateSuffixPattern,
            @Value("#{ @environment['titus.es.taskDocumentEsIndexName'] ?: 'titustasks_'}") String esIndexName,
            @Value("#{ @environment['titus.es.localMode'] ?: false}") boolean isLocalMode) {
        this.region = region;
        this.env = env;
        this.account = account;
        this.getTaskDocumentEsHostName = getTaskDocumentEsHostName;
        this.getTaskDocumentEsPort = getTaskDocumentEsPort;
        this.taskDocumentEsIndexDateSuffixPattern = esIndexDateSuffixPattern;
        this.taskDocumentEsIndexName = esIndexName;
        this.isLocalMode = isLocalMode;
    }

    public boolean isLocalMode() {
        return isLocalMode;
    }

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

    public String getGetTaskDocumentEsHostName() {
        return getTaskDocumentEsHostName;
    }

    public int getGetTaskDocumentEsPort() {
        return getTaskDocumentEsPort;
    }
}
