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
package com.netflix.titus.es.publish;

import com.netflix.titus.es.publish.config.EsPublisherConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TitusClientUtils {
    private static final Logger logger = LoggerFactory.getLogger(TitusClientUtils.class);
    private EsPublisherConfiguration publisherConfiguration;

    @Autowired
    public TitusClientUtils(EsPublisherConfiguration esPublisherConfiguration) {
        this.publisherConfiguration = esPublisherConfiguration;
    }

    public String buildTitusApiHost() {
        String account = "streamingtest";
        if (publisherConfiguration.getAccount().equalsIgnoreCase("prod")) {
            account = "streamingprod";
        } else if (publisherConfiguration.getAccount().startsWith("mce")) {
            account = publisherConfiguration.getAccount();
        }

        final String hostName = String.format("api.%s.%s.titus.netflix.net", publisherConfiguration.getRegion(), account);
        logger.debug("Titus API HOST :: {}", hostName);
        return hostName;
    }

    public String buildTitusApiVip() {
        String stackId = "main01";
        String envId = "test";

        if (publisherConfiguration.getAccount().startsWith("mce")) {
            stackId = "mainmce01";
        }

        if (publisherConfiguration.getAccount().contains("prod")) {
            envId = "prod";
        }
        final String vipId = String.format("titusapigrpc-%s-%s", envId, stackId);
        logger.debug("Titus API VIP :: {}", vipId);
        return vipId;
    }


}
