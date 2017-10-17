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

package io.netflix.titus.gateway.connector.titusmaster.internal;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.gateway.connector.titusmaster.Address;
import io.netflix.titus.gateway.connector.titusmaster.LeaderResolver;
import io.netflix.titus.gateway.startup.TitusGatewayConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ConfigurationLeaderResolver implements LeaderResolver {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationLeaderResolver.class);

    private volatile Address leaderAddress;

    @Inject
    public ConfigurationLeaderResolver(TitusGatewayConfiguration configuration) {
        leaderAddress = new Address(configuration.getMasterScheme(), configuration.getMasterIp(), configuration.getMasterHttpPort());
        logger.info("Created address: {}", leaderAddress);
    }

    public Optional<Address> resolve() {
        return Optional.of(leaderAddress);
    }
}
