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

package com.netflix.titus.ext.eureka.reverseproxy.http;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.titus.common.network.reverseproxy.http.ReactorHttpClientFactory;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.ext.eureka.reverseproxy.EurekaReverseProxyConfiguration;
import reactor.ipc.netty.http.client.HttpClient;

/**
 * FIXME {@link HttpClient} accepts single address only. We need a wrapper that would work with Eureka based name resolver.
 */
@Singleton
public class EurekaReactorHttpClientFactory implements ReactorHttpClientFactory {

    private final EurekaClient eurekaClient;
    private final EurekaReverseProxyConfiguration configuration;
    private final ConcurrentMap<String, HttpClient> clients = new ConcurrentHashMap<>();

    @Inject
    public EurekaReactorHttpClientFactory(EurekaClient eurekaClient, EurekaReverseProxyConfiguration configuration) {
        this.eurekaClient = eurekaClient;
        this.configuration = configuration;
    }

    @Override
    public Optional<HttpClient> newHttpClient(String path) {
        if (path == null) {
            return Optional.empty();
        }

        Optional<HttpClient> clientOpt = find(path);
        if (clientOpt.isPresent()) {
            return clientOpt;
        }

        return resolve(path);
    }

    private Optional<HttpClient> find(String path) {
        for (Map.Entry<String, HttpClient> entry : clients.entrySet()) {
            if (path.contains(entry.getKey())) {
                return Optional.of(entry.getValue());
            }
        }
        return Optional.empty();
    }

    private Optional<HttpClient> resolve(String path) {
        // Refresh all
        configuration.getServiceWithVipAddresses().forEach((serviceName, address) -> {
            HttpClient current = clients.get(serviceName);
            if (current == null) {
                List<InstanceInfo> instances = eurekaClient.getInstancesByVipAddress(address.getVipAddress(), address.isSecure());
                if (!CollectionsExt.isNullOrEmpty(instances)) {
                    InstanceInfo target = instances.get(0);
                    HttpClient client = HttpClient.create(target.getIPAddr(), address.isSecure() ? target.getSecurePort() : target.getPort());
                    clients.putIfAbsent(serviceName, client);
                }
            }
        });

        return find(path);
    }
}
