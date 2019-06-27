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

package com.netflix.titus.ext.eureka.common;

import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.titus.common.runtime.TitusRuntime;

public class EurekaLoadBalancer implements Closeable {

    private final EurekaClient eurekaClient;

    private final Function<URI, String> vipExtractor;

    private final TitusRuntime titusRuntime;

    private final ConcurrentMap<String, SingleServiceLoadBalancer> loadBalancers = new ConcurrentHashMap<>();

    public EurekaLoadBalancer(EurekaClient eurekaClient,
                              Function<URI, String> vipExtractor,
                              TitusRuntime titusRuntime) {
        this.eurekaClient = eurekaClient;
        this.vipExtractor = vipExtractor;
        this.titusRuntime = titusRuntime;
    }

    @Override
    public void close() {
        List<SingleServiceLoadBalancer> toClose = new ArrayList<>(loadBalancers.values());
        loadBalancers.clear();
        toClose.forEach(SingleServiceLoadBalancer::close);
    }

    public Optional<InstanceInfo> chooseNext(URI eurekaUri) {
        return loadBalancers.computeIfAbsent(
                toEndpointId(eurekaUri),
                uri -> newSingleServiceLoadBalancer(eurekaUri)
        ).chooseNext();
    }

    public void recordSuccess(URI eurekaUri, InstanceInfo instanceInfo) {
        SingleServiceLoadBalancer singleLB = loadBalancers.get(toEndpointId(eurekaUri));
        if (singleLB != null) {
            singleLB.recordSuccess(instanceInfo);
        }
    }

    public void recordFailure(URI eurekaUri, InstanceInfo instanceInfo) {
        SingleServiceLoadBalancer singleLB = loadBalancers.get(toEndpointId(eurekaUri));
        if (singleLB != null) {
            singleLB.recordFailure(instanceInfo);
        }
    }

    private String toEndpointId(URI eurekaUri) {
        return eurekaUri.getHost() + ':' + eurekaUri.getPort();
    }

    private SingleServiceLoadBalancer newSingleServiceLoadBalancer(URI eurekaUri) {
        return new SingleServiceLoadBalancer(eurekaClient, vipExtractor.apply(eurekaUri), EurekaUris.isSecure(eurekaUri), titusRuntime);
    }
}
