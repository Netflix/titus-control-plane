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

package com.netflix.titus.master.mesos.resolver;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.mesos.MesosMasterResolver;

/**
 * Default implementation of {@link MesosMasterResolver} that reads address information from configuration file.
 * The Mesos address should be in one of the supported forms, either a comma separated list of host/port pairs
 * (mesos1:5050,mesos2:5050) or Zookeepr URI. For the former case, the value is parsed, and all addresses are
 * return by {@link #resolveMesosAddresses()}. Call to {@link #resolveLeader()} always returns empty reply.
 */
@Singleton
public class DefaultMesosMasterResolver implements MesosMasterResolver {

    private static final int DEFAULT_MESOS_PORT = 5050;

    private final String masterAddress;
    private final List<InetSocketAddress> mesosAddresses;

    @Inject
    public DefaultMesosMasterResolver(MasterConfiguration config) {
        this.masterAddress = config.getMasterLocation();
        this.mesosAddresses = parseMesosAddress(masterAddress);
    }

    @Override
    public Optional<String> resolveCanonical() {
        return Optional.of(masterAddress);
    }

    @Override
    public Optional<InetSocketAddress> resolveLeader() {
        return Optional.empty();
    }

    @Override
    public List<InetSocketAddress> resolveMesosAddresses() {
        return mesosAddresses;
    }

    private List<InetSocketAddress> parseMesosAddress(String mesosAddress) {
        if (mesosAddress.startsWith("zk://")) {
            return Collections.emptyList();
        }
        List<String> hostPortPairs = StringExt.splitByComma(mesosAddress);
        List<InetSocketAddress> result = new ArrayList<>(hostPortPairs.size());
        for (String hostPortPair : hostPortPairs) {
            int idx = hostPortPair.indexOf(':');
            if (idx == -1) {
                result.add(new InetSocketAddress(hostPortPair, DEFAULT_MESOS_PORT));
            } else {
                String host = hostPortPair.substring(0, idx);
                int port;
                try {
                    port = Integer.parseInt(hostPortPair.substring(idx + 1));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid port number in the configured Mesos address " + mesosAddress);
                }
                result.add(new InetSocketAddress(host, port));
            }
        }
        return result;
    }
}
