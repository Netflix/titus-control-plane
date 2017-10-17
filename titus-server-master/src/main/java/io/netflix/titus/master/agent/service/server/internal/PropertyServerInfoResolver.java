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

package io.netflix.titus.master.agent.service.server.internal;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.netflix.titus.common.util.PropertiesExt;
import io.netflix.titus.master.agent.ServerInfo;
import io.netflix.titus.master.agent.service.server.ServerInfoResolver;
import io.netflix.titus.master.agent.service.server.ServerInfoResolvers;

import static io.netflix.titus.common.util.CollectionsExt.asSet;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Documentation in {@link ServerInfoResolvers#fromProperties(Map, int)}.
 */
public class PropertyServerInfoResolver implements ServerInfoResolver {

    public static final String CPUS = "cpus";
    public static final String GPUS = "gpus";
    public static final String MEMORY_GB = "memoryGB";
    public static final String STORAGE_GB = "storageGB";
    public static final String NETWORK_MBS = "networkMbs";
    public static final String ATTRIBUTES = "attributes";

    private static final Set<String> PROPERTY_NAMES = asSet(CPUS, GPUS, MEMORY_GB, STORAGE_GB, NETWORK_MBS, ATTRIBUTES);

    private final Map<String, ServerInfo> serverInfoByName;
    private final List<ServerInfo> allServerInfos;

    public PropertyServerInfoResolver(Map<String, String> properties, int rootParts) {
        this.allServerInfos = unmodifiableList(
                PropertiesExt
                        .groupByRootName(properties, rootParts).entrySet()
                        .stream()
                        .map(serverEntry -> buildServerInfo(serverEntry.getKey(), serverEntry.getValue()))
                        .collect(Collectors.toList())
        );
        this.serverInfoByName = unmodifiableMap(
                allServerInfos.stream().collect(Collectors.toMap(ServerInfo::getServerType, Function.identity()))
        );
    }

    @Override
    public Optional<ServerInfo> resolve(String serverType) {
        return Optional.ofNullable(serverInfoByName.get(serverType));
    }

    @Override
    public List<ServerInfo> resolveAll() {
        return allServerInfos;
    }

    private ServerInfo buildServerInfo(String serverType, Map<String, String> serverProps) {
        List<String> badNames = PropertiesExt.verifyRootNames(serverProps, PROPERTY_NAMES);
        if (!badNames.isEmpty()) {
            throw new IllegalArgumentException("Unrecognized property values for server type " + serverType + ": " + badNames);
        }

        return ServerInfo.newBuilder()
                .withServerType(serverType)
                .withCpus(getInt(serverProps, CPUS, 0))
                .withGpus(getInt(serverProps, GPUS, 0))
                .withMemoryGB(getInt(serverProps, MEMORY_GB, 0))
                .withStorageGB(getInt(serverProps, STORAGE_GB, 0))
                .withNetworkMbs(getInt(serverProps, NETWORK_MBS, 0))
                .withAttributes(PropertiesExt.getPropertiesOf(ATTRIBUTES, serverProps))
                .build();
    }

    private int getInt(Map<String, String> serverProps, String key, int defaultValue) {
        String value = serverProps.get(key);
        if (value == null) {
            return defaultValue;
        }
        value = value.trim();
        if (value.isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }
}
