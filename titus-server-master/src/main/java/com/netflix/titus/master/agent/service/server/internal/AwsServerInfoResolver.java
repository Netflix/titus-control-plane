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

package com.netflix.titus.master.agent.service.server.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.master.agent.service.server.ServerInfoResolver;
import com.netflix.titus.common.aws.AwsInstanceDescriptor;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.master.agent.ServerInfo;
import com.netflix.titus.master.agent.service.server.ServerInfoResolver;

/**
 * {@link ServerInfoResolver} implementation resolving AWS instance types.
 */
public final class AwsServerInfoResolver implements ServerInfoResolver {

    public static final String EBS_BANDWIDTH_MBS = "ebsBandwidthMbs";

    private static final ServerInfoResolver INSTANCE = new AwsServerInfoResolver();

    private static final Map<String, ServerInfo> AWS_SERVER_INFOS = load();

    private static final List<ServerInfo> ALL_SERVER_INFOS = Collections.unmodifiableList(new ArrayList<>(AWS_SERVER_INFOS.values()));

    private AwsServerInfoResolver() {
    }

    @Override
    public Optional<ServerInfo> resolve(String serverType) {
        ServerInfo serverInfo = AWS_SERVER_INFOS.get(serverType);
        if (serverInfo != null) {
            return Optional.of(serverInfo);
        }
        return Optional.ofNullable(AWS_SERVER_INFOS.get(serverType.toUpperCase()));
    }

    @Override
    public List<ServerInfo> resolveAll() {
        return ALL_SERVER_INFOS;
    }

    private static Map<String, ServerInfo> load() {
        Map<String, ServerInfo> result = new HashMap<>();
        for (AwsInstanceType type : AwsInstanceType.values()) {
            AwsInstanceDescriptor descriptor = type.getDescriptor();
            String serverType = descriptor.getId();
            ServerInfo serverInfo = ServerInfo.newBuilder()
                    .withServerType(serverType)
                    .withCpus(descriptor.getvCPUs())
                    .withGpus(descriptor.getvGPUs())
                    .withMemoryGB(descriptor.getMemoryGB())
                    .withStorageGB(descriptor.getStorageGB())
                    .withNetworkMbs(descriptor.getNetworkMbs())
                    .withAttribute(EBS_BANDWIDTH_MBS, Integer.toString(descriptor.getEbsBandwidthMbs()))
                    .build();
            result.put(type.name().toUpperCase(), serverInfo);
            result.put(serverType, serverInfo);
        }
        return result;
    }

    public static ServerInfoResolver getInstance() {
        return INSTANCE;
    }
}
