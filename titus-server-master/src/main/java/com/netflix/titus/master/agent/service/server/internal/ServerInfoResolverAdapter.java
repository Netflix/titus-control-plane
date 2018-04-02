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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.master.agent.service.server.ServerInfoResolvers;
import com.netflix.titus.master.agent.ServerInfo;
import com.netflix.titus.master.agent.service.server.ServerInfoResolver;
import com.netflix.titus.master.agent.service.server.ServerInfoResolvers;

import static java.util.Collections.unmodifiableMap;

/**
 * Documentation in {@link ServerInfoResolvers#adapt(ServerInfoResolver, Function)}.
 */
public class ServerInfoResolverAdapter implements ServerInfoResolver {

    private final List<ServerInfo> allServerInfos;
    private final Map<String, ServerInfo> serverInfoByName;

    public ServerInfoResolverAdapter(ServerInfoResolver delegate, Function<ServerInfo, ServerInfo> adapter) {
        this.allServerInfos = Collections.unmodifiableList(
                delegate.resolveAll().stream().map(adapter).collect(Collectors.toList())
        );
        this.serverInfoByName = unmodifiableMap(
                allServerInfos.stream().distinct().collect(Collectors.toMap(ServerInfo::getServerType, Function.identity()))
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
}
