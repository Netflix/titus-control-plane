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

package com.netflix.titus.master.agent.service.server;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.common.util.PropertiesExt;
import com.netflix.titus.master.agent.ServerInfo;
import com.netflix.titus.master.agent.service.server.internal.AwsServerInfoResolver;
import com.netflix.titus.master.agent.service.server.internal.PropertyServerInfoResolver;
import com.netflix.titus.master.agent.service.server.internal.ServerInfoResolverAdapter;

/**
 * A collection of predefined {@link ServerInfoResolver} implementations.
 */
public final class ServerInfoResolvers {

    private ServerInfoResolvers() {
    }

    /**
     * Returns {@link ServerInfo} representation of AWS instance types. The AWS instance type name should be given
     * in the canonical form, for example 'm4.4xlarge'.
     */
    public static ServerInfoResolver fromAwsInstanceTypes() {
        return AwsServerInfoResolver.getInstance();
    }

    /**
     * {@link ServerInfoResolver} implementation that reads {@link ServerInfo} data from property collection.
     * The file should be in the following format: &lt;serverType&gt;.&lt;resource_name&gt;=&lt;dimension&gt;.
     * Server name may consists of multiple parts, like in the example below. The number of parts is configurable
     * via 'rootPart' parameter.
     * For example:
     * <ul>
     * <li>m4.4xlarge.cpus=16</li>
     * <li>m4.4xlarge.memoryGB=64</li>
     * <li>m4.4xlarge.storageGB=512</li>
     * <li>m4.4xlarge.networkMbs=2000</li>
     * <li>m4.4xlarge.attributes.ebsBandwidthMbs=2000</li>
     * </ul>
     */
    public static ServerInfoResolver fromProperties(Map<String, String> properties, int rootParts) {
        return new PropertyServerInfoResolver(properties, rootParts);
    }

    /**
     * A property {@link ServerInfoResolver} (see {@link #fromProperties(Map, int)}), that loads property values from
     * the classpath resource.
     *
     * @return empty optional if resource with the given name is not found on classpath
     */
    public static Optional<ServerInfoResolver> fromResource(String resourceName, int rootParts) throws IOException {
        Optional<Map<String, String>> properties = PropertiesExt.loadFromClassPath(resourceName);
        return properties.isPresent()
                ? Optional.of(fromProperties(properties.get(), rootParts))
                : Optional.empty();
    }

    /**
     * {@link ServerInfoResolver} implementation that converts the original {@link ServerInfo} values with
     * the user provided function. It assumes the server data for a particular type never change, so the result is
     * cached permanently.
     */
    public static ServerInfoResolver adapt(ServerInfoResolver original, Function<ServerInfo, ServerInfo> adapter) {
        return new ServerInfoResolverAdapter(original, adapter);
    }

    /**
     * A convenience {@link ServerInfoResolver} adapter implementation (see {@link #adapt(ServerInfoResolver, Function)}),
     * that modifies only CPU, memory and disk resources.
     */
    public static ServerInfoResolver adapt(ServerInfoResolver original,
                                           Function<Integer, Integer> cpuAdapter,
                                           Function<Integer, Integer> memoryAdapter,
                                           Function<Integer, Integer> diskAdapter) {
        return adapt(original, serverInfo ->
                new ServerInfo(
                        serverInfo.getServerType(),
                        cpuAdapter.apply(serverInfo.getCpus()),
                        serverInfo.getGpus(),
                        memoryAdapter.apply(serverInfo.getMemoryGB()),
                        diskAdapter.apply(serverInfo.getStorageGB()),
                        serverInfo.getNetworkMbs(),
                        serverInfo.getAttributes()
                ));
    }
}
