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

package com.netflix.titus.client.clustermembership.grpc;

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.client.clustermembership.resolver.ClusterMemberResolver;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.IOExt;
import io.grpc.Attributes;
import io.grpc.NameResolver;

public class GrpcClusterMembershipNameResolverFactory extends NameResolver.Factory implements Closeable {

    private static final String SCHEME = "leader";

    private final GrpcClusterMembershipNameResolverConfiguration configuration;
    private final Function<URI, ClusterMemberResolver> clusterMemberResolverProvider;
    private final Function<ClusterMember, ClusterMemberAddress> addressSelector;

    private volatile boolean closed;
    private final ConcurrentMap<URI, Allocation> allocatedResolvers = new ConcurrentHashMap<>();

    public GrpcClusterMembershipNameResolverFactory(GrpcClusterMembershipNameResolverConfiguration configuration,
                                                    Function<URI, ClusterMemberResolver> clusterMemberResolverProvider,
                                                    Function<ClusterMember, ClusterMemberAddress> addressSelector) {
        this.configuration = configuration;
        this.clusterMemberResolverProvider = clusterMemberResolverProvider;
        this.addressSelector = addressSelector;
    }

    @Override
    public void close() {
        closed = true;
        allocatedResolvers.values().forEach(Allocation::close);
        allocatedResolvers.clear();
    }

    @Nullable
    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        Preconditions.checkState(!closed, "Name resolver already closed");

        return allocatedResolvers.computeIfAbsent(targetUri, uri -> {
            ClusterMemberResolver resolver = clusterMemberResolverProvider.apply(targetUri);
            return new Allocation(
                    resolver,
                    new GrpcClusterMembershipLeaderNameResolver(configuration, resolver, addressSelector)
            );
        }).getGrpcClusterMembershipLeaderNameResolver();
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    private static class Allocation implements Closeable {

        private final ClusterMemberResolver clusterMemberResolver;
        private final GrpcClusterMembershipLeaderNameResolver grpcClusterMembershipLeaderNameResolver;

        private Allocation(ClusterMemberResolver clusterMemberResolver,
                           GrpcClusterMembershipLeaderNameResolver grpcClusterMembershipLeaderNameResolver) {
            this.clusterMemberResolver = clusterMemberResolver;
            this.grpcClusterMembershipLeaderNameResolver = grpcClusterMembershipLeaderNameResolver;
        }

        private GrpcClusterMembershipLeaderNameResolver getGrpcClusterMembershipLeaderNameResolver() {
            return grpcClusterMembershipLeaderNameResolver;
        }

        @Override
        public void close() {
            ExceptionExt.silent(grpcClusterMembershipLeaderNameResolver::shutdown);
            IOExt.closeSilently(clusterMemberResolver);
        }
    }
}
