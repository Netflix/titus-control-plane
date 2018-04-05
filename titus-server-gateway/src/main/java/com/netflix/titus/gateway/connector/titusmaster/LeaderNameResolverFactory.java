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

package com.netflix.titus.gateway.connector.titusmaster;

import java.net.URI;

import com.netflix.titus.common.runtime.TitusRuntime;
import io.grpc.Attributes;
import io.grpc.NameResolver;

public final class LeaderNameResolverFactory extends NameResolver.Factory {

    private static final String SCHEME = "leader";
    private final LeaderResolver leaderResolver;
    private final int port;
    private final TitusRuntime titusRuntime;

    public LeaderNameResolverFactory(LeaderResolver leaderResolver, int port, TitusRuntime titusRuntime) {
        this.leaderResolver = leaderResolver;
        this.port = port;
        this.titusRuntime = titusRuntime;
    }

    @Override
    public LeaderNameResolver newNameResolver(URI targetUri, Attributes params) {
        if (SCHEME.equals(targetUri.getScheme())) {
            return new LeaderNameResolver(targetUri, leaderResolver, port, titusRuntime);
        } else {
            return null;
        }
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }
}
