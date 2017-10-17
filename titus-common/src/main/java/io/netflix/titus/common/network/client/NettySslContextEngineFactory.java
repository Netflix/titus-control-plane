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

package io.netflix.titus.common.network.client;

import javax.net.ssl.SSLEngine;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.reactivex.netty.pipeline.ssl.SSLEngineFactory;

public class NettySslContextEngineFactory implements SSLEngineFactory {

    private final SslContext sslContext;

    public NettySslContextEngineFactory(SslContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    public SSLEngine createSSLEngine(ByteBufAllocator allocator) {
        return sslContext.newEngine(allocator);
    }
}
