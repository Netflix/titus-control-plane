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

package io.netflix.titus.gateway.connector.titusmaster;

import java.util.Collections;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.netflix.titus.common.network.http.HttpClient;
import io.netflix.titus.common.network.http.RxHttpClient;
import io.netflix.titus.common.network.http.internal.okhttp.CompositeRetryInterceptor;
import io.netflix.titus.common.network.http.internal.okhttp.EndpointResolverInterceptor;
import io.netflix.titus.common.network.http.internal.okhttp.OkHttpClient;
import io.netflix.titus.common.network.http.internal.okhttp.PassthroughInterceptor;
import io.netflix.titus.common.network.http.internal.okhttp.RxOkHttpClient;
import io.netflix.titus.gateway.connector.titusmaster.internal.ConfigurationLeaderResolver;
import io.netflix.titus.gateway.connector.titusmaster.internal.LeaderEndpointResolver;
import okhttp3.Interceptor;

public class TitusMasterConnectorModule extends AbstractModule {
    public static final String TITUS_MASTER_CLIENT = "TitusMaster";
    public static final String RX_TITUS_MASTER_CLIENT = "RxTitusMaster";

    private static final int NUMBER_OF_RETRIES = 3;
    private static final int DEFAULT_CONNECT_TIMEOUT = 10_000;
    private static final int DEFAULT_READ_TIMEOUT = 30_000;

    @Override
    protected void configure() {

        bind(LeaderResolver.class).to(ConfigurationLeaderResolver.class);

        install(new JobManagementClientModule());
        install(new AgentManagementClientModule());
        install(new AutoScalingClientModule());
        install(new LoadBalancerClientModule());
    }

    @Named(TITUS_MASTER_CLIENT)
    @Provides
    @Singleton
    public HttpClient httpClient() {
        OkHttpClient.Builder builder = OkHttpClient.newBuilder();
        Interceptor retryInterceptor = new CompositeRetryInterceptor(Collections.singletonList(new PassthroughInterceptor()), NUMBER_OF_RETRIES);
        builder.interceptor(retryInterceptor)
                .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
                .readTimeout(DEFAULT_READ_TIMEOUT);
        return builder.build();
    }

    @Named(RX_TITUS_MASTER_CLIENT)
    @Provides
    @Singleton
    public RxHttpClient rxHttpClient(LeaderResolver leaderResolver) {
        RxOkHttpClient.Builder builder = RxOkHttpClient.newBuilder();
        LeaderEndpointResolver endpointResolver = new LeaderEndpointResolver(leaderResolver);
        Interceptor retryInterceptor = new CompositeRetryInterceptor(Collections.singletonList(new EndpointResolverInterceptor(endpointResolver)), NUMBER_OF_RETRIES);
        builder.interceptor(retryInterceptor)
                .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
                .readTimeout(DEFAULT_READ_TIMEOUT);
        return builder.build();
    }
}
