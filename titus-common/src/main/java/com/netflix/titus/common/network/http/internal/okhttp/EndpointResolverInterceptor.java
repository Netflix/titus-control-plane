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

package com.netflix.titus.common.network.http.internal.okhttp;

import java.io.IOException;

import com.netflix.titus.common.network.http.EndpointResolver;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

public class EndpointResolverInterceptor implements Interceptor {

    private static final String EMPTY_ENDPOINT = "http://<empty>";

    private final EndpointResolver endpointResolver;

    public EndpointResolverInterceptor(EndpointResolver endpointResolver) {
        this.endpointResolver = endpointResolver;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        if (request.url().toString().startsWith(EMPTY_ENDPOINT)) {
            String endpoint = endpointResolver.resolve();
            String newUrl = request.url().toString().replaceAll(EMPTY_ENDPOINT, endpoint);
            request = request.newBuilder().url(newUrl).build();
        }
        return chain.proceed(request);
    }
}
