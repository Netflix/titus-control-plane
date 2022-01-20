/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.kubernetes.okhttp;

import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Disables compression by adding the Http header.
 */
public class DisableCompressionInterceptor implements Interceptor {
    @Override
    public Response intercept(Chain chain) throws IOException {

        Request newRequest = chain.request()
                .newBuilder()
                .addHeader(HttpHeaders.ACCEPT_ENCODING, "identity")
                .build();

        return chain.proceed(newRequest);
    }
}
