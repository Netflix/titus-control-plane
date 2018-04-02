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
import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.network.http.Methods;
import com.netflix.titus.common.network.http.StatusCode;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

public class CompositeRetryInterceptor implements Interceptor {

    private final List<Interceptor> interceptors;
    private final int attempts;

    public CompositeRetryInterceptor(List<Interceptor> interceptors, int numberOfRetries) {
        Preconditions.checkNotNull(interceptors);
        Preconditions.checkArgument(interceptors.size() > 0, "There must be at least 1 intercepor");

        this.interceptors = interceptors;
        this.attempts = numberOfRetries + 1;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        Response response = null;
        boolean shouldRetry = true;
        int maxAttempts = Methods.isBodyAllowed(request.method()) ? 1 : this.attempts;
        int attempts = 0;

        while (shouldRetry) {
            boolean lastAttempt = attempts >= maxAttempts - 1;
            try {
                for (Interceptor interceptor : interceptors) {
                    response = interceptor.intercept(chain);
                }
                if (response != null) {
                    StatusCode statusCode = StatusCode.fromCode(response.code());
                    shouldRetry = statusCode != null && statusCode.isRetryable();
                }
            } catch (Exception e) {
                if (lastAttempt) {
                    throw e;
                }
            } finally {
                attempts++;
                shouldRetry = shouldRetry && !lastAttempt;
                if (response != null && shouldRetry) {
                    response.close();
                }
            }
        }
        return response;
    }
}
