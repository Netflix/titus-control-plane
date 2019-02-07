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

package com.netflix.titus.runtime.endpoint.metadata;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import static com.netflix.titus.common.util.Evaluators.getOrDefault;

@Singleton
public class SimpleHttpCallMetadataResolver implements CallMetadataResolver {

    private final ThreadLocal<CallMetadata> callMetadataThreadLocal = new ThreadLocal<>();

    @Override
    public Optional<CallMetadata> resolve() {
        return Optional.ofNullable(callMetadataThreadLocal.get());
    }

    protected Optional<String> resolveDirectCallerId(HttpServletRequest httpServletRequest) {
        return Optional.empty();
    }

    private void interceptBefore(HttpServletRequest httpServletRequest) {
        String callerId = getOrDefault(httpServletRequest.getHeader(CallMetadataHeaders.CALLER_ID_HEADER), "unknownCallerId");
        String callerType = getOrDefault(httpServletRequest.getHeader(CallMetadataHeaders.CALLER_TYPE_HEADER), "");
        String directCallerId = resolveDirectCallerId(httpServletRequest)
                .orElseGet(() ->
                        getOrDefault(httpServletRequest.getHeader(CallMetadataHeaders.DIRECT_CALLER_ID_HEADER), "unknownDirectCallerId")
                );
        String callReason = getOrDefault(httpServletRequest.getHeader(CallMetadataHeaders.CALL_REASON_HEADER), "reason not given");

        callMetadataThreadLocal.set(CallMetadata.newBuilder()
                .withCallerId(callerId)
                .withCallerType(CallerType.parseCallerType(callerId, callerType))
                .withCallReason(callReason)
                .withCallPath(Collections.singletonList(directCallerId))
                .build()
        );
    }

    private void interceptAfter() {
        callMetadataThreadLocal.set(null);
    }

    @Singleton
    public static class CallMetadataInterceptorFilter implements Filter {

        private final SimpleHttpCallMetadataResolver resolver;

        @Inject
        public CallMetadataInterceptorFilter(SimpleHttpCallMetadataResolver resolver) {
            this.resolver = resolver;
        }

        @Override
        public void init(FilterConfig filterConfig) throws ServletException {
        }

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
            try {
                resolver.interceptBefore((HttpServletRequest) request);
                chain.doFilter(request, response);
            } finally {
                resolver.interceptAfter();
            }
        }

        @Override
        public void destroy() {
        }
    }
}
