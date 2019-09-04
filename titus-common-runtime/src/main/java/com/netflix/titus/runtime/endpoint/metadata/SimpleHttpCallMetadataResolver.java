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

package com.netflix.titus.runtime.endpoint.metadata;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.api.jobmanager.model.Caller;
import com.netflix.titus.api.jobmanager.model.CallerType;
import com.netflix.titus.common.util.StringExt;

import static com.netflix.titus.common.util.Evaluators.getOrDefault;
import static java.util.Arrays.asList;

@Singleton
public class SimpleHttpCallMetadataResolver implements CallMetadataResolver {

    private final ThreadLocal<CallMetadata> callMetadataThreadLocal = new ThreadLocal<>();

    @Override
    public Optional<CallMetadata> resolve() {
        return Optional.ofNullable(callMetadataThreadLocal.get());
    }

    protected Optional<Caller> resolveDirectCaller(HttpServletRequest httpServletRequest) {
        return Optional.empty();
    }

    protected Map<String, String> getContextFromServletRequest(HttpServletRequest httpServletRequest) {
        Map<String, String> context = new HashMap<>();

        context.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_SERVICE_NAME, httpServletRequest.getRequestURI());
        context.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_SERVICE_METHOD, httpServletRequest.getMethod());
        context.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_TRANSPORT_TYPE, "HTTP");
        context.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_TRANSPORT_SECURE, "" + httpServletRequest.isSecure());

        return context;
    }

    private void interceptBefore(HttpServletRequest httpServletRequest) {
        Caller directCaller = resolveDirectCaller(httpServletRequest).orElseGet(() -> resolveDirectCallerFromServletRequest(httpServletRequest));
        String callReason = getOrDefault(httpServletRequest.getHeader(CallMetadataHeaders.CALL_REASON_HEADER), "reason not given");

        String originalCallerId = httpServletRequest.getHeader(CallMetadataHeaders.CALLER_ID_HEADER);

        CallMetadata.Builder callMetadataBuilder = CallMetadata.newBuilder().withCallReason(callReason);

        if (originalCallerId == null) {
            callMetadataBuilder
                    .withCallerId(directCaller.getId())
                    .withCallPath(Collections.singletonList(directCaller.getId()))
                    .withCallers(Collections.singletonList(directCaller));
        } else {
            CallerType originalCallerType = CallerType.parseCallerType(originalCallerId, httpServletRequest.getHeader(CallMetadataHeaders.CALLER_TYPE_HEADER));
            Caller originalCaller = Caller.newBuilder()
                    .withId(originalCallerId)
                    .withCallerType(originalCallerType)
                    .build();

            callMetadataBuilder
                    .withCallerId(originalCallerId)
                    .withCallPath(asList(originalCallerId, directCaller.getId()))
                    .withCallers(asList(originalCaller, directCaller));
        }

        callMetadataThreadLocal.set(callMetadataBuilder.build());
    }

    private Caller resolveDirectCallerFromServletRequest(HttpServletRequest httpServletRequest) {
        String directCallerId = httpServletRequest.getHeader(CallMetadataHeaders.DIRECT_CALLER_ID_HEADER);

        Caller.Builder callerBuilder = Caller.newBuilder();

        if (StringExt.isEmpty(directCallerId)) {
            String httpClientId = httpServletRequest.getRemoteUser();
            if (httpClientId == null) {
                httpClientId = httpServletRequest.getRemoteHost();
            }
            if (httpClientId == null) {
                httpClientId = CommonCallMetadataUtils.UNKNOWN_CALLER_ID;
            }

            callerBuilder
                    .withId(httpClientId)
                    .withCallerType(CallerType.Unknown);
        } else {
            callerBuilder
                    .withId(directCallerId)
                    .withCallerType(CallerType.parseCallerType(directCallerId, httpServletRequest.getHeader(CallMetadataHeaders.DIRECT_CALLER_TYPE_HEADER)));
        }

        return callerBuilder
                .withContext(getContextFromServletRequest(httpServletRequest))
                .build();
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
        public void init(FilterConfig filterConfig) {
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
