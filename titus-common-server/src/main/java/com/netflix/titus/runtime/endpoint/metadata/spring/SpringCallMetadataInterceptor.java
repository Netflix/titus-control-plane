/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.runtime.endpoint.metadata.spring;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.Caller;
import com.netflix.titus.api.model.callmetadata.CallerType;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataHeaders;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import static java.util.Arrays.asList;

/**
 * Spring interceptor to decorate a request {@link Authentication} object with {@link CallMetadata}.
 */
public class SpringCallMetadataInterceptor extends HandlerInterceptorAdapter {

    @VisibleForTesting
    static final String DEBUG_QUERY_PARAM = "debug";

    @Override
    public boolean preHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object handler) throws Exception {
        Authentication delegate = SecurityContextHolder.getContext().getAuthentication();
        if (delegate == null) {
            return super.preHandle(httpServletRequest, httpServletResponse, handler);
        }

        String callReason = httpServletRequest.getHeader(CallMetadataHeaders.CALL_REASON_HEADER);
        String debugQueryParameter = httpServletRequest.getParameter(DEBUG_QUERY_PARAM);
        boolean debug = debugQueryParameter == null
                ? Boolean.parseBoolean(httpServletRequest.getHeader(CallMetadataHeaders.DEBUG_HEADER))
                : Boolean.parseBoolean(debugQueryParameter);

        String originalCallerId = httpServletRequest.getHeader(CallMetadataHeaders.CALLER_ID_HEADER);

        Caller directCaller = getDirectCaller(httpServletRequest, delegate);

        CallMetadata.Builder callMetadataBuilder = CallMetadata.newBuilder()
                .withCallReason(callReason)
                .withDebug(debug);

        if (originalCallerId == null) {
            callMetadataBuilder.withCallers(Collections.singletonList(directCaller));
        } else {
            CallerType originalCallerType = CallerType.parseCallerType(
                    originalCallerId,
                    httpServletRequest.getHeader(CallMetadataHeaders.CALLER_TYPE_HEADER)
            );
            Caller originalCaller = Caller.newBuilder()
                    .withId(originalCallerId)
                    .withCallerType(originalCallerType)
                    .build();

            callMetadataBuilder.withCallers(asList(originalCaller, directCaller));
        }

        CallMetadataAuthentication authentication = new CallMetadataAuthentication(callMetadataBuilder.build(), delegate);
        SecurityContextHolder.getContext().setAuthentication(authentication);

        return super.preHandle(httpServletRequest, httpServletResponse, handler);
    }

    protected Caller getDirectCaller(HttpServletRequest httpServletRequest, Authentication delegate) {
        // If there is an original caller, we know the direct caller must be an application.
        CallerType callerType = httpServletRequest.getHeader(CallMetadataHeaders.CALLER_ID_HEADER) == null
                ? CallerType.Unknown
                : CallerType.Application;

        return Caller.newBuilder()
                .withId(delegate.getName())
                .withCallerType(callerType)
                .withContext(getContextFromServletRequest(httpServletRequest))
                .build();
    }

    protected Map<String, String> getContextFromServletRequest(HttpServletRequest httpServletRequest) {
        Map<String, String> context = new HashMap<>();

        context.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_SERVICE_NAME, httpServletRequest.getRequestURI());
        context.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_SERVICE_METHOD, httpServletRequest.getMethod());
        context.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_TRANSPORT_TYPE, "HTTP");
        context.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_TRANSPORT_SECURE, "" + httpServletRequest.isSecure());

        return context;
    }
}
