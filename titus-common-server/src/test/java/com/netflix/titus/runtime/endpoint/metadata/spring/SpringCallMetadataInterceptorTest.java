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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.CallerType;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataHeaders;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import static com.netflix.titus.runtime.endpoint.metadata.spring.SpringCallMetadataInterceptor.DEBUG_QUERY_PARAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * It is safe to run tests concurrently as the default security context strategy uses thread local variable.
 */
public class SpringCallMetadataInterceptorTest {

    private static final String MY_ORIGINAL_CALLER = "myOriginalCaller";
    private static final String MY_DIRECT_CALLER = "myDirectCaller";
    private static final String MY_PASSWORD = "myPassword";
    private static final String MY_REASON = "myReason";

    private final SpringCallMetadataInterceptor interceptor = new SpringCallMetadataInterceptor();

    private final HttpServletRequest request = mock(HttpServletRequest.class);

    private final HttpServletResponse response = mock(HttpServletResponse.class);

    @Before
    public void setUp() {
        SecurityContextHolder.setContext(SecurityContextHolder.createEmptyContext());
        SecurityContextHolder.getContext().setAuthentication(new TestingAuthenticationToken(MY_DIRECT_CALLER, MY_PASSWORD));

        when(request.getHeader(CallMetadataHeaders.CALL_REASON_HEADER)).thenReturn(MY_REASON);
    }

    @Test
    public void testDirectCaller() throws Exception {
        interceptor.preHandle(request, response, new Object());
        CallMetadata callMetadata = expectCallMetadataAuthentication();
        assertThat(callMetadata.getCallers()).hasSize(1);
        assertThat(callMetadata.getCallers().get(0).getId()).isEqualTo(MY_DIRECT_CALLER);
        assertThat(callMetadata.getCallers().get(0).getCallerType()).isEqualTo(CallerType.Unknown);
    }

    @Test
    public void testOriginalCallerId() throws Exception {
        when(request.getHeader(CallMetadataHeaders.CALLER_ID_HEADER)).thenReturn(MY_ORIGINAL_CALLER);

        interceptor.preHandle(request, response, new Object());
        CallMetadata callMetadata = expectCallMetadataAuthentication();
        assertThat(callMetadata.getCallers()).hasSize(2);
        assertThat(callMetadata.getCallers().get(0).getId()).isEqualTo(MY_ORIGINAL_CALLER);
        assertThat(callMetadata.getCallers().get(1).getId()).isEqualTo(MY_DIRECT_CALLER);
        assertThat(callMetadata.getCallers().get(1).getCallerType()).isEqualTo(CallerType.Application);
    }

    @Test
    public void testDebug() throws Exception {
        // Not set
        expectDebug(false);

        // Via header
        when(request.getHeader(CallMetadataHeaders.DEBUG_HEADER)).thenReturn("true");
        expectDebug(true);

        // Via query parameter
        when(request.getHeader(CallMetadataHeaders.DEBUG_HEADER)).thenReturn(null);
        when(request.getParameter(DEBUG_QUERY_PARAM)).thenReturn("true");
        expectDebug(true);

        // Both, but query parameter has higher priority
        when(request.getHeader(CallMetadataHeaders.DEBUG_HEADER)).thenReturn("false");
        when(request.getParameter(DEBUG_QUERY_PARAM)).thenReturn("true");
        expectDebug(true);
    }

    private void expectDebug(boolean expectedDebug) throws Exception {
        interceptor.preHandle(request, response, new Object());
        CallMetadata callMetadata = expectCallMetadataAuthentication();
        assertThat(callMetadata.isDebug()).isEqualTo(expectedDebug);
    }

    private com.netflix.titus.api.model.callmetadata.CallMetadata expectCallMetadataAuthentication() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        assertThat(authentication).isNotNull();
        assertThat(authentication).isInstanceOf(CallMetadataAuthentication.class);

        CallMetadataAuthentication callMetadataAuthentication = (CallMetadataAuthentication) authentication;

        CallMetadata callMetadata = callMetadataAuthentication.getCallMetadata();
        assertThat(callMetadata).isNotNull();
        assertThat(callMetadata.getCallReason()).isEqualTo(MY_REASON);

        return callMetadata;
    }
}