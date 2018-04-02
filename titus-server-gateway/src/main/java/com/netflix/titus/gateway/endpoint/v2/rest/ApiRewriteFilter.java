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

package com.netflix.titus.gateway.endpoint.v2.rest;

import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.gateway.MetricConstants;
import com.netflix.titus.gateway.MetricConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ApiRewriteFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(ApiRewriteFilter.class);

    private final Counter requestPassthroughs;
    private final Counter requestRewrites;

    @Inject
    public ApiRewriteFilter(Registry registry) {
        this.requestPassthroughs = registry.counter(MetricConstants.METRIC_REWRITE + "requestPassthroughs");
        this.requestRewrites = registry.counter(MetricConstants.METRIC_REWRITE + "requestRewrites");
    }

    @Override
    public void init(FilterConfig config) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws ServletException, IOException {
        HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        String requestUri = httpServletRequest.getRequestURI();
        String newUri = null;

        String remoteIp = httpServletRequest.getRemoteAddr();

        if (!requestUri.startsWith("/api/v2/") && !requestUri.startsWith("/static")) {
            if (requestUri.startsWith("/jobs") || requestUri.startsWith("/tasks") || requestUri.startsWith("/logs")) {
                newUri = "/api/v2" + requestUri;
            } else if (requestUri.startsWith("/v2") && (requestUri.startsWith("/v2/jobs") ||
                    requestUri.startsWith("/v2/tasks") || requestUri.startsWith("/v2/logs"))) {
                newUri = "/api" + requestUri;
            }
        }

        if (newUri != null) {
            logger.warn("[DEPRECATED API] IP: {} called: {} {} and the path was rewritten to: {}", remoteIp, httpServletRequest.getMethod(),
                    requestUri, newUri);
            requestRewrites.increment();
            request.getRequestDispatcher(newUri).forward(request, response);
        } else {
            requestPassthroughs.increment();
            chain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {
    }
}
