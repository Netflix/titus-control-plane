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

package com.netflix.titus.master.endpoint.v2.rest.filter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.titus.master.endpoint.v2.rest.Util;
import com.netflix.titus.master.cluster.LeaderActivator;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.endpoint.v2.rest.Util;
import com.netflix.titus.master.master.MasterDescription;
import com.netflix.titus.master.master.MasterMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.common.util.CollectionsExt.asSet;

@Singleton
public class LeaderRedirectingFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(LeaderRedirectingFilter.class);

    private static final Set<String> ALWAYS_OPEN_PATHS = asSet("/api/v2/leader", "/api/v2/status", "/health");

    private final MasterConfiguration config;
    private final MasterMonitor masterMonitor;
    private final LeaderActivator leaderActivator;

    @Inject
    public LeaderRedirectingFilter(MasterConfiguration config, MasterMonitor masterMonitor, LeaderActivator leaderActivator) {
        this.config = config;
        this.masterMonitor = masterMonitor;
        this.leaderActivator = leaderActivator;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        boolean alwaysOpen = ALWAYS_OPEN_PATHS.contains(((HttpServletRequest) request).getRequestURI());
        if (alwaysOpen) {
            chain.doFilter(request, response);
        } else if (isLocal() || leaderActivator.isLeader()) {
            if (leaderActivator.isActivated()) {
                chain.doFilter(request, response);
            } else {
                httpResponse.setStatus(503);
            }
        } else {
            URI redirectUri = getRedirectUri((HttpServletRequest) request, masterMonitor.getLatestMaster());
            logger.info("Redirecting to " + redirectUri.toURL().toString());
            httpResponse.sendRedirect(redirectUri.toURL().toString());
        }
    }

    private URI getRedirectUri(HttpServletRequest request, MasterDescription leaderInfo) {
        URI requestUri;
        try {
            requestUri = new URI(request.getRequestURL().toString());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("The request has invalid URI: " + e.getMessage(), e);
        }

        try {
            return new URI(
                    requestUri.getScheme(),
                    requestUri.getUserInfo(),
                    leaderInfo.getHostname(),
                    leaderInfo.getApiPort(),
                    requestUri.getPath(),
                    requestUri.getQuery(),
                    requestUri.getFragment()
            );
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Failed to construct redirect URI: " + e.getMessage(), e);
        }
    }

    @Override
    public void destroy() {
    }

    private boolean isLocal() {
        return config.isLocalMode() || Util.isLocalHost(masterMonitor.getLatestMaster());
    }
}
