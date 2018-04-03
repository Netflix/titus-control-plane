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

package com.netflix.titus.runtime.endpoint.common.rest.filter;

import java.io.IOException;
import java.util.Optional;
import javax.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

/**
 * Jersey does not expose all {@link javax.servlet.http.HttpServletRequest} data. To make it available we inject
 * them in this filter into thread local variable.
 */
@Singleton
public class CallerContextFilter implements Filter {

    private static final ThreadLocal<String> currentCallerAddress = new ThreadLocal<>();

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        currentCallerAddress.set(request.getRemoteAddr());
        try {
            chain.doFilter(request, response);
        } finally {
            currentCallerAddress.set(null);
        }
    }

    @Override
    public void destroy() {
    }

    public static Optional<String> getCurrentCallerAddress() {
        return Optional.ofNullable(currentCallerAddress.get());
    }
}
