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

package com.netflix.titus.runtime.endpoint.rest.spring;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.runtime.endpoint.metadata.spring.CallMetadataAuthentication;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

/**
 * HTTP request interceptor for Spring.
 */
public class SpringSpectatorInterceptor extends HandlerInterceptorAdapter {

    private static final String METRICS_REQUEST = "titus.httpServerSpring.request";
    private static final String METRICS_REQUEST_LATENCY = "titus.httpServerSpring.requestLatency";

    private static final String REQUEST_TIMESTAMP = "titus.httpServerSpring.requestTimestamp";

    private static final Pattern UUID_PATTERN = Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    /**
     * An AWS instance id, consists of 'i-' prefix and 17 alpha-numeric characters following it, for example: i-07d1b67286b43458e.
     */
    private static final Pattern INSTANCE_ID_PATTERN = Pattern.compile("i-(\\p{Alnum}){17}+");

    private final Registry registry;
    private final Clock clock;

    public SpringSpectatorInterceptor(TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();
        this.clock = titusRuntime.getClock();
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        request.setAttribute(REQUEST_TIMESTAMP, clock.wallTime());
        return super.preHandle(request, response, handler);
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
        super.postHandle(request, response, handler, modelAndView);

        String callerId = getCallerId();
        String path = request.getServletPath() == null ? "unknown" : trimPath(request.getServletPath());

        List<Tag> tags = Arrays.asList(
                new BasicTag("port", "" + request.getServerPort()),
                new BasicTag("method", request.getMethod()),
                new BasicTag("path", path),
                new BasicTag("status", "" + response.getStatus()),
                new BasicTag("caller", callerId)
        );

        registry.counter(METRICS_REQUEST, tags).increment();

        Long timestamp = (Long) request.getAttribute(REQUEST_TIMESTAMP);
        if (timestamp != null) {
            registry.timer(METRICS_REQUEST_LATENCY, tags).record(clock.wallTime() - timestamp, TimeUnit.MILLISECONDS);
        }
    }

    private String getCallerId() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) {
            return "anonymous";
        }
        if (authentication instanceof CallMetadataAuthentication) {
            CallMetadataAuthentication cma = (CallMetadataAuthentication) authentication;
            return cma.getCallMetadata().getCallers().get(0).getId();
        }
        return authentication.getName();
    }

    static String trimPath(String path) {
        return removeTrailingSlash(removePatterns(INSTANCE_ID_PATTERN, removePatterns(UUID_PATTERN, path)));
    }

    private static String removeTrailingSlash(String text) {
        if (!text.endsWith("/")) {
            return text;
        }
        return removeTrailingSlash(text.substring(0, text.length() - 1));
    }

    static String removePatterns(Pattern pattern, String text) {
        return pattern.matcher(text).replaceAll("");
    }
}
