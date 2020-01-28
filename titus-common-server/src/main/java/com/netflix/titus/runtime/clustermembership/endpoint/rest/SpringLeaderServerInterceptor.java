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

package com.netflix.titus.runtime.clustermembership.endpoint.rest;

import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationStatus;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

public class SpringLeaderServerInterceptor extends HandlerInterceptorAdapter {

    private final LeaderActivationStatus leaderActivationStatus;
    private final List<String> notProtectedPaths;

    public SpringLeaderServerInterceptor(LeaderActivationStatus leaderActivationStatus,
                                         List<String> notProtectedPaths) {
        this.leaderActivationStatus = leaderActivationStatus;
        this.notProtectedPaths = notProtectedPaths;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (leaderActivationStatus.isActivatedLeader()) {
            return super.preHandle(request, response, handler);
        }

        for (String path : notProtectedPaths) {
            if (request.getRequestURI().contains(path)) {
                return super.preHandle(request, response, handler);
            }
        }

        response.setStatus(503); // Service Unavailable
        try (OutputStream os = response.getOutputStream()) {
            os.write("Not a leader or not activated yet".getBytes());
        }
        return false;
    }

    public static SpringLeaderServerInterceptor clusterMembershipAllowed(LeaderActivationStatus leaderActivationStatus) {
        return new SpringLeaderServerInterceptor(leaderActivationStatus, Collections.singletonList("/api/v3/clustermembership"));
    }
}
