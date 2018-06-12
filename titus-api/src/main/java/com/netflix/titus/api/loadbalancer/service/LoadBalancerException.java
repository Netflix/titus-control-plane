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

package com.netflix.titus.api.loadbalancer.service;

import org.slf4j.event.Level;

import static java.lang.String.format;

public class LoadBalancerException extends RuntimeException {

    public enum ErrorCode {
        JobNotRoutableIp,
        JobMaxLoadBalancers,
        TargetGroupNotFound
    }

    private final ErrorCode errorCode;

    private LoadBalancerException(Builder builder) {
        super(builder.message, builder.cause);
        this.errorCode = builder.errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static LoadBalancerException jobNotRoutableIp(String jobId) {
        return new Builder(ErrorCode.JobNotRoutableIp, format("Job %s does not have a routable IP", jobId)).build();
    }

    public static LoadBalancerException jobMaxLoadBalancers(String jobId, int maxLoadBalancers, int curLoadBalancers) {
        return new Builder(
                ErrorCode.JobMaxLoadBalancers,
                format(
                        "Job %s already has %d load balancers and maximum is %s",
                        jobId, curLoadBalancers, maxLoadBalancers))
                .build();
    }

    public static LoadBalancerException targetGroupNotFound(String targetGroupId, Throwable cause) {
        return new Builder(ErrorCode.TargetGroupNotFound, format("TargetGroup '%s' not found.", targetGroupId))
                .withCause(cause)
                .build();
    }

    public static final class Builder {
        private final ErrorCode errorCode;
        private final String message;
        private Throwable cause;

        private Builder(ErrorCode errorCode, String message) {
            this.errorCode = errorCode;
            this.message = message;
        }

        public Builder withCause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        public LoadBalancerException build() {
            return new LoadBalancerException(this);
        }
    }

    public static Level getLogLevel(Throwable throwable) {
        Level level = Level.ERROR;

        if (throwable instanceof LoadBalancerException) {
            LoadBalancerException loadBalancerException = (LoadBalancerException) throwable;
            if (loadBalancerException.getErrorCode().equals(ErrorCode.TargetGroupNotFound)) {
                // TODO: We don't handle out-of-band deletion of TargetGroup, so this is not an exceptional case.
                level = Level.DEBUG;
            }
        }

        return level;
    }
}
