/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.loadbalancer.service;

import static java.lang.String.format;

public class LoadBalancerException extends RuntimeException {

    public enum ErrorCode {
        JobNotRoutableIp,
        JobMaxLoadBalancers
    }

    private final ErrorCode errorCode;

    private LoadBalancerException(ErrorCode errorCode, String message) { this(errorCode, message, null); }

    private LoadBalancerException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static LoadBalancerException jobNotRoutableIp(String jobId) {
        return new LoadBalancerException(ErrorCode.JobNotRoutableIp, format("Job %s does not have a routable IP", jobId));
    }

    public static LoadBalancerException jobMaxLoadBalancers(String jobId, int maxLoadBalancers, int curLoadBalancers) {
        return new LoadBalancerException(ErrorCode.JobMaxLoadBalancers,
                format("Job %s already has %d load balancers and maximum is %s", jobId, curLoadBalancers, maxLoadBalancers));
    }
}
