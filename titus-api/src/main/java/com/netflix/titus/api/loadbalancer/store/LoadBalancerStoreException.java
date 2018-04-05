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

package com.netflix.titus.api.loadbalancer.store;

import java.util.Set;
import javax.validation.ConstraintViolation;

public class LoadBalancerStoreException extends RuntimeException {
    public enum ErrorCode {
        BAD_DATA,
        CASSANDRA_DRIVER_ERROR,
    }

    private final ErrorCode errorCode;

    private LoadBalancerStoreException(String message, ErrorCode errorCode) {
        this(message, errorCode, null);
    }

    private LoadBalancerStoreException(String message, ErrorCode errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static LoadBalancerStoreException cassandraDriverError(Throwable e) {
        return new LoadBalancerStoreException(e.getMessage(), ErrorCode.CASSANDRA_DRIVER_ERROR, e);
    }

    public static <T> LoadBalancerStoreException badData(T value, Set<ConstraintViolation<T>> violations) {
        return new LoadBalancerStoreException("Entity " + value + " violates constraints: " + violations, ErrorCode.BAD_DATA);
    }

}
